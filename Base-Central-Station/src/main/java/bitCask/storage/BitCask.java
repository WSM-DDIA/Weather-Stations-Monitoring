package bitCask.storage;

import bitCask.util.Constants;
import bitCask.util.DiskReader;
import bitCask.util.DiskResponse;
import bitCask.util.DiskWriter;
import com.google.common.primitives.Ints;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class BitCask implements IBitCask {
    private final DiskWriter diskWriter;
    private final String dbDirectory;
    private Map<Integer, EntryMetaData> keyToEntryMetaData;

    public BitCask(String dbDirectory) throws FileNotFoundException {
        this.dbDirectory = dbDirectory;
        this.keyToEntryMetaData = new HashMap<>();
        DiskReader.setDbDirectory(this.dbDirectory);
        reConstruct();
        this.diskWriter = new DiskWriter(this.dbDirectory);
    }

    private void reConstruct() {
        File directory = new File(dbDirectory);
        File[] files = directory.listFiles();
        List<File> filesToReConstruct = Arrays.stream(Objects.requireNonNull(files))
                .filter(file -> file.getName().startsWith("file_"))
                .sorted((o1, o2) -> {
                    long l1 = Long.parseLong(o1.getName().split("_")[1]);
                    long l2 = Long.parseLong(o2.getName().split("_")[1]);
                    return (int) (l2 - l1);
                })
                .toList();

        for (File file : filesToReConstruct) {
            try {
                File hintFile = new File(file.getParent() + "/" + Constants.HINT_FILE_PREFIX + file.getName());
                if (hintFile.exists())
                    DiskReader.readHintFile(hintFile, keyToEntryMetaData);
                else
                    DiskReader.readEntriesFromDisk(file.getName(), keyToEntryMetaData);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public byte[] get(byte[] key) throws IOException {
        int keyValue = Ints.fromByteArray(key);

        if (!this.keyToEntryMetaData.containsKey(keyValue))
            return null;

        EntryMetaData entryMetaData = keyToEntryMetaData.get(keyValue);

        return DiskReader.readEntryValueFromDisk(entryMetaData.getFileID(), entryMetaData.getValuePosition(), entryMetaData.getValueSize());
    }

    @Override
    public void delete(byte[] key) {
        throw new UnsupportedOperationException("Unimplemented method 'Delete'");
    }

    @Override
    public int open(File directory) throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'Open'");
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        BitCaskEntry bitCaskEntry = new BitCaskEntry(key.length, System.currentTimeMillis(), key, value);

        DiskResponse diskResponse = diskWriter.writeEntryToActiveFile(bitCaskEntry);
        EntryMetaData entryMetaData = new EntryMetaData(bitCaskEntry.getValueSize(), diskResponse.getValuePosition(),
                bitCaskEntry.getTimestamp(), diskResponse.getFileName());

        keyToEntryMetaData.put(Ints.fromByteArray(key), entryMetaData);
    }

    @Override
    public void mergeAndCompaction() throws IOException {
        File directory = new File(dbDirectory);
        File[] files = directory.listFiles();

        System.out.println("Checking Compaction");
        if (Objects.requireNonNull(files).length < 7)
            return;

        System.out.println("Begin Compaction");

        Map<Integer, EntryMetaData> compactedKeyToEntryMetaData = new HashMap<>();
        Map<Integer, byte[]> keyToValue = new HashMap<>();

        List<File> filesToCompact = Arrays.stream(files)
                .filter(file -> file.getName().startsWith("replica_"))
                .sorted((o1, o2) -> {
                    long l1 = Long.parseLong(o1.getName().split("_")[2]);
                    long l2 = Long.parseLong(o2.getName().split("_")[2]);
                    return (int) (l2 - l1);
                })
                .toList();

        String activeFileName = filesToCompact.get(0).getName().substring(8);
        for (int i = 1; i < filesToCompact.size(); i++) {
            Map<Integer, byte[]> tempKeyToValue = DiskReader.readEntriesFromDisk(filesToCompact.get(i).getName(), compactedKeyToEntryMetaData);
            for (Map.Entry<Integer, byte[]> entry : tempKeyToValue.entrySet()) {
                if (!keyToValue.containsKey(entry.getKey())) {
                    keyToValue.put(entry.getKey(), entry.getValue());
                }
            }
        }

        File firstFile = filesToCompact.get(filesToCompact.size() - 1);
        String compactedFileName = firstFile.getParent() + '/' + firstFile.getName().substring(8) + 'm';
        String replicaCompactedFileName = firstFile.getPath() + 'm';
        File compactedFile = new File(compactedFileName);
        File replicaCompactedFile = new File(replicaCompactedFileName);
        boolean created = compactedFile.createNewFile();
        boolean replicaCreated = replicaCompactedFile.createNewFile();

        if (!created || !replicaCreated) {
            throw new IOException("Failed to create compacted file");
        }

        for (Map.Entry<Integer, byte[]> entry : keyToValue.entrySet()) {
            byte[] key = Ints.toByteArray(entry.getKey());
            BitCaskEntry bitCaskEntry = new BitCaskEntry(
                    key.length,
                    compactedKeyToEntryMetaData.get(entry.getKey()).getTimestamp(),
                    key,
                    entry.getValue()
            );

            DiskResponse diskResponse = diskWriter.writeCompacted(bitCaskEntry, compactedFile);
            EntryMetaData entryMetaData = new EntryMetaData(bitCaskEntry.getValueSize(), diskResponse.getValuePosition(),
                    bitCaskEntry.getTimestamp(), diskResponse.getFileName());

            compactedKeyToEntryMetaData.put(entry.getKey(), entryMetaData);
        }

        deleteOldFiles(files, activeFileName);

        compactedFile = renameFile(compactedFileName, compactedFile);
        renameFile(replicaCompactedFileName, replicaCompactedFile);
        String hintFileName = firstFile.getParent() + '/' + Constants.HINT_FILE_PREFIX + firstFile.getName().substring(8) + 'm';
        renameFile(hintFileName, new File(hintFileName));

        for (Map.Entry<Integer, EntryMetaData> entry : compactedKeyToEntryMetaData.entrySet()) {
            entry.getValue().setFileID(compactedFile.getName());
            if (keyToEntryMetaData.containsKey(entry.getKey()) &&
                    keyToEntryMetaData.get(entry.getKey()).getTimestamp() == entry.getValue().getTimestamp() &&
                    !keyToEntryMetaData.get(entry.getKey()).getFileID().equals(entry.getValue().getFileID())) {
                keyToEntryMetaData.put(entry.getKey(), entry.getValue());
            }
        }

        System.out.println("Finish Compaction");
    }

    private File renameFile(String fileNameToRename, File fileToRename) {
        File renamedFileWithoutSuffix = new File(fileNameToRename.substring(0, fileNameToRename.length() - 1));
        boolean rename = fileToRename.renameTo(renamedFileWithoutSuffix);
        if (!rename)
            System.out.println("Failed to rename file");

        return renamedFileWithoutSuffix;
    }

    private void deleteOldFiles(File[] files, String activeFileName) {
        Arrays.stream(files)
                .filter(
                        file -> !file.getName().endsWith("m") &&
                                !file.getName().endsWith(activeFileName) &&
                                !file.getName().substring(8).endsWith(activeFileName) &&
                                (
                                        file.getName().startsWith(Constants.FILE_PREFIX) ||
                                                file.getName().startsWith(Constants.HINT_FILE_PREFIX) ||
                                                file.getName().startsWith(Constants.REPLICA_FILE_PREFIX)
                                )
                )
                .forEach(File::delete);
    }
}
