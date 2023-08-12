package bitCask.storage;

import bitCask.exception.DirectoryNotFoundException;
import bitCask.util.DirectoryConstants;
import bitCask.util.DiskReader;
import bitCask.util.DiskWriter;
import com.google.common.primitives.Ints;
import lombok.Getter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class BitCask implements IBitCask {
    private final BitCaskFacade bitCaskFacade = new BitCaskFacade(this);
    public int status = 400;
    @Getter
    private DiskWriter diskWriter;
    private String dbDirectory;
    @Getter
    private Map<Integer, EntryMetaData> keyToEntryMetaData;

    public BitCask() {
        this.keyToEntryMetaData = new HashMap<>();
    }

    /**
     * Opens the database and recover the data from the disk
     */
    private void reConstruct() {
        File directory = new File(dbDirectory);
        File[] files = directory.listFiles();
        List<File> filesToReConstruct = BitCaskFacade.listFilesGivenPrefixName(Objects.requireNonNull(files),
                DirectoryConstants.DataExtension);

        for (File file : filesToReConstruct) {
            try {
                File hintFile = new File(file.getParent() + "/" +
                        DirectoryConstants.getFileTimeStamp(file.getName()) + DirectoryConstants.HintExtension);
                if (hintFile.exists())
                    DiskReader.readHintFile(hintFile, keyToEntryMetaData);
                else
                    DiskReader.readEntriesFromDisk(file.getName(), keyToEntryMetaData);
            } catch (IOException e) {
                e.getCause();
            }
        }
    }

    @Override
    public byte[] get(byte[] key) throws IOException, DirectoryNotFoundException {
        if (dbDirectory == null)
            throw new DirectoryNotFoundException("Directory not found, please open the database first");
        int keyValue = Ints.fromByteArray(key);

        if (!this.keyToEntryMetaData.containsKey(keyValue))
            return null;

        EntryMetaData entryMetaData = keyToEntryMetaData.get(keyValue);

        return DiskReader.readEntryValueFromDisk(entryMetaData.getFileID(), entryMetaData.getValuePosition(), entryMetaData.getValueSize());
    }

    @Override
    public void delete(byte[] key) throws DirectoryNotFoundException, IOException {
        if (dbDirectory == null)
            throw new DirectoryNotFoundException("Directory not found, please open the database first");

        byte[] value = {-1, 0, -1, 0, -1};

        bitCaskFacade.writeEntryMetaData(key, value);

        keyToEntryMetaData.remove(Ints.fromByteArray(key));
    }

    @Override
    public int open(String directory) throws FileNotFoundException {
        this.dbDirectory = directory;
        this.keyToEntryMetaData = new HashMap<>();
        DiskReader.setDbDirectory(this.dbDirectory);
        reConstruct();
        this.diskWriter = new DiskWriter(this.dbDirectory);
        status = 200;
        return 200;
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException, DirectoryNotFoundException {
        if (dbDirectory == null)
            throw new DirectoryNotFoundException("Directory not found, please open the database first");
        EntryMetaData entryMetaData = bitCaskFacade.writeEntryMetaData(key, value);

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

        List<File> filesToCompact = BitCaskFacade.listFilesGivenPrefixName(files, DirectoryConstants.ReplicaExtension);
        BitCaskFacade.readAllKeys(compactedKeyToEntryMetaData, keyToValue, filesToCompact);

        String activeFileName = DirectoryConstants.getFileTimeStamp(filesToCompact.get(filesToCompact.size() - 1).getName());
        File firstFile = filesToCompact.get(0);

        String compactedFileName = firstFile.getParent() + '/' + DirectoryConstants.getFileTimeStamp(firstFile.getName())
                + DirectoryConstants.DataExtension + 'm';
        String replicaCompactedFileName = firstFile.getPath() + 'm';

        File compactedFile = new File(compactedFileName);
        File replicaCompactedFile = new File(replicaCompactedFileName);

        boolean created = compactedFile.createNewFile();
        boolean replicaCreated = replicaCompactedFile.createNewFile();

        if (!created || !replicaCreated)
            throw new IOException("Failed to create compacted file");

        bitCaskFacade.writeCompactedFile(compactedKeyToEntryMetaData, keyToValue, compactedFile);
        bitCaskFacade.deleteOldFiles(files, activeFileName);

        compactedFile = bitCaskFacade.renameFile(compactedFileName, compactedFile);
        bitCaskFacade.renameFile(replicaCompactedFileName, replicaCompactedFile);

        String hintFileName = firstFile.getParent() + '/' + DirectoryConstants.getFileTimeStamp(firstFile.getName()) + DirectoryConstants.HintExtension + 'm';
        bitCaskFacade.renameFile(hintFileName, new File(hintFileName));

        bitCaskFacade.updateInMemoryKeysAfterCompaction(compactedKeyToEntryMetaData, compactedFile);

        System.out.println("Finish Compaction");
    }
}
