package bitCask.storage;

import bitCask.util.DirectoryConstants;
import bitCask.util.DiskReader;
import bitCask.util.DiskResponse;
import com.google.common.primitives.Ints;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class BitCaskFacade {
    private final BitCask bitCask;

    public BitCaskFacade(BitCask bitCask) {
        this.bitCask = bitCask;
    }

    /**
     * Lists (sorted) all the files with the given suffix name in the database directory
     *
     * @param files files in the database directory
     * @param suffixName suffix name of the file to search for
     * @return sorted list of files with the given suffix name
     */
    static List<File> listFilesGivenPrefixName(File[] files, String suffixName) {
        return Arrays.stream(files)
                .filter(file -> file.getName().endsWith(suffixName))
                .sorted((o1, o2) -> {
                    long l1 = Long.parseLong(DirectoryConstants.getFileTimeStamp(o1.getName()));
                    long l2 = Long.parseLong(DirectoryConstants.getFileTimeStamp(o2.getName()));
                    return (int) (l1 - l2);
                })
                .toList();
    }

    /**
     * Reads all the keys from the files to be compacted and stores them in the keyToValue map.
     * Also, it updates the map of key to entry meta-data.
     *
     * @param compactedKeyToEntryMetaData map of key to entry meta-data
     * @param keyToValue map of key to value
     * @param filesToCompact list of files to be compacted
     * @throws IOException if there is an error while reading the files
     */
    static void readAllKeys(Map<Integer, EntryMetaData> compactedKeyToEntryMetaData, Map<Integer, byte[]> keyToValue,
                            List<File> filesToCompact) throws IOException {
        for (int i = 0; i < filesToCompact.size() - 1; i++) {
            Map<Integer, byte[]> tempKeyToValue = DiskReader.readEntriesFromDisk(filesToCompact.get(i).getName(), compactedKeyToEntryMetaData);
            for (Map.Entry<Integer, byte[]> entry : tempKeyToValue.entrySet()) {
                if (!keyToValue.containsKey(entry.getKey())) {
                    keyToValue.put(entry.getKey(), entry.getValue());
                }
            }
        }
    }

    /**
     * Writes the given key and value to the active file and hint file and returns the entry meta-data
     *
     * @param key key to be written
     * @param value value to be written
     * @return entry meta-data of the written key and value
     * @throws IOException if there is an error while writing the key and value
     */
    EntryMetaData writeEntryMetaData(byte[] key, byte[] value) throws IOException {
        BitCaskEntry bitCaskEntry = new BitCaskEntry(key.length, System.currentTimeMillis(), key, value);

        DiskResponse diskResponse = bitCask.getDiskWriter().writeEntryToActiveFile(bitCaskEntry);
        return new EntryMetaData(bitCaskEntry.getValueSize(), diskResponse.getValuePosition(),
                bitCaskEntry.getTimestamp(), diskResponse.getFileName());
    }

    /**
     * Updates the in-memory keys after compaction
     *
     * @param compactedKeyToEntryMetaData map of key to entry meta-data
     * @param compactedFile file to which the keys are compacted
     */
    void updateInMemoryKeysAfterCompaction(Map<Integer, EntryMetaData> compactedKeyToEntryMetaData, File compactedFile) {
        for (Map.Entry<Integer, EntryMetaData> entry : compactedKeyToEntryMetaData.entrySet()) {
            entry.getValue().setFileID(compactedFile.getName());
            if (bitCask.getKeyToEntryMetaData().containsKey(entry.getKey()) &&
                    bitCask.getKeyToEntryMetaData().get(entry.getKey()).getTimestamp() == entry.getValue().getTimestamp() &&
                    !bitCask.getKeyToEntryMetaData().get(entry.getKey()).getFileID().equals(entry.getValue().getFileID())) {
                bitCask.getKeyToEntryMetaData().put(entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * Writes the given key and value to the compacted file (original, replica and hint) and returns the entry meta-data
     *
     * @param compactedKeyToEntryMetaData map of key to entry meta-data
     * @param keyToValue map of key to value
     * @param compactedFile file to which the keys are compacted
     * @throws IOException if there is an error while writing the key and value
     */
    void writeCompactedFile(Map<Integer, EntryMetaData> compactedKeyToEntryMetaData,
                            Map<Integer, byte[]> keyToValue, File compactedFile) throws IOException {
        for (Map.Entry<Integer, byte[]> entry : keyToValue.entrySet()) {
            byte[] key = Ints.toByteArray(entry.getKey());
            BitCaskEntry bitCaskEntry = new BitCaskEntry(
                    key.length,
                    compactedKeyToEntryMetaData.get(entry.getKey()).getTimestamp(),
                    key,
                    entry.getValue()
            );

            DiskResponse diskResponse = bitCask.getDiskWriter().writeCompacted(bitCaskEntry, compactedFile);
            EntryMetaData entryMetaData = new EntryMetaData(bitCaskEntry.getValueSize(), diskResponse.getValuePosition(),
                    bitCaskEntry.getTimestamp(), diskResponse.getFileName());

            compactedKeyToEntryMetaData.put(entry.getKey(), entryMetaData);
        }
    }

    /**
     * Renames the file to be compacted
     *
     * @param fileNameToRename name of the file to be renamed
     * @param fileToRename file to be renamed
     * @return renamed file
     */
    File renameFile(String fileNameToRename, File fileToRename) {
        File renamedFileWithoutSuffix = new File(fileNameToRename.substring(0, fileNameToRename.length() - 1));
        boolean rename = fileToRename.renameTo(renamedFileWithoutSuffix);
        if (!rename)
            System.out.println("Failed to rename file");

        return renamedFileWithoutSuffix;
    }

    /**
     * Deletes the old files after compaction
     *
     * @param files files in the database directory
     * @param activeFileName name of the active file
     */
    void deleteOldFiles(File[] files, String activeFileName) {
        Arrays.stream(files)
                .filter(
                        file -> !file.getName().endsWith("m") &&
                                !file.getName().startsWith(activeFileName) &&
                                (
                                        file.getName().endsWith(DirectoryConstants.DataExtension) ||
                                                file.getName().endsWith(DirectoryConstants.HintExtension) ||
                                                file.getName().endsWith(DirectoryConstants.ReplicaExtension)
                                )
                )
                .forEach(File::delete);
    }
}