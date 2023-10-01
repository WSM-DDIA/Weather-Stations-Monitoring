package bitCask.util;

import bitCask.storage.BitCaskEntry;
import bitCask.storage.EntryMetaData;
import com.google.common.primitives.Ints;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class DiskReader {
    static String dbDirectory;

    public static void setDbDirectory(String dbDirectory) {
        DiskReader.dbDirectory = dbDirectory;
    }

    /**
     * Reads the value of the entry from the disk.
     *
     * @param fileID        name of the file
     * @param valuePosition position of the value in the file
     * @param valueSize     size of the value
     * @return value of the entry
     * @throws IOException if the file is not found
     */
    public static byte[] readEntryValueFromDisk(String fileID, long valuePosition, int valueSize) throws IOException {
        byte[] value = new byte[valueSize];
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(new File(dbDirectory + fileID), "r")) {
            randomAccessFile.seek(valuePosition);
            randomAccessFile.read(value, 0, valueSize);
        }

        return value;
    }

    /**
     * Reads all the entries from the disk from the given file and constructs a map of key to value.
     * Also, it updates the map of key to entry meta-data.
     * If the entry is faulty, it stops reading the file.
     * If the entry is a tombstone, it removes the entry from the map of key to value.
     *
     * @param fileID             name of the file
     * @param keyToEntryMetaData map of key to entry meta-data
     * @return map of key to value
     * @throws IOException if the file is not found
     */
    public static Map<Integer, byte[]> readEntriesFromDisk(String fileID, Map<Integer, EntryMetaData> keyToEntryMetaData) throws IOException {
        BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(dbDirectory + fileID));
        byte[] bytes = bufferedInputStream.readAllBytes();

        int byteCursor = 0;
        Map<Integer, byte[]> keyToValue = new HashMap<>();

        while (byteCursor < bytes.length) {
            int entrySize = Ints.fromByteArray(Arrays.copyOfRange(bytes, byteCursor, byteCursor + 4));
            if (isFaultyRecord(bytes, byteCursor, entrySize))
                break;
            byte[] entryBytes = Arrays.copyOfRange(bytes, byteCursor + 4, byteCursor + 4 + entrySize);
            byteCursor += 4 + entrySize;

            BitCaskEntry bitCaskEntry = BitCaskEntry.buildEntryFromBytes(entryBytes);
            int key = Ints.fromByteArray(bitCaskEntry.getKey());

            if (isTombStone(bitCaskEntry.getValue(), bitCaskEntry.getValueSize())) {
                keyToEntryMetaData.remove(key);
                keyToValue.remove(key);
                continue;
            }

            if (!keyToEntryMetaData.containsKey(key) ||
                    keyToEntryMetaData.get(key).getTimestamp() <= bitCaskEntry.getTimestamp()) {
                int valuePosition = byteCursor - bitCaskEntry.getValueSize();

                keyToValue.put(key, bitCaskEntry.getValue());
                keyToEntryMetaData.put(key,
                        new EntryMetaData(bitCaskEntry.getValueSize(), valuePosition, bitCaskEntry.getTimestamp(), fileID));
            }
        }
        return keyToValue;
    }

    /**
     * Reads all the entries from the disk from the given hint file and updates the map of key to entry meta-data.
     * If the entry is faulty, it stops reading the file.
     * If the entry is a tombstone, it removes the entry from the map of key to value.
     *
     * @param hintFile           hint file to read from
     * @param keyToEntryMetaData map of key to entry meta-data
     * @throws IOException if the file is not found
     */
    public static void readHintFile(File hintFile, Map<Integer, EntryMetaData> keyToEntryMetaData) throws IOException {
        BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(hintFile));
        byte[] bytes = bufferedInputStream.readAllBytes();

        int byteCursor = 0;

        while (byteCursor < bytes.length) {
            int entrySize = Ints.fromByteArray(Arrays.copyOfRange(bytes, byteCursor, byteCursor + 4));
            if (isFaultyRecord(bytes, byteCursor, entrySize))
                break;

            byte[] entryBytes = Arrays.copyOfRange(bytes, byteCursor + 4, byteCursor + 4 + entrySize);
            int keySize = Ints.fromByteArray(Arrays.copyOfRange(bytes, byteCursor + 12, byteCursor + 16));
            byte[] key = Arrays.copyOfRange(bytes, byteCursor + 28, byteCursor + 28 + keySize);
            byteCursor += 4 + entrySize;

            EntryMetaData entryMetaData = EntryMetaData.buildEntryFromBytes(entryBytes,
                    DirectoryConstants.getFileTimeStamp(hintFile.getName()) + DirectoryConstants.DataExtension);

            byte[] valueBytes = readEntryValueFromDisk(entryMetaData.getFileID(), entryMetaData.getValuePosition(), entryMetaData.getValueSize());

            int keyValue = Ints.fromByteArray(key);

            if (isTombStone(valueBytes, entryMetaData.getValueSize())) {
                keyToEntryMetaData.remove(keyValue);
                continue;
            }

            if (!keyToEntryMetaData.containsKey(keyValue) ||
                    keyToEntryMetaData.get(keyValue).getTimestamp() <= entryMetaData.getTimestamp()) {
                keyToEntryMetaData.put(keyValue, entryMetaData);
            }
        }
    }

    /**
     * Checks if the entry is faulty or not.
     *
     * @param bytes      bytes of the entry
     * @param byteCursor current position of the cursor
     * @param entrySize  size of the entry
     * @return true if the entry is faulty, false otherwise
     */
    private static boolean isFaultyRecord(byte[] bytes, int byteCursor, int entrySize) {
        return byteCursor + 4 + entrySize > bytes.length;
    }

    /**
     * Checks if the entry is a tombstone or not.
     *
     * @param valueBytes bytes of the value
     * @param valueSize  size of the value
     * @return true if the entry is a tombstone, false otherwise
     */
    private static boolean isTombStone(byte[] valueBytes, int valueSize) {
        if (valueSize != 5)
            return false;

        return valueBytes[0] == -1 && valueBytes[2] == -1 && valueBytes[4] == -1;
    }
}
