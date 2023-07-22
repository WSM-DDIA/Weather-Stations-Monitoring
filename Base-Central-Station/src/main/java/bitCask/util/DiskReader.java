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

    public static byte[] readEntryValueFromDisk(String fileID, long valuePosition, int valueSize) throws IOException {
        byte[] value = new byte[valueSize];
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(new File(dbDirectory + fileID), "r")) {
            randomAccessFile.seek(valuePosition);
            randomAccessFile.read(value, 0, valueSize);
        }

        return value;
    }

    public static Map<Integer, byte[]> readEntriesFromDisk(String fileID, Map<Integer, EntryMetaData> keyToEntryMetaData) throws IOException {
        BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(dbDirectory + fileID));
        byte[] bytes = bufferedInputStream.readAllBytes();

        int byteCursor = 0;
        Map<Integer, byte[]> keyToValue = new HashMap<>();

        while (byteCursor < bytes.length) {
            int entrySize = Ints.fromByteArray(Arrays.copyOfRange(bytes, byteCursor, byteCursor + 4));
            if (byteCursor + 4 + entrySize > bytes.length)
                break;
            byte[] entryBytes = Arrays.copyOfRange(bytes, byteCursor + 4, byteCursor + 4 + entrySize);
            byteCursor += 4 + entrySize;

            BitCaskEntry bitCaskEntry = BitCaskEntry.buildEntryFromBytes(entryBytes);
            int key = Ints.fromByteArray(bitCaskEntry.getKey());

            if (!keyToEntryMetaData.containsKey(key) ||
                    keyToEntryMetaData.get(key).getFileID().equals(fileID)) {
                int valuePosition = byteCursor - bitCaskEntry.getValueSize();

                keyToValue.put(key, bitCaskEntry.getValue());
                keyToEntryMetaData.put(key,
                        new EntryMetaData(bitCaskEntry.getValueSize(), valuePosition, bitCaskEntry.getTimestamp(), fileID));
            }
        }
        return keyToValue;
    }

    public static void readHintFile(File hintFile, Map<Integer, EntryMetaData> keyToEntryMetaData) throws IOException {
        BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(hintFile));
        byte[] bytes = bufferedInputStream.readAllBytes();

        int byteCursor = 0;

        while (byteCursor < bytes.length) {
            int entrySize = Ints.fromByteArray(Arrays.copyOfRange(bytes, byteCursor, byteCursor + 4));
            if (byteCursor + 4 + entrySize > bytes.length)
                break;

            byte[] entryBytes = Arrays.copyOfRange(bytes, byteCursor + 4, byteCursor + 4 + entrySize);
            int keySize = Ints.fromByteArray(Arrays.copyOfRange(bytes, byteCursor + 12, byteCursor + 16));
            byte[] key = Arrays.copyOfRange(bytes, byteCursor + 28, byteCursor + 28 + keySize);
            byteCursor += 4 + entrySize;

            EntryMetaData entryMetaData = EntryMetaData.buildEntryFromBytes(entryBytes, hintFile.getName().substring(5));

            int keyValue = Ints.fromByteArray(key);
            if (!keyToEntryMetaData.containsKey(keyValue)) {
                keyToEntryMetaData.put(keyValue, entryMetaData);
            }
        }
    }
}
