package bitCask.util;

import bitCask.storage.BitCaskEntry;
import bitCask.storage.EntryMetaData;
import com.google.common.primitives.Ints;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class DiskReader {
    public static void setDbDirectory(String dbDirectory) {
        DiskReader.dbDirectory = dbDirectory;
    }

    static String dbDirectory;
    public static byte[] readEntryValueFromDisk(String fileID, long valuePosition, int valueSize) throws IOException {
        byte[] value = new byte[valueSize];
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(new File(dbDirectory + fileID), "r")) {
            randomAccessFile.seek(valuePosition);
            randomAccessFile.read(value, 0, valueSize);
        }

        return value;
    }

    public static Map<String, String> readEntriesFromDisk(String fileID, Map<String, EntryMetaData> keyToEntryMetaData) throws IOException {
        BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(dbDirectory + fileID));
        byte[] bytes = bufferedInputStream.readAllBytes();

        int byteCursor = 0;
        Map<String, String> keyToValue = new HashMap<>();

        while (byteCursor < bytes.length) {
            int entrySize = Ints.fromByteArray(Arrays.copyOfRange(bytes, byteCursor, byteCursor + 4));
            if (byteCursor + 4 + entrySize > bytes.length)
                break;
            byte[] entryBytes = Arrays.copyOfRange(bytes, byteCursor + 4, byteCursor + 4 + entrySize);
            byteCursor += 4 + entrySize;

            BitCaskEntry bitCaskEntry = BitCaskEntry.buildEntryFromBytes(entryBytes);

            if (!keyToEntryMetaData.containsKey(bitCaskEntry.getKey()) ||
                    keyToEntryMetaData.get(bitCaskEntry.getKey()).getFileID().equals(fileID)) {
                int valuePosition = byteCursor - bitCaskEntry.getValueSize();

                keyToValue.put(bitCaskEntry.getKey(), bitCaskEntry.getValue());
                keyToEntryMetaData.put(bitCaskEntry.getKey(),
                        new EntryMetaData(bitCaskEntry.getValueSize(), valuePosition, bitCaskEntry.getTimestamp(), fileID));
            }
        }
        return keyToValue;
    }

    public static void readHintFile(File hintFile, Map<String, EntryMetaData> keyToEntryMetaData) throws IOException {
        BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(hintFile));
        byte[] bytes = bufferedInputStream.readAllBytes();

        int byteCursor = 0;

        while (byteCursor < bytes.length) {
            int entrySize = Ints.fromByteArray(Arrays.copyOfRange(bytes, byteCursor, byteCursor + 4));
            if (byteCursor + 4 + entrySize > bytes.length)
                break;

            byte[] entryBytes = Arrays.copyOfRange(bytes, byteCursor + 4, byteCursor + 4 + entrySize);
            int keySize = Ints.fromByteArray(Arrays.copyOfRange(bytes, byteCursor + 12, byteCursor + 16));
            String key = new String(Arrays.copyOfRange(bytes, byteCursor + 28, byteCursor + 28 + keySize),
                    StandardCharsets.UTF_8);
            byteCursor += 4 + entrySize;

            EntryMetaData entryMetaData = EntryMetaData.buildEntryFromBytes(entryBytes, hintFile.getName().substring(5));

            if (!keyToEntryMetaData.containsKey(key)) {
                keyToEntryMetaData.put(key, entryMetaData);
            }
        }
    }
}
