package bitCask.storage;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class BitCaskEntry {
    int valueSize, keySize;
    long timestamp;
    String value, key;

    public BitCaskEntry(int keySize, int valueSize, long timestamp, String key, String value) {
        this.valueSize = valueSize;
        this.keySize = keySize;
        this.timestamp = timestamp;
        this.value = value;
        this.key = key;
    }

    public static BitCaskEntry buildEntryFromBytes(byte[] bytes) {
        long timestamp = parseBytesToTimestamp(bytes);
        int keySize = parseBytesToKeySize(bytes);
        int valueSize = parseBytesToValueSize(bytes, keySize);
        String key = parseBytesToKey(bytes, keySize);
        String value = parseBytesToValue(bytes, keySize, valueSize);
        return new BitCaskEntry(valueSize, keySize, timestamp, key, value);
    }

    private static long parseBytesToTimestamp(byte[] bytes) {
        byte[] timestampBytes = Arrays.copyOfRange(bytes, 0, 8);
        return Longs.fromByteArray(timestampBytes);
    }

    private static int parseBytesToValueSize(byte[] bytes, int keySize) {
        byte[] valueSizeBytes = Arrays.copyOfRange(bytes, keySize + 12, keySize + 16);
        return Ints.fromByteArray(valueSizeBytes);
    }

    private static int parseBytesToKeySize(byte[] bytes) {
        byte[] keySizeBytes = Arrays.copyOfRange(bytes, 8, 12);
        return Ints.fromByteArray(keySizeBytes);
    }

    private static String parseBytesToValue(byte[] bytes, int keySize, int valueSize) {
        byte[] valueBytes = Arrays.copyOfRange(bytes, keySize + 16, keySize + 16 + valueSize);
        return new String(valueBytes, StandardCharsets.UTF_8);
    }

    private static String parseBytesToKey(byte[] bytes, int keySize) {
        byte[] keyBytes = Arrays.copyOfRange(bytes, 12, keySize + 12);
        return new String(keyBytes, StandardCharsets.UTF_8);
    }

    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byteArrayOutputStream.write(Longs.toByteArray(timestamp));
        byteArrayOutputStream.write(Ints.toByteArray(keySize));
        byteArrayOutputStream.write(key.getBytes());
        byteArrayOutputStream.write(Ints.toByteArray(valueSize));
        byteArrayOutputStream.write(value.getBytes());

        return byteArrayOutputStream.toByteArray();
    }

    public int getValueSize() {
        return valueSize;
    }

    public int getKeySize() {
        return keySize;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getValue() {
        return value;
    }

    public String getKey() {
        return key;
    }
}
