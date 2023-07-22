package bitCask.storage;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class BitCaskEntry {
    int valueSize, keySize;
    long timestamp;
    byte[] value, key;

    public BitCaskEntry(int keySize, long timestamp, byte[] key, byte[] value) {
        this.valueSize = value.length;
        this.keySize = keySize;
        this.timestamp = timestamp;
        this.value = value;
        this.key = key;
    }

    public static BitCaskEntry buildEntryFromBytes(byte[] bytes) {
        long timestamp = parseBytesToTimestamp(bytes);
        int keySize = parseBytesToKeySize(bytes);
        int valueSize = parseBytesToValueSize(bytes, keySize);
        byte[] key = extractKeyBytes(bytes, keySize);
        byte[] value = extractValueBytes(bytes, keySize, valueSize);
        return new BitCaskEntry(keySize, timestamp, key, value);
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

    private static byte[] extractValueBytes(byte[] bytes, int keySize, int valueSize) {
        return Arrays.copyOfRange(bytes, keySize + 16, keySize + 16 + valueSize);
    }

    private static byte[] extractKeyBytes(byte[] bytes, int keySize) {
        return Arrays.copyOfRange(bytes, 12, keySize + 12);
    }

    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byteArrayOutputStream.write(Longs.toByteArray(timestamp));
        byteArrayOutputStream.write(Ints.toByteArray(keySize));
        byteArrayOutputStream.write(key);
        byteArrayOutputStream.write(Ints.toByteArray(valueSize));
        byteArrayOutputStream.write(value);

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

    public byte[] getValue() {
        return value;
    }

    public byte[] getKey() {
        return key;
    }
}
