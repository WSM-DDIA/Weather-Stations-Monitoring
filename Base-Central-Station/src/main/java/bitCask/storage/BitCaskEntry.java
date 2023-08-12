package bitCask.storage;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import lombok.Getter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

@Getter
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

    /**
     * Builds a {@link BitCaskEntry} from a byte array
     *
     * @param bytes byte array representation of {@link BitCaskEntry}
     * @return {@link BitCaskEntry} instance
     */
    public static BitCaskEntry buildEntryFromBytes(byte[] bytes) {
        long timestamp = getTimestampFromBytes(bytes);
        int keySize = getKeySizeFromBytes(bytes);
        int valueSize = getValueSizeFromBytes(bytes, keySize);
        byte[] key = extractKeyBytes(bytes, keySize);
        byte[] value = extractValueBytes(bytes, keySize, valueSize);
        return new BitCaskEntry(keySize, timestamp, key, value);
    }

    /**
     * Gets timestamp from the byte array
     *
     * @param bytes byte array representation of {@link BitCaskEntry}
     * @return timestamp
     */
    private static long getTimestampFromBytes(byte[] bytes) {
        byte[] timestampBytes = Arrays.copyOfRange(bytes, 0, 8);
        return Longs.fromByteArray(timestampBytes);
    }

    /**
     * Gets value size from the byte array
     *
     * @param bytes   byte array representation of {@link BitCaskEntry}
     * @param keySize size of the key
     * @return value size
     */
    private static int getValueSizeFromBytes(byte[] bytes, int keySize) {
        byte[] valueSizeBytes = Arrays.copyOfRange(bytes, keySize + 12, keySize + 16);
        return Ints.fromByteArray(valueSizeBytes);
    }

    /**
     * Gets key size from the byte array
     *
     * @param bytes byte array representation of {@link BitCaskEntry}
     * @return key size
     */
    private static int getKeySizeFromBytes(byte[] bytes) {
        byte[] keySizeBytes = Arrays.copyOfRange(bytes, 8, 12);
        return Ints.fromByteArray(keySizeBytes);
    }

    /**
     * Extracts the value bytes from the byte array
     *
     * @param bytes     byte array representation of {@link BitCaskEntry}
     * @param keySize   size of the key
     * @param valueSize size of the value
     * @return value bytes
     */
    private static byte[] extractValueBytes(byte[] bytes, int keySize, int valueSize) {
        return Arrays.copyOfRange(bytes, keySize + 16, keySize + 16 + valueSize);
    }

    /**
     * Extracts the key bytes from the byte array
     *
     * @param bytes   byte array representation of {@link BitCaskEntry}
     * @param keySize size of the key
     * @return key bytes
     */
    private static byte[] extractKeyBytes(byte[] bytes, int keySize) {
        return Arrays.copyOfRange(bytes, 12, keySize + 12);
    }

    /**
     * Converts the {@link BitCaskEntry} to a byte array
     *
     * @return byte array representation of {@link BitCaskEntry}
     * @throws IOException if an I/O error occurs
     */
    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byteArrayOutputStream.write(Longs.toByteArray(timestamp));
        byteArrayOutputStream.write(Ints.toByteArray(keySize));
        byteArrayOutputStream.write(key);
        byteArrayOutputStream.write(Ints.toByteArray(valueSize));
        byteArrayOutputStream.write(value);

        return byteArrayOutputStream.toByteArray();
    }
}
