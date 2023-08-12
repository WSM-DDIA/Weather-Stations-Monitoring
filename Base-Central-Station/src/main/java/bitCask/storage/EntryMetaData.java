package bitCask.storage;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import lombok.Getter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

@Getter
public class EntryMetaData {
    int valueSize;
    long valuePosition, timestamp;
    String fileID;

    public EntryMetaData(int valueSize, long valuePosition, long timestamp, String fileID) {
        this.valueSize = valueSize;
        this.valuePosition = valuePosition;
        this.timestamp = timestamp;
        this.fileID = fileID;
    }

    /**
     * Builds a {@link EntryMetaData} from a byte array.
     *
     * @param bytes byte array representation of {@link EntryMetaData}
     * @return {@link EntryMetaData} instance
     */
    public static EntryMetaData buildEntryFromBytes(byte[] bytes, String fileID) {
        long timestamp = getTimestampFromBytes(bytes);
        int valueSize = getValueSizeFromBytes(bytes);
        long valuePosition = getValuePositionFromBytes(bytes);

        return new EntryMetaData(valueSize, valuePosition, timestamp, fileID);
    }

    /**
     * Gets timestamp from the byte array.
     *
     * @param bytes byte array representation of {@link EntryMetaData}
     * @return timestamp
     */
    private static long getTimestampFromBytes(byte[] bytes) {
        byte[] timestampBytes = Arrays.copyOfRange(bytes, 0, 8);
        return Longs.fromByteArray(timestampBytes);
    }

    /**
     * Gets value size from the byte array.
     *
     * @param bytes byte array representation of {@link EntryMetaData}
     * @return value size
     */
    private static int getValueSizeFromBytes(byte[] bytes) {
        byte[] valueSizeBytes = Arrays.copyOfRange(bytes, 12, 16);
        return Ints.fromByteArray(valueSizeBytes);
    }

    /**
     * Gets value position from the byte array.
     *
     * @param bytes byte array representation of {@link EntryMetaData}
     * @return value position
     */
    private static Long getValuePositionFromBytes(byte[] bytes) {
        byte[] valuePositionBytes = Arrays.copyOfRange(bytes, 16, 24);
        return Longs.fromByteArray(valuePositionBytes);
    }

    /**
     * Converts the {@link EntryMetaData} to a byte array.
     *
     * @param key          key
     * @param valuePosition value position
     * @return byte array representation of {@link EntryMetaData}
     * @throws IOException if an I/O error occurs
     */
    public byte[] toBytes(byte[] key, long valuePosition) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byteArrayOutputStream.write(Longs.toByteArray(timestamp));
        byteArrayOutputStream.write(Ints.toByteArray(key.length));
        byteArrayOutputStream.write(Ints.toByteArray(valueSize));
        byteArrayOutputStream.write(Longs.toByteArray(valuePosition));
        byteArrayOutputStream.write(key);

        return byteArrayOutputStream.toByteArray();
    }

    /**
     * Sets file id.
     *
     * @param fileID file id string value
     */
    public void setFileID(String fileID) {
        this.fileID = fileID;
    }
}
