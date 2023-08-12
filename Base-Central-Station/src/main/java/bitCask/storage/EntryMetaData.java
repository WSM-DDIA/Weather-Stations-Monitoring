package bitCask.storage;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

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

    public static EntryMetaData buildEntryFromBytes(byte[] bytes, String fileID) {
        long timestamp = parseBytesToTimestamp(bytes);
        int valueSize = parseBytesToValueSize(bytes);
        long valuePosition = parseBytesToValuePosition(bytes);

        return new EntryMetaData(valueSize, valuePosition, timestamp, fileID);
    }

    private static long parseBytesToTimestamp(byte[] bytes) {
        byte[] timestampBytes = Arrays.copyOfRange(bytes, 0, 8);
        return Longs.fromByteArray(timestampBytes);
    }

    private static int parseBytesToValueSize(byte[] bytes) {
        byte[] valueSizeBytes = Arrays.copyOfRange(bytes, 12, 16);
        return Ints.fromByteArray(valueSizeBytes);
    }

    private static Long parseBytesToValuePosition(byte[] bytes) {
        byte[] valuePositionBytes = Arrays.copyOfRange(bytes, 16, 24);
        return Longs.fromByteArray(valuePositionBytes);
    }

    public byte[] toBytes(byte[] key, long valuePosition) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byteArrayOutputStream.write(Longs.toByteArray(timestamp));
        byteArrayOutputStream.write(Ints.toByteArray(key.length));
        byteArrayOutputStream.write(Ints.toByteArray(valueSize));
        byteArrayOutputStream.write(Longs.toByteArray(valuePosition));
        byteArrayOutputStream.write(key);

        return byteArrayOutputStream.toByteArray();
    }

    public int getValueSize() {
        return valueSize;
    }

    public long getValuePosition() {
        return valuePosition;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getFileID() {
        return fileID;
    }

    public void setFileID(String fileID) {
        this.fileID = fileID;
    }
}
