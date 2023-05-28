package bitCask.storage;

import bitCask.proto.WeatherStatus;
import bitCask.proto.WeatherStatusMessage;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.protobuf.InvalidProtocolBufferException;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class BitCaskEntry {
    int valueSize, keySize;
    long timestamp;
    String value, key;

    public BitCaskEntry(int keySize, long timestamp, String key, String value) throws InvalidProtocolBufferException {
        this.valueSize = parseValueToBytesArray(value).length;
        this.keySize = keySize;
        this.timestamp = timestamp;
        this.value = value;
        this.key = key;
    }

    public static BitCaskEntry buildEntryFromBytes(byte[] bytes) throws InvalidProtocolBufferException {
        long timestamp = parseBytesToTimestamp(bytes);
        int keySize = parseBytesToKeySize(bytes);
        int valueSize = parseBytesToValueSize(bytes, keySize);
        String key = parseBytesToKey(bytes, keySize);
        String value = parseBytesToValue(bytes, keySize, valueSize);
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

    private static String parseBytesToValue(byte[] bytes, int keySize, int valueSize) throws InvalidProtocolBufferException {
        byte[] valueBytes = Arrays.copyOfRange(bytes, keySize + 16, keySize + 16 + valueSize);

        return parseProtoBufBytesToValue(valueBytes);
    }

    public static String parseBytesToValue(byte[] bytes) throws InvalidProtocolBufferException {
        return parseProtoBufBytesToValue(bytes);
    }

    private static String parseProtoBufBytesToValue(byte[] valueBytes) throws InvalidProtocolBufferException {
        WeatherStatus builder = WeatherStatus.newBuilder().mergeFrom(valueBytes).build();

        WeatherStatusMessage weatherStatusMessage = WeatherStatusMessage.builder()
                .stationId(builder.getStationId())
                .sNo(builder.getSNo())
                .batteryStatus(builder.getBatteryStatus())
                .statusTimestamp(builder.getStatusTimestamp())
                .humidity(builder.getWeather().getHumidity())
                .temperature(builder.getWeather().getTemperature())
                .windSpeed(builder.getWeather().getWindSpeed())
                .build();
        return weatherStatusMessage.toJsonString();
    }

    private static String parseBytesToKey(byte[] bytes, int keySize) {
        byte[] keyBytes = Arrays.copyOfRange(bytes, 12, keySize + 12);
        return new String(keyBytes, StandardCharsets.UTF_8);
    }

    private static byte[] parseValueToBytesArray(String value) throws InvalidProtocolBufferException {
        JSONObject weatherStatusJson = new JSONObject(value);
        WeatherStatus.Builder builder = WeatherStatus.newBuilder();
        JsonFormat.parser().merge(weatherStatusJson.toString(), builder);
        Message weatherStatusMessage = builder.build();

        return weatherStatusMessage.toByteArray();
    }

    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byteArrayOutputStream.write(Longs.toByteArray(timestamp));
        byteArrayOutputStream.write(Ints.toByteArray(keySize));
        byteArrayOutputStream.write(key.getBytes());
        byteArrayOutputStream.write(Ints.toByteArray(valueSize));
        byteArrayOutputStream.write(parseValueToBytesArray(value));

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
