package baseCentralStation.Utilities;

import baseCentralStation.proto.WeatherStatus;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import lombok.Data;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.MessageType;
import org.json.JSONObject;

import java.io.Serializable;
import java.util.HashMap;


@Data
public class WeatherStatusMessage implements Serializable {
    private String stationId;
    private String sNo;
    private String batteryStatus;
    private String statusTimestamp;
    private String humidity;
    private String temperature;
    private String windSpeed;

    public WeatherStatusMessage(HashMap<String, String> msg) {
        this.stationId = msg.get("station_id");
        this.sNo = msg.get("s_no");
        this.batteryStatus = msg.get("battery_status");
        this.statusTimestamp = msg.get("status_timestamp");
        this.humidity = msg.get("humidity");
        this.temperature = msg.get("temperature");
        this.windSpeed = msg.get("wind_speed");
    }

    public static String parseProtoBufBytesToValue(byte[] valueBytes) throws InvalidProtocolBufferException {
        WeatherStatus builder = WeatherStatus.newBuilder().mergeFrom(valueBytes).build();

        baseCentralStation.proto.WeatherStatusMessage weatherStatusMessage = baseCentralStation.proto.WeatherStatusMessage.builder()
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

    protected static byte[] parseValueToBytesArray(String value) throws InvalidProtocolBufferException {
        JSONObject weatherStatusJson = new JSONObject(value);
        WeatherStatus.Builder builder = WeatherStatus.newBuilder();
        JsonFormat.parser().merge(weatherStatusJson.toString(), builder);
        Message weatherStatusMessage = builder.build();

        return weatherStatusMessage.toByteArray();
    }

    public Group toGroup(MessageType schema) {
        GroupFactory groupFactory = new SimpleGroupFactory(schema);
        Group weatherStatusGroup = groupFactory.newGroup();
        weatherStatusGroup.add("station_id", Long.parseLong(stationId));
        weatherStatusGroup.add("s_no", Long.parseLong(sNo));
        weatherStatusGroup.add("battery_status", batteryStatus);
        weatherStatusGroup.add("status_timestamp", Long.parseLong(statusTimestamp));
        weatherStatusGroup.add("humidity", (int) Float.parseFloat(humidity));
        weatherStatusGroup.add("temperature", (int) Float.parseFloat(temperature));
        weatherStatusGroup.add("wind_speed", (int) Float.parseFloat(windSpeed));
        return weatherStatusGroup;
    }

    public String toString() {
        return "{station_id:" + this.stationId +
                ", s_no:" + this.sNo +
                ", battery_status:'" + this.batteryStatus + '\'' +
                ", status_timestamp:" + this.statusTimestamp +
                ", weather:{humidity:" + this.humidity + ", temperature:" + this.temperature + ", wind_speed:" + this.windSpeed + "}" +
                '}';
    }
}
