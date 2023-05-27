package baseCentralStation.Utilities;

import lombok.Data;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.MessageType;


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

    public WeatherStatusMessage(HashMap<String, String> msg){
        this.stationId = msg.get("station_id");
        this.sNo = msg.get("s_no");
        this.batteryStatus = msg.get("battery_status");
        this.statusTimestamp = msg.get("status_timestamp");
        this.humidity = msg.get("humidity");
        this.temperature = msg.get("temperature");
        this.windSpeed = msg.get("wind_speed");
    }

    public Group toGroup(MessageType schema){
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

    public String toString(){
        return "{station_id:" + this.stationId +
                ", s_no:" + this.sNo +
                ", battery_status:'" + this.batteryStatus + '\'' +
                ", status_timestamp:" + this.statusTimestamp +
                ", weather:{humidity:" + this.humidity + ", temperature:" + this.temperature + ", wind_speed:" + this.windSpeed + "}" +
                '}';
    }
}
