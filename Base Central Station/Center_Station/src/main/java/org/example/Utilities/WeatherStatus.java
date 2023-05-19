package org.example.Utilities;

import lombok.Data;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.MessageType;


import java.io.Serializable;
import java.util.HashMap;


@Data
public class WeatherStatus implements Serializable {
    private String stationId;
    private String sNo;
    private String batteryStatus;
    private String statusTimestamp;
    private String humidity;
    private String temperature;
    private String windSpeed;

    public WeatherStatus(HashMap<String, String> msg){
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
        Group group = groupFactory.newGroup();
        // Set primitive fields
        group.add("station_id", Long.parseLong(stationId));
        group.add("s_no", Long.parseLong(sNo));
        group.add("battery_status", batteryStatus);
        group.add("status_timestamp", Long.parseLong(statusTimestamp));
        group.add("humidity", (int) Float.parseFloat(humidity));
        group.add("temperature", (int) Float.parseFloat(temperature));
        group.add("wind_speed", (int) Float.parseFloat(windSpeed));
        return group;
    }

}
