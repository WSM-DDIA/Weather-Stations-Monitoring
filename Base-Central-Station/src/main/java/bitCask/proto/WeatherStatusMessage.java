package bitCask.proto;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class WeatherStatusMessage implements Serializable {
    private long stationId;
    private long sNo;
    private String batteryStatus;
    private long statusTimestamp;
    private float humidity;
    private float temperature;
    private float windSpeed;

    public String toJsonString() {
        return "{station_id:" + this.stationId +
                ", s_no:" + this.sNo +
                ", battery_status:'" + this.batteryStatus + '\'' +
                ", status_timestamp:" + this.statusTimestamp +
                ", weather:{humidity:" + this.humidity + ", temperature:" + this.temperature + ", wind_speed:" + this.windSpeed + "}" +
                '}';
    }
}
