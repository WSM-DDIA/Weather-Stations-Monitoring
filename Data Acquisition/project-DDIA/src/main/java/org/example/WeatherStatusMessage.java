package org.example;


import java.util.Random;

public class WeatherStatusMessage {
    public String station_id;
    public static long s_no;
    public String battery_status;
    public long status_timestamp;
    public int humidity;
    public double temperature;
    public double wind_speed;
    private static final Random RANDOM = new Random();

    private static String getBatteryStatus() {
        int rand = RANDOM.nextInt(10);
        if (rand < 3) {
            return "low";
        } else if (rand < 7) {
            return "medium";
        } else {
            return "high";
        }
    }
    public WeatherStatusMessage(){}

    public WeatherStatusMessage(String station_id) {
        this.station_id = station_id;
        this.s_no = s_no;
    }
    public String getStationId() {
        return station_id;
    }



    public void generateWeatherStatusMessage(long s_no, long status_timestamp, Double temperature, int humidity, Double windSpeed) {
        this.s_no = s_no;
        this.status_timestamp = status_timestamp;
        this.battery_status = getBatteryStatus();
        this.temperature = temperature;
        this.humidity = humidity;
        this.wind_speed = windSpeed;
    }

    @Override
    public String toString() {
        return "WeatherStatusMessage{" +
                "station_id=" + this.station_id +
                ", s_no=" + this.s_no +
                ", battery_status='" + this.battery_status + '\'' +
                ", status_timestamp=" + this.status_timestamp +
                ", weather={humidity=" + this.humidity + ", temperature=" + this.temperature + ", wind_speed=" + this.wind_speed + "}" +
                '}';
    }
}

