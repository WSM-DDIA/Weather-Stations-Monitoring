package org.example;


import java.util.Random;

public class WeatherStatusMessage {
    private static final Random RANDOM = new Random();
    public static long s_no;
    public String station_id;
    public String battery_status;
    public long status_timestamp;
    public int humidity;
    public double temperature;
    public double wind_speed;

    public WeatherStatusMessage() {
    }

    public WeatherStatusMessage(String station_id) {
        this.station_id = station_id;
    }

    /**
     * Generate a random battery status.
     *
     * @return battery status
     */
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

    /**
     * Generate a weather status message with random values.
     *
     * @param sNo message number
     * @param statusTimestamp timestamp of the message
     */
    public void generateWeatherStatusMessage(long sNo, long statusTimestamp, Double temperature, int humidity, Double windSpeed) {
        s_no = sNo;
        this.status_timestamp = statusTimestamp;
        this.battery_status = getBatteryStatus();
        this.temperature = temperature;
        this.humidity = humidity;
        this.wind_speed = windSpeed;
    }

    @Override
    public String toString() {
        return "{station_id=" + this.station_id +
                ", s_no=" + s_no +
                ", battery_status='" + this.battery_status + '\'' +
                ", status_timestamp=" + this.status_timestamp +
                ", weather={humidity=" + this.humidity + ", temperature=" + this.temperature + ", wind_speed=" + this.wind_speed + "}" +
                '}';
    }
}

