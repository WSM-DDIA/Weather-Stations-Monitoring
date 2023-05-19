package org.example;

import org.json.JSONArray;

public class Weather {
    private long timeStart;
    private JSONArray temperature;
    private JSONArray humidity;
    private JSONArray windSpeed;

    public Weather(long timeStart, JSONArray temperature, JSONArray humidity, JSONArray windSpeed) {
        this.timeStart = timeStart;
        this.temperature = temperature;
        this.humidity = humidity;
        this.windSpeed = windSpeed;
    }
    public long getTimeStart() {
        return timeStart;
    }

    public JSONArray getTemperature() {
        return temperature;
    }

    public JSONArray getHumidity() {
        return humidity;
    }

    public JSONArray getWindSpeed() {
        return windSpeed;
    }
}
