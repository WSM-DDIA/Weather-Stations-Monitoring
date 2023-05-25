package org.example;

import org.json.JSONArray;

public class Weather {
    private JSONArray temperature;
    private JSONArray humidity;
    private JSONArray windSpeed;

    public Weather(JSONArray temperature, JSONArray humidity, JSONArray windSpeed) {
        this.temperature = temperature;
        this.humidity = humidity;
        this.windSpeed = windSpeed;
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
