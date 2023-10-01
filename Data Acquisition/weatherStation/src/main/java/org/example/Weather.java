package org.example;

import lombok.Getter;
import org.json.JSONArray;

@Getter
public class Weather {
    private final JSONArray temperature;
    private final JSONArray humidity;
    private final JSONArray windSpeed;

    public Weather(JSONArray temperature, JSONArray humidity, JSONArray windSpeed) {
        this.temperature = temperature;
        this.humidity = humidity;
        this.windSpeed = windSpeed;
    }
}
