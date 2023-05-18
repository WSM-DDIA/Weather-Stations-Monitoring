package com.example.DDIA;

import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class GetData {

    @Autowired
    private RestTemplate restTemplate = new RestTemplate();
    private String url = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly=temperature_2m,relativehumidity_2m,windspeed_80m&" +
            "current_weather=true&temperature_unit=fahrenheit&timeformat=unixtime&forecast_days=1";

    @ServiceActivator(inputChannel = "inputChannel")
    public Weather get(String message) {
        // Send HTTP GET request to Open Meteo API
        String response = restTemplate.getForObject(url, String.class);

        // Parse the JSON response from the API.
        JSONObject jsonObject = new JSONObject(response.toString());
        JSONObject currentWeatherDaily = jsonObject.getJSONObject("hourly"); // get all data through all day hourly;
        JSONArray temperature_2m = currentWeatherDaily.getJSONArray("temperature_2m");
        JSONArray windspeed_80m = currentWeatherDaily.getJSONArray("windspeed_80m");
        JSONArray time = currentWeatherDaily.getJSONArray("time");
        JSONArray relativehumidity_2m = currentWeatherDaily.getJSONArray("relativehumidity_2m");

        Weather weather = new Weather(time.getInt(0),temperature_2m, relativehumidity_2m,windspeed_80m);
        return weather;
    }
}