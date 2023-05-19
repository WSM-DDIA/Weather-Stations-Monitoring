package org.example;

import org.json.JSONArray;
import org.json.JSONObject;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Scanner;

public class GetData {

    private String url = "https://api.open-meteo.com/v1/forecast?latitude=30.06&longitude=31.25&hourly=relativehumidity_2m,windspeed_80m,temperature_80m&" +
            "current_weather=true&temperature_unit=fahrenheit&timeformat=unixtime&forecast_days=1&timezone=";
    public GetData(String position){
        this.url = this.url + position;
    }
    private String fetchDataFromOpenMeteo(){
        URL url;
        HttpURLConnection conn;
        Scanner scanner;
        String responseBody = "";
        try {
            url = new URL(this.url);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json");

            scanner = new Scanner(conn.getInputStream());
            while (scanner.hasNext()) {
                responseBody += scanner.nextLine();
            }
            scanner.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return responseBody.toString();
    }
    public  Weather getData() {
            String responseBody = this.fetchDataFromOpenMeteo();

            // Parse the JSON response from the API.
            JSONObject jsonObject = new JSONObject(responseBody.toString());
            JSONObject currentWeatherDaily = jsonObject.getJSONObject("hourly"); // get all data through all day hourly;
            JSONArray relativehumidity_2m = currentWeatherDaily.getJSONArray("relativehumidity_2m");
            JSONArray windspeed_80m = currentWeatherDaily.getJSONArray("windspeed_80m");
            JSONArray temperature_2m = currentWeatherDaily.getJSONArray("temperature_80m");

            Weather weather = new Weather(temperature_2m, relativehumidity_2m,windspeed_80m);
            return weather;
    }

}