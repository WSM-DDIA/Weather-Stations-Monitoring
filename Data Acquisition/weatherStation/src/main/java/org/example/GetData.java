package org.example;

import org.json.JSONArray;
import org.json.JSONObject;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Scanner;

public class GetData {

    private String url = "";

    public GetData(String latitude, String longitude) {
        this.url = "https://api.open-meteo.com/v1/forecast?latitude=" +
                latitude + "&longitude=" + longitude + "&hourly=relativehumidity_2m,windspeed_80m,temperature_80m&" +
                "current_weather=true&temperature_unit=fahrenheit&timeformat=unixtime&forecast_days=1&timezone=Africa%2FCairo";

    }

    /**
     * Fetches the data from the API.
     *
     * @return String that contains the data from the API.
     */
    private String fetchDataFromOpenMeteo() {
        URL url;
        HttpURLConnection conn;
        Scanner scanner;
        StringBuilder responseBody = new StringBuilder();
        try {
            url = new URL(this.url);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json");

            scanner = new Scanner(conn.getInputStream());
            while (scanner.hasNext()) {
                responseBody.append(scanner.nextLine());
            }
            scanner.close();
        } catch (Exception e) {
            e.getCause();
        }
        return responseBody.toString();
    }

    /**
     * Gets the data from the API. And parses it into a Weather object.
     * Gets data hourly.
     *
     * @return Weather object that contains the data from the API.
     */
    public Weather getData() {
        String responseBody = this.fetchDataFromOpenMeteo();

        JSONObject jsonObject = new JSONObject(responseBody.toString());
        JSONObject currentWeatherDaily = jsonObject.getJSONObject("hourly");
        JSONArray relativeHumidity_2m = currentWeatherDaily.getJSONArray("relativehumidity_2m");
        JSONArray windSpeed_80m = currentWeatherDaily.getJSONArray("windspeed_80m");
        JSONArray temperature_2m = currentWeatherDaily.getJSONArray("temperature_80m");

        return new Weather(temperature_2m, relativeHumidity_2m, windSpeed_80m);
    }

}