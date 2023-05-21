package org.example;

import org.json.JSONArray;
import org.json.JSONObject;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Scanner;

public class GetData {

//    @Autowired
//    private RestTemplate restTemplate = new RestTemplate();
    private String url = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly=temperature_2m,relativehumidity_2m,windspeed_80m&" +
            "current_weather=true&temperature_unit=fahrenheit&timeformat=unixtime&forecast_days=1";
//
//    @ServiceActivator(inputChannel = "inputChannel")
//    public Weather get(String message) {
//        // Send HTTP GET request to Open Meteo API
//        String response = restTemplate.getForObject(url, String.class);
//
//        // Parse the JSON response from the API.
//        JSONObject jsonObject = new JSONObject(response.toString());
//        JSONObject currentWeatherDaily = jsonObject.getJSONObject("hourly"); // get all data through all day hourly;
//        JSONArray temperature_2m = currentWeatherDaily.getJSONArray("temperature_2m");
//        JSONArray windspeed_80m = currentWeatherDaily.getJSONArray("windspeed_80m");
//        JSONArray time = currentWeatherDaily.getJSONArray("time");
//        JSONArray relativehumidity_2m = currentWeatherDaily.getJSONArray("relativehumidity_2m");
//
//        Weather weather = new Weather(time.getInt(0),temperature_2m, relativehumidity_2m,windspeed_80m);
//        return weather;
//    }
        public  Weather fetchOpenMeteoData() {
            String urlString = url;
            URL url;
            HttpURLConnection conn;
            Scanner scanner;
            String responseBody = "";
            try {
                url = new URL(urlString);
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
            // Parse the JSON response from the API.
            JSONObject jsonObject = new JSONObject(responseBody.toString());
            JSONObject currentWeatherDaily = jsonObject.getJSONObject("hourly"); // get all data through all day hourly;
            JSONArray temperature_2m = currentWeatherDaily.getJSONArray("temperature_2m");
            JSONArray windspeed_80m = currentWeatherDaily.getJSONArray("windspeed_80m");
            JSONArray time = currentWeatherDaily.getJSONArray("time");
            JSONArray relativehumidity_2m = currentWeatherDaily.getJSONArray("relativehumidity_2m");

            Weather weather = new Weather(time.getInt(0),temperature_2m, relativehumidity_2m,windspeed_80m);
            return weather;
        }

}