package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;

import java.util.Properties;
import java.util.Random;

public class WeatherStationProducer2 {
    private static final Random RANDOM = new Random();
    private Properties properties;
    public WeatherStationProducer2() {
        // Set up Kafka producer properties
        properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    private static boolean isDrop() {
        int rand = RANDOM.nextInt(10);
        if (rand == 1) {
            return true;
        } else
            return false;
    }

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

    public int produce(){
        long s_no = 1;
        // Create a Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        GetData getData = new GetData();
        Weather weather = getData.fetchOpenMeteoData();
        long timeStart = weather.getTimeStart();
        JSONArray temperature = weather.getTemperature();
        JSONArray humidity = weather.getHumidity();
        JSONArray windSpeed = weather.getWindSpeed();
        WeatherStatusMessage message = new WeatherStatusMessage("2");
        int count = 0;
        while (true) {
            if(isDrop()){
                s_no++;
                if((s_no % 24) == 1) {
                    weather = getData.fetchOpenMeteoData();
                    temperature = weather.getTemperature();
                    humidity = weather.getHumidity();
                    windSpeed = weather.getWindSpeed();
                    count = 0;
                }
                continue;
            }
            message.generateWeatherStatusMessage(s_no,timeStart, (Double) temperature.get(count), (Integer) humidity.get(count), (Double) windSpeed.get(count));
            String value = message.toString();

            ProducerRecord<String, String> record = new ProducerRecord<>("weather-status-messages",value);
            producer.send(record);
            System.out.println("Sent message: " + value);
            s_no++;
            timeStart++;
            count++;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if((s_no % 24) == 1) {
                weather = getData.fetchOpenMeteoData();
                temperature = weather.getTemperature();
                humidity = weather.getHumidity();
                windSpeed = weather.getWindSpeed();
                count = 0;
            }
        }
    }
}
