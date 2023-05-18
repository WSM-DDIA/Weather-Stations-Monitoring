package com.example.DDIA;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONArray;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.*;

import java.util.Properties;
import java.util.Random;

@Component
@RestController
public class WeatherStationProducer1 {
    private static final Random RANDOM = new Random();
    private Properties properties;
    public WeatherStationProducer1() {
        // Set up Kafka producer properties
        properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", LongSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());
    }

    private static boolean isDrop() {
        int rand = RANDOM.nextInt(10);
        if (rand == 1) {
            return true;
        } else
            return false;
    }

    @RequestMapping("/produce")
    public int produce(){
        System.out.println("enter producer");
        long s_no = 1;
        // Create a Kafka producer
        KafkaProducer<Long, String> producer = new KafkaProducer<>(properties);
        GetData getData = new GetData();
        Weather weather = getData.get("");
        long timeStart = weather.getTimeStart();
        JSONArray temperature = weather.getTemperature();
        JSONArray humidity = weather.getHumidity();
        JSONArray windSpeed = weather.getWindSpeed();
        WeatherStatusMessage message = new WeatherStatusMessage(1);
        int count = 0;
        while (true) {
            if(isDrop()){
                s_no++;
                continue;
            }
            message.generateWeatherStatusMessage(s_no,timeStart, (Double) temperature.get(count), (Integer) humidity.get(count), (Double) windSpeed.get(count));
            String value = message.toString();

            ProducerRecord<Long, String> record = new ProducerRecord<>("weather-status-messages", message.getStationId(), value);
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
                weather = getData.get("");
                temperature = weather.getTemperature();
                humidity = weather.getHumidity();
                windSpeed = weather.getWindSpeed();
                count = 0;
            }
        }
    }

}
