package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import java.util.Random;

public class WeatherStationProducer {

    private static final Random RANDOM = new Random();
    private static boolean isDrop() {
        int rand = RANDOM.nextInt(10);
        if (rand == 1) {
            return true;
        } else
            return false;
    }

    public static void main(String[] args) {
        long s_no = 1;
        // Set up Kafka producer properties
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", LongSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());

        // Create a Kafka producer
        KafkaProducer<Long, String> producer = new KafkaProducer<>(properties);

        while (true) {
            WeatherStatusMessage message = new WeatherStatusMessage(1);
            message.generateWeatherStatusMessage(s_no);
            String value = message.toString();
            if(isDrop()){
                s_no++;
                continue;
            }
            ProducerRecord<Long, String> record = new ProducerRecord<>("weather-status-messages", message.getStationId(), value);
            producer.send(record);
            System.out.println("Sent message: " + value);
            s_no++;

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
