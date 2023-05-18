package com.example.DDIA;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@RestController
//@CrossOrigin(origins = "http://localhost:4200")
//@RequestMapping("/consumer")
public class WeatherStationConsumer {

    Properties props;

    public WeatherStationConsumer() {
        props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "weather-consumer-group");
    }

    @RequestMapping("/consume")
    public int consume(){
        // Create a Kafka consumer
        Consumer<Long, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the weather-status topic
        consumer.subscribe(Collections.singletonList("weather-status-messages"));

        while (true) {
            // Poll for new messages
            ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(100));

            // Process the new messages
            for (ConsumerRecord<Long, String> record : records) {
                String message = record.value();
                System.out.println("Received message: " + message);

                // Extract humidity value from the message
                int startIndex = message.indexOf("humidity=") + 9;
                int endIndex = message.indexOf(",", startIndex);
                float humidity = Float.parseFloat(message.substring(startIndex, endIndex));

                // Check if humidity is higher than 70%
                if (humidity > 70) {
                    System.out.println("Humidity is higher than 70%");
                }
            }
        }
    }
}
