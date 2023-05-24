package baseCentralStation.Utilities;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;

import java.time.Duration;
import java.util.*;

public class KafkaAPI {

    private String topic;
    private Properties props;

    public KafkaAPI(String bootstrapServers, String topic) {
        this.topic = topic;
        props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }


    // Consumes messages from the Kafka server
    public List<String> consumeMessages() {

        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        List<String> message = new ArrayList<>();

        consumer.subscribe(Collections.singletonList(topic));

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

        for (ConsumerRecord<String, String> record : records) {
            message.add(record.value());
        }

        consumer.close();
        return message;
    }
}
