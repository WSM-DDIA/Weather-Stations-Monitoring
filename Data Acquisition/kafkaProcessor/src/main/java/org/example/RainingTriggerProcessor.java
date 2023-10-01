package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RainingTriggerProcessor {
    private static final Pattern messagePattern = Pattern.compile("\\{station_id=(\\d+), s_no=(\\d+), battery_status='(\\w+)', status_timestamp=(\\d+), " +
            "weather=\\{humidity=(\\d+(?:\\.\\d+)?), temperature=(\\d+(?:\\.\\d+)?), wind_speed=(\\d+(?:\\.\\d+)?)}}");
    private final Properties config;

    public RainingTriggerProcessor() {
        this.config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "raining-trigger-processor");
        Map<String, String> env = System.getenv();
        String kafkaBroker = "localhost:9092";
        System.out.println(kafkaBroker);
        config.put("bootstrap.servers", kafkaBroker);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    }

    public void run() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> weatherStream = builder.stream("weather-status-messages", Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> rainEvents = weatherStream
                .filter(
                        (key, value) -> {
                            Matcher matcher = messagePattern.matcher(value);
                            if (messagePattern.matcher(value).matches() && matcher.find()) {
                                return Integer.parseInt(matcher.group(5)) >= 70;
                            } else {
                                return false;
                            }
                        })
                .mapValues(
                        value -> {
                            Matcher matcher = messagePattern.matcher(value);
                            if (matcher.find()) {
                                return "RainingMessage{ station_id=" +
                                        matcher.group(1) +
                                        ", " +
                                        "s_no=" +
                                        matcher.group(2) +
                                        ", " +
                                        "battery_status=" +
                                        matcher.group(3) +
                                        ", " +
                                        "status_timestamp=" +
                                        matcher.group(4) +
                                        ", " +
                                        "weather='raining' }";
                            } else {
                                return "";
                            }

                        }
                )
                .peek(
                        (key, value) -> System.out.println(value)
                );

        rainEvents.to("raining-status-messages", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), this.config);
        streams.start();
    }

}