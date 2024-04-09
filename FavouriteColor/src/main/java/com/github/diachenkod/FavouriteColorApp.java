package com.github.diachenkod;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.log4j.BasicConfigurator;

import java.util.List;
import java.util.Properties;

public class FavouriteColorApp {

    private static final List<String> availableColors = List.of("red", "blue", "green");

    public static void main(String[] args) {
        BasicConfigurator.configure();
        final Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-color");
        try (Serde<String> string = Serdes.String()) {
            properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, string.getClass());
            properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, string.getClass());
        }
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put("bootstrap.servers", "localhost:9092");

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> lineStream = builder.stream("favourite-color-input");

        final KTable<String, String> nameToColor = lineStream
                .mapValues(value -> value.toLowerCase())
                .filter((key, value) -> {
                    final String[] personColor = value.split(",");
                    return personColor.length == 2 && availableColors.contains(personColor[1]);
                })
                .selectKey((ignoredKey, value) -> value.split(",")[0])
                .mapValues(value -> value.split(",")[1])
                .toTable();

        nameToColor
                .groupBy((name, color) -> KeyValue.pair(color, color), Grouped.with(Serdes.String(), Serdes.String()))
                .count()
                .toStream().to("favourite-color-output", Produced.with(Serdes.String(), Serdes.Long()));
        try (KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties)) {
            kafkaStreams.start();
            //TIP Just for the testing purposes
            while (true) {
                kafkaStreams.metadataForLocalThreads().forEach(System.out::println);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }
}
