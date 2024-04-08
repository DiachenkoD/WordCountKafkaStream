package com.github.diachenkod;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.log4j.BasicConfigurator;

import java.util.Arrays;
import java.util.Properties;

public class StreamsStarterApp {

    public static void main(String[] args) {
        BasicConfigurator.configure();
        final Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count");
        try (Serde<String> string = Serdes.String()) {
            properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, string.getClass());
            properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, string.getClass());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put("bootstrap.servers", "localhost:9092");

        final StreamsBuilder builder = new StreamsBuilder();

        // 1 - stream from Kafka
        final KStream<String, String> lineStream = builder.stream("word-count-input");

        final KTable<String, Long> countedTable = lineStream
                // 2 - map values to lowercase
                .mapValues(value -> value.toLowerCase())
                // 3 - flatmap values split by space
                .flatMapValues(value -> Arrays.asList(value.split(" ")))
                // 4 - select key to apply a key (we discard the old key)
                .selectKey((ignoredKey, value) -> value)
                // 5 - group by key before aggregation
                .groupByKey()
                // 6 - count occurrences
                .count(Materialized.as("Counts"));

        // 7 - to in order to write the results back to kafka
        countedTable.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

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
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }


}
