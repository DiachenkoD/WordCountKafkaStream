package com.github.diachenkod.stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

import static com.github.diachenkod.BankBalanceConstants.SERVER;

public class StreamProperties {
    private static final Properties streamProperties = new Properties();

    static {
        streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-application");
        try (Serde<String> string = Serdes.String()) {
            streamProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, string.getClass());
            streamProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, string.getClass());
        }
        streamProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamProperties.put("bootstrap.servers", SERVER);
        streamProperties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0");
        streamProperties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
    }


    public static Properties getStreamProperties() {
        return streamProperties;
    }
}
