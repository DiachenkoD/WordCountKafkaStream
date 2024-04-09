package com.github.diachenkod.producer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static com.github.diachenkod.BankBalanceConstants.SERVER;

public class ProducerProperties {
    private static final Properties producerProperties = new Properties();

    static {
        final String serializerName = StringSerializer.class.getName();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializerName);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializerName);
        producerProperties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // strongest producing guarantee
        producerProperties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        producerProperties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        producerProperties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

    }


    public static Properties getProducerProperties() {
        return producerProperties;
    }
}
