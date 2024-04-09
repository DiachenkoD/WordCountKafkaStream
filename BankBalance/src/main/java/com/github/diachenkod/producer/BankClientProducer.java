package com.github.diachenkod.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class BankClientProducer {
    private final KafkaProducer<String, String> producer;
    private final String topicName;

    public BankClientProducer(final String topicName) {
        producer = new KafkaProducer<>(ProducerProperties.getProducerProperties());
        this.topicName = topicName;
    }

    public void produce(final ClientTransaction transaction) {
        final ProducerRecord<String, String> record = new ProducerRecord<>(topicName, transaction.name(), transaction.getTransaction());

        producer.send(record);
    }

    public void flush() {
        producer.flush();
    }
}
