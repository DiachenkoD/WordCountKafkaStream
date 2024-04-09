package com.github.diachenkod.stream;

import com.github.diachenkod.producer.ClientTransaction;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;

import java.nio.charset.StandardCharsets;

import static com.github.diachenkod.BankBalanceConstants.TOPIC_INPUT;
import static com.github.diachenkod.BankBalanceConstants.TOPIC_OUTPUT;

public class BankBalanceStreamApp {
    public static void main(String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        final Serde<ClientBalance> clientCountSerde = Serdes.serdeFrom(
                (s, clientBalance) -> clientBalance.getJson().getBytes(StandardCharsets.UTF_8),
                (s, bytes) -> {
                    if (bytes == null || bytes.length == 0) {
                        return null;
                    }
                    return ClientBalance.parseBalance(new String(bytes, StandardCharsets.UTF_8));
                });

        final ClientBalance clientBalance = new ClientBalance(0.0, 0L, "");

        final KStream<String, String> clientsStream = builder.stream(TOPIC_INPUT, Consumed.with(Serdes.String(), Serdes.String()));
        final KTable<String, ClientBalance> nameCountAggregate = clientsStream
                .peek((k, v) -> System.out.println("Something here before serialization:" + k + " " + v))
                .mapValues(ClientTransaction::parseTransaction)
                .groupByKey()
                .aggregate(
                        () -> clientBalance,
                        (client, transaction, aggregator) -> generateResponse(aggregator, transaction),
                        Materialized.<String, ClientBalance>as(Stores.persistentKeyValueStore("aggregated-amounts"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(clientCountSerde));

        nameCountAggregate.toStream().to(TOPIC_OUTPUT, Produced.with(Serdes.String(), clientCountSerde));

        final KafkaStreams streams = new KafkaStreams(builder.build(), StreamProperties.getStreamProperties());
        streams.setStateListener((newState, oldState) -> System.out.println("State changed from " + oldState + " to " + newState));
        streams.cleanUp(); // Cleans up local state store
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    private static ClientBalance generateResponse(final ClientBalance aggregator, final ClientTransaction transaction) {
        final long count = aggregator.count() + 1;
        final String lastTime = aggregator.time();
        final String newTime = transaction.time();
        return new ClientBalance(aggregator.amount() + transaction.amount(), count, lastTime.compareTo(newTime) > 0 ? lastTime : newTime);
    }


}
