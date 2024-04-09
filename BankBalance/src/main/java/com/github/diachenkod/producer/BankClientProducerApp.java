package com.github.diachenkod.producer;


import java.time.LocalDateTime;
import java.util.List;

import static com.github.diachenkod.BankBalanceConstants.TOPIC_INPUT;

public class BankClientProducerApp {

    private static final List<String> clients = List.of("John", "Stephane", "Alice", "Karma", "William", "Vitaliy");

    public static void main(String[] args) {
        final BankClientProducer clientProducer = new BankClientProducer(TOPIC_INPUT);

        for (int i = 0, j = 0; i < 100; i++, j++) {
            clientProducer.produce(new ClientTransaction(clients.get(j), Math.random() * 1000 + 1, LocalDateTime.now().toString()));
            if (j == clients.size() - 1) {
                j = 0;
            }
        }

        clientProducer.flush();
    }
}