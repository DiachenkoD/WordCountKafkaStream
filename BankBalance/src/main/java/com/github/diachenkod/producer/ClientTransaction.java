package com.github.diachenkod.producer;

import static com.github.diachenkod.BankBalanceConstants.GSON;

public record ClientTransaction(String name, Double amount, String time) {

    public String getTransaction() {
        return GSON.toJson(this);
    }

    public static ClientTransaction parseTransaction(final String transactionLine) {
        return GSON.fromJson(transactionLine, ClientTransaction.class);
    }
}
