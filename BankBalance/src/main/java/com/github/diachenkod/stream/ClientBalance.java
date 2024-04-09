package com.github.diachenkod.stream;

import static com.github.diachenkod.BankBalanceConstants.GSON;

public record ClientBalance(double amount, long count, String time) {

    public String getJson() {
        return GSON.toJson(this);
    }

    public static ClientBalance parseBalance(final String balance) {
        return GSON.fromJson(balance, ClientBalance.class);
    }
}
