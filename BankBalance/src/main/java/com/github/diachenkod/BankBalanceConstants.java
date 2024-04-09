package com.github.diachenkod;

import com.google.gson.Gson;

public class BankBalanceConstants {
    public static final String SERVER = "localhost:9092";
    public static final String TOPIC_INPUT = "bank-balance-input";
    public static final String TOPIC_OUTPUT = "bank-balance-output";
    public static final Gson GSON = new Gson();
}
