package com.fintech.pix.api;

public class TransactionNotFoundException extends RuntimeException {

    public TransactionNotFoundException(String transactionId) {
        super("transaction '%s' not found".formatted(transactionId));
    }
}
