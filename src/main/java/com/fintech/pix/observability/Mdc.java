package com.fintech.pix.observability;

import org.slf4j.MDC;

import java.util.function.Supplier;

/** Puts the business transactionId into every log line for the duration of one call chain. */
public final class Mdc {

    private static final String TRANSACTION_ID_KEY = "transactionId";

    private Mdc() {
    }

    public static <T> T call(String transactionId, Supplier<T> action) {
        String previous = MDC.get(TRANSACTION_ID_KEY);
        MDC.put(TRANSACTION_ID_KEY, transactionId);
        try {
            return action.get();
        } finally {
            if (previous != null) {
                MDC.put(TRANSACTION_ID_KEY, previous);
            } else {
                MDC.remove(TRANSACTION_ID_KEY);
            }
        }
    }

    public static void run(String transactionId, Runnable action) {
        call(transactionId, () -> {
            action.run();
            return null;
        });
    }
}
