package com.fintech.pix.messaging;

import java.math.BigDecimal;

/** Payload published to {@link Topics#PIX_REQUESTED} once a transaction is durably persisted. */
public record PixRequestedEvent(String transactionId, BigDecimal amount, String pixKey, String description) {
}
