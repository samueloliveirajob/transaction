package com.fintech.pix.messaging;

import com.fintech.pix.domain.PixStatus;

/** Payload published to {@link Topics#PIX_PROCESSED} once a worker reaches a terminal outcome. */
public record PixProcessedEvent(String transactionId, PixStatus status, String failureReason) {
}
