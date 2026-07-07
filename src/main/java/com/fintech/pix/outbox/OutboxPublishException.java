package com.fintech.pix.outbox;

import java.util.UUID;

/** Rolls back the relay's batch transaction so unpublished rows stay claimable on the next tick. */
public class OutboxPublishException extends RuntimeException {

    public OutboxPublishException(UUID eventId, Throwable cause) {
        super("failed to publish outbox event " + eventId, cause);
    }
}
