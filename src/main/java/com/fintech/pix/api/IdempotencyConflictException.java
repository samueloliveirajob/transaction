package com.fintech.pix.api;

/**
 * Thrown when a POST /pix reuses a {@code transactionId} that already exists but with a
 * different amount/pixKey — i.e. it isn't a safe retry of the same request, it's a key collision.
 */
public class IdempotencyConflictException extends RuntimeException {

    public IdempotencyConflictException(String transactionId) {
        super("transactionId '%s' already exists with different request data".formatted(transactionId));
    }
}
