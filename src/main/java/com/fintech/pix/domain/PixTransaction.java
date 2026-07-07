package com.fintech.pix.domain;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Aggregate root for a PIX transfer request. {@code transactionId} is the client-supplied
 * idempotency key (unique constraint at the DB level) — a repeated POST with the same
 * transactionId must never create a second row.
 */
@Entity
@Table(name = "pix_transaction")
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class PixTransaction {

    @Id
    @GeneratedValue
    private UUID id;

    @Column(name = "transaction_id", nullable = false, unique = true, updatable = false)
    private String transactionId;

    @Column(nullable = false, precision = 19, scale = 2)
    private BigDecimal amount;

    @Column(name = "pix_key", nullable = false)
    private String pixKey;

    private String description;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private PixStatus status;

    @Column(name = "failure_reason")
    private String failureReason;

    /** Optimistic lock: guards the check-then-act status transition against concurrent redeliveries. */
    @Version
    private Long version;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    public static PixTransaction receive(String transactionId, BigDecimal amount, String pixKey, String description) {
        PixTransaction tx = new PixTransaction();
        tx.transactionId = transactionId;
        tx.amount = amount;
        tx.pixKey = pixKey;
        tx.description = description;
        tx.status = PixStatus.RECEIVED;
        Instant now = Instant.now();
        tx.createdAt = now;
        tx.updatedAt = now;
        return tx;
    }

    public void markProcessing() {
        this.status = PixStatus.PROCESSING;
        this.updatedAt = Instant.now();
    }

    public void markConfirmed() {
        this.status = PixStatus.CONFIRMED;
        this.updatedAt = Instant.now();
    }

    public void markFailed(String reason) {
        this.status = PixStatus.FAILED;
        this.failureReason = reason;
        this.updatedAt = Instant.now();
    }

    public void markFailedRetryable(String reason) {
        this.status = PixStatus.FAILED_RETRYABLE;
        this.failureReason = reason;
        this.updatedAt = Instant.now();
    }

    public boolean isTerminal() {
        return status == PixStatus.CONFIRMED || status == PixStatus.FAILED || status == PixStatus.FAILED_RETRYABLE;
    }
}
