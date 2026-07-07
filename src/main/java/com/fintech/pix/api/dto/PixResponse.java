package com.fintech.pix.api.dto;

import com.fintech.pix.domain.PixStatus;
import com.fintech.pix.domain.PixTransaction;

import java.math.BigDecimal;
import java.time.Instant;

public record PixResponse(
        String transactionId,
        PixStatus status,
        BigDecimal amount,
        String pixKey,
        String description,
        String failureReason,
        Instant createdAt,
        Instant updatedAt
) {
    public static PixResponse from(PixTransaction tx) {
        return new PixResponse(
                tx.getTransactionId(),
                tx.getStatus(),
                tx.getAmount(),
                tx.getPixKey(),
                tx.getDescription(),
                tx.getFailureReason(),
                tx.getCreatedAt(),
                tx.getUpdatedAt()
        );
    }
}
