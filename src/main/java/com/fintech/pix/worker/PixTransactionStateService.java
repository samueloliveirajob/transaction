package com.fintech.pix.worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fintech.pix.domain.OutboxEvent;
import com.fintech.pix.domain.PixTransaction;
import com.fintech.pix.messaging.PixProcessedEvent;
import com.fintech.pix.messaging.Topics;
import com.fintech.pix.observability.PixMetrics;
import com.fintech.pix.repository.OutboxEventRepository;
import com.fintech.pix.repository.PixTransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

/**
 * Short, isolated DB transactions around the (potentially multi-second) partner call in
 * {@link PixProcessingService} — a transaction never spans the blocking call itself, so we
 * don't hold a pooled connection for the duration of a partner round trip.
 */
@Service
@Profile({"worker", "all"})
@RequiredArgsConstructor
@Slf4j
public class PixTransactionStateService {

    private final PixTransactionRepository transactionRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final PixMetrics metrics;

    /** Returns empty if the transaction is already terminal — the caller should treat that as a no-op (idempotent redelivery). */
    @Transactional
    Optional<PixTransaction> beginProcessing(String transactionId) {
        PixTransaction tx = require(transactionId);
        if (tx.isTerminal()) {
            log.info("idempotent skip: transactionId={} already terminal with status={}", transactionId, tx.getStatus());
            return Optional.empty();
        }
        tx.markProcessing();
        return Optional.of(tx);
    }

    @Transactional
    void confirm(String transactionId) {
        PixTransaction tx = require(transactionId);
        if (tx.isTerminal()) {
            return;
        }
        tx.markConfirmed();
        metrics.recordTerminal(tx.getStatus(), tx.getCreatedAt());
        publishProcessed(tx);
    }

    @Transactional
    void reject(String transactionId, String reason) {
        PixTransaction tx = require(transactionId);
        if (tx.isTerminal()) {
            return;
        }
        tx.markFailed(reason);
        metrics.recordTerminal(tx.getStatus(), tx.getCreatedAt());
        publishProcessed(tx);
    }

    @Transactional
    public void markFailedRetryable(String transactionId, String reason) {
        PixTransaction tx = require(transactionId);
        if (tx.isTerminal()) {
            return;
        }
        tx.markFailedRetryable(reason);
        metrics.recordTerminal(tx.getStatus(), tx.getCreatedAt());
        publishProcessed(tx);
    }

    private PixTransaction require(String transactionId) {
        return transactionRepository.findByTransactionId(transactionId)
                .orElseThrow(() -> new IllegalStateException("no transaction found for transactionId=" + transactionId));
    }

    private void publishProcessed(PixTransaction tx) {
        PixProcessedEvent event = new PixProcessedEvent(tx.getTransactionId(), tx.getStatus(), tx.getFailureReason());
        outboxEventRepository.save(OutboxEvent.of(Topics.PIX_PROCESSED, tx.getTransactionId(), serialize(event)));
    }

    private String serialize(PixProcessedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("failed to serialize " + event, e);
        }
    }
}
