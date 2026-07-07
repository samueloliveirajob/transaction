package com.fintech.pix.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fintech.pix.api.IdempotencyConflictException;
import com.fintech.pix.api.TransactionNotFoundException;
import com.fintech.pix.api.dto.PixRequest;
import com.fintech.pix.domain.OutboxEvent;
import com.fintech.pix.domain.PixTransaction;
import com.fintech.pix.messaging.PixRequestedEvent;
import com.fintech.pix.messaging.Topics;
import com.fintech.pix.observability.PixMetrics;
import com.fintech.pix.repository.OutboxEventRepository;
import com.fintech.pix.repository.PixTransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

/**
 * Owns the synchronous, client-facing part of the pipeline: validate, persist idempotently,
 * and hand off to the async pipeline via a transactional outbox row written in the same
 * DB transaction as the transaction row itself. Nothing here talks to Kafka or the partner —
 * that's why this path stays fast regardless of partner latency.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class PixIngestionService {

    private final PixTransactionRepository transactionRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final PixMetrics metrics;

    @Transactional
    public PixTransaction ingest(PixRequest request) {
        Optional<PixTransaction> existing = transactionRepository.findByTransactionId(request.transactionId());
        if (existing.isPresent()) {
            return requireMatchingReplay(existing.get(), request);
        }

        PixTransaction transaction = PixTransaction.receive(
                request.transactionId(), request.amount(), request.pixKey(), request.description());

        try {
            transactionRepository.saveAndFlush(transaction);
        } catch (DataIntegrityViolationException raceLoserOnUniqueConstraint) {
            // Two concurrent requests with the same transactionId: the loser lands here instead
            // of creating a duplicate row. Treat it the same as an idempotent replay.
            PixTransaction winner = transactionRepository.findByTransactionId(request.transactionId())
                    .orElseThrow(() -> raceLoserOnUniqueConstraint);
            return requireMatchingReplay(winner, request);
        }

        outboxEventRepository.save(OutboxEvent.of(
                Topics.PIX_REQUESTED,
                transaction.getTransactionId(),
                serialize(new PixRequestedEvent(
                        transaction.getTransactionId(), transaction.getAmount(),
                        transaction.getPixKey(), transaction.getDescription()))));

        metrics.incrementReceived();
        log.info("pix transaction received transactionId={} amount={}", transaction.getTransactionId(), transaction.getAmount());
        return transaction;
    }

    @Transactional(readOnly = true)
    public PixTransaction findByTransactionId(String transactionId) {
        return transactionRepository.findByTransactionId(transactionId)
                .orElseThrow(() -> new TransactionNotFoundException(transactionId));
    }

    private PixTransaction requireMatchingReplay(PixTransaction existing, PixRequest request) {
        boolean sameRequest = existing.getAmount().compareTo(request.amount()) == 0
                && existing.getPixKey().equals(request.pixKey());
        if (!sameRequest) {
            throw new IdempotencyConflictException(request.transactionId());
        }
        log.info("idempotent replay for transactionId={}, returning existing status={}",
                request.transactionId(), existing.getStatus());
        return existing;
    }

    private String serialize(PixRequestedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("failed to serialize " + event, e);
        }
    }
}
