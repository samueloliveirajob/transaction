package com.fintech.pix.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fintech.pix.api.IdempotencyConflictException;
import com.fintech.pix.api.TransactionNotFoundException;
import com.fintech.pix.api.dto.PixRequest;
import com.fintech.pix.domain.PixStatus;
import com.fintech.pix.domain.PixTransaction;
import com.fintech.pix.observability.PixMetrics;
import com.fintech.pix.repository.OutboxEventRepository;
import com.fintech.pix.repository.PixTransactionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;

import java.math.BigDecimal;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class PixIngestionServiceTest {

    private final PixTransactionRepository transactionRepository = mock(PixTransactionRepository.class);
    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final PixMetrics metrics = mock(PixMetrics.class);
    private final PixIngestionService service =
            new PixIngestionService(transactionRepository, outboxEventRepository, new ObjectMapper(), metrics);

    private final PixRequest request = new PixRequest("tx-1", new BigDecimal("150.75"), "cliente@email.com", "fatura");

    @BeforeEach
    void setUp() {
        when(transactionRepository.findByTransactionId("tx-1")).thenReturn(Optional.empty());
    }

    @Test
    void createsTransactionAndOutboxEventOnFirstRequest() {
        PixTransaction created = service.ingest(request);

        assertThat(created.getStatus()).isEqualTo(PixStatus.RECEIVED);
        assertThat(created.getTransactionId()).isEqualTo("tx-1");
        verify(transactionRepository).saveAndFlush(any(PixTransaction.class));
        verify(outboxEventRepository).save(argThat(event ->
                event.getTopic().equals("pix.requested") && event.getAggregateKey().equals("tx-1")));
        verify(metrics).incrementReceived();
    }

    @Test
    void replayingTheSameRequestIsIdempotentAndDoesNotCreateANewRow() {
        PixTransaction existing = PixTransaction.receive("tx-1", new BigDecimal("150.75"), "cliente@email.com", "fatura");
        when(transactionRepository.findByTransactionId("tx-1")).thenReturn(Optional.of(existing));

        PixTransaction result = service.ingest(request);

        assertThat(result).isSameAs(existing);
        verify(transactionRepository, never()).saveAndFlush(any());
        verify(outboxEventRepository, never()).save(any());
    }

    @Test
    void reusingTheTransactionIdWithDifferentDataIsRejected() {
        PixTransaction existing = PixTransaction.receive("tx-1", new BigDecimal("999.00"), "other@email.com", "fatura");
        when(transactionRepository.findByTransactionId("tx-1")).thenReturn(Optional.of(existing));

        assertThatThrownBy(() -> service.ingest(request)).isInstanceOf(IdempotencyConflictException.class);
    }

    @Test
    void concurrentInsertRaceIsResolvedByReReadingTheWinningRow() {
        PixTransaction winner = PixTransaction.receive("tx-1", new BigDecimal("150.75"), "cliente@email.com", "fatura");
        when(transactionRepository.saveAndFlush(any())).thenThrow(new DataIntegrityViolationException("unique violation"));
        when(transactionRepository.findByTransactionId("tx-1"))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(winner));

        PixTransaction result = service.ingest(request);

        assertThat(result).isSameAs(winner);
        verify(outboxEventRepository, never()).save(any());
    }

    @Test
    void statusLookupThrowsWhenTransactionIsUnknown() {
        when(transactionRepository.findByTransactionId("missing")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.findByTransactionId("missing"))
                .isInstanceOf(TransactionNotFoundException.class);
    }

    @Test
    void statusLookupReturnsTheStoredTransaction() {
        PixTransaction tx = PixTransaction.receive("tx-1", new BigDecimal("150.75"), "cliente@email.com", "fatura");
        when(transactionRepository.findByTransactionId("tx-1")).thenReturn(Optional.of(tx));

        assertThat(service.findByTransactionId("tx-1")).isSameAs(tx);
    }
}
