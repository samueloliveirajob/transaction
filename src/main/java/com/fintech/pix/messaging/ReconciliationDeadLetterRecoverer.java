package com.fintech.pix.messaging;

import com.fintech.pix.worker.PixTransactionStateService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.stereotype.Component;

/**
 * Fires once the container's backoff attempts for {@code pix.requested} are exhausted.
 * Marks the transaction {@code FAILED_RETRYABLE} (so it surfaces for manual reconciliation
 * instead of sitting invisibly in PROCESSING forever) and then delegates to Kafka's own
 * dead-letter recoverer to actually publish the poison record to {@code pix.requested.dlt}.
 */
@Component
@Profile({"worker", "all"})
@RequiredArgsConstructor
@Slf4j
class ReconciliationDeadLetterRecoverer implements ConsumerRecordRecoverer {

    private final PixTransactionStateService stateService;
    private final DeadLetterPublishingRecoverer delegate;

    @Override
    public void accept(ConsumerRecord<?, ?> record, Exception exception) {
        String transactionId = String.valueOf(record.key());
        log.error("exhausted retries for transactionId={}, routing to DLT: {}", transactionId, exception.getMessage());
        try {
            stateService.markFailedRetryable(transactionId, exception.getMessage());
        } catch (Exception e) {
            // Best-effort: the record must still reach the DLT even if this local update fails.
            log.warn("could not mark transactionId={} as FAILED_RETRYABLE", transactionId, e);
        }
        delegate.accept(record, exception);
    }
}
