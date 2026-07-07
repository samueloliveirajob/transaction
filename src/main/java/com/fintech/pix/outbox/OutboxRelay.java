package com.fintech.pix.outbox;

import com.fintech.pix.domain.OutboxEvent;
import com.fintech.pix.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Polling implementation of the transactional outbox relay. Simpler to run than CDC
 * (e.g. Debezium tailing the WAL) at the cost of publish latency bounded by the poll
 * interval and one extra DB read per tick — an acceptable trade for this scope; a
 * high-throughput production deployment would swap this for CDC without touching the
 * ingestion or worker code, since both only depend on the outbox table's contract.
 */
@Component
@Profile({"api", "all"})
@RequiredArgsConstructor
@Slf4j
public class OutboxRelay {

    private static final int BATCH_SIZE = 100;

    private final OutboxEventRepository outboxEventRepository;
    private final OutboxEventPublisher publisher;

    @Scheduled(fixedDelayString = "${app.outbox.relay-fixed-delay-ms:200}")
    @Transactional
    public void relay() {
        List<OutboxEvent> batch = outboxEventRepository.claimUnpublishedBatch(BATCH_SIZE);
        if (batch.isEmpty()) {
            return;
        }
        for (OutboxEvent event : batch) {
            try {
                publisher.publish(event);
                event.markPublished();
            } catch (Exception e) {
                log.error("failed to publish outbox event id={} topic={} key={}, will retry next tick",
                        event.getId(), event.getTopic(), event.getAggregateKey(), e);
                throw new OutboxPublishException(event.getId(), e);
            }
        }
        log.debug("relayed {} outbox events", batch.size());
    }
}
