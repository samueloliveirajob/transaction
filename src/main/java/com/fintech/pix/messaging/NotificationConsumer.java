package com.fintech.pix.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Stand-in for a future downstream consumer (client webhook, fraud pipeline, ledger export...).
 * Demonstrates that {@code pix.processed} is a public extension point: new consumers subscribe
 * independently, with no change to the ingestion or worker code paths.
 */
@Component
@Profile({"worker", "all"})
@RequiredArgsConstructor
@Slf4j
public class NotificationConsumer {

    private final ObjectMapper objectMapper;

    @KafkaListener(topics = Topics.PIX_PROCESSED, groupId = "pix-notification-stub")
    public void onPixProcessed(String payload) throws Exception {
        PixProcessedEvent event = objectMapper.readValue(payload, PixProcessedEvent.class);
        log.info("[notification-stub] would notify client for transactionId={} status={}",
                event.transactionId(), event.status());
    }
}
