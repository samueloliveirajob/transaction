package com.fintech.pix.outbox;

import com.fintech.pix.domain.OutboxEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
class OutboxEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;

    /** Blocks until the broker acknowledges (acks=all) so a thrown exception reliably means "not published". */
    void publish(OutboxEvent event) throws Exception {
        kafkaTemplate.send(event.getTopic(), event.getAggregateKey(), event.getPayload()).get();
    }
}
