package com.fintech.pix.domain;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

/**
 * Transactional outbox row: written in the same DB transaction as the business change it
 * describes, so the {@link com.fintech.pix.outbox.OutboxRelay} can publish it to Kafka
 * without ever risking "DB committed but the event was lost" or vice versa.
 */
@Entity
@Table(name = "outbox_event")
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class OutboxEvent {

    @Id
    @GeneratedValue
    private UUID id;

    /** Kafka topic this event should be published to. */
    @Column(nullable = false)
    private String topic;

    /** Kafka partition key — the transactionId, to preserve per-transaction ordering. */
    @Column(name = "aggregate_key", nullable = false)
    private String aggregateKey;

    @Column(nullable = false, columnDefinition = "text")
    private String payload;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    public static OutboxEvent of(String topic, String aggregateKey, String payload) {
        OutboxEvent event = new OutboxEvent();
        event.topic = topic;
        event.aggregateKey = aggregateKey;
        event.payload = payload;
        event.createdAt = Instant.now();
        return event;
    }

    public boolean isPublished() {
        return publishedAt != null;
    }

    public void markPublished() {
        this.publishedAt = Instant.now();
    }
}
