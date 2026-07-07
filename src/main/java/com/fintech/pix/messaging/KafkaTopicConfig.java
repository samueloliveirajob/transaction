package com.fintech.pix.messaging;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Partition count bounds how many worker instances can consume {@code pix.requested} in
 * parallel (one consumer per partition per consumer group) while {@code transactionId} keying
 * still guarantees per-transaction ordering. 12 is a starting point sized for local/dev; bump
 * it (and worker replica count) as volume grows — repartitioning later requires a migration,
 * so production would size this from real traffic projections rather than picking loosely.
 */
@Configuration
public class KafkaTopicConfig {

    @Value("${app.kafka.replication-factor:1}")
    private short replicationFactor;

    @Value("${app.kafka.pix-requested-partitions:12}")
    private int pixRequestedPartitions;

    @Bean
    public NewTopic pixRequestedTopic() {
        return TopicBuilder.name(Topics.PIX_REQUESTED)
                .partitions(pixRequestedPartitions)
                .replicas(replicationFactor)
                .build();
    }

    @Bean
    public NewTopic pixRequestedDltTopic() {
        return TopicBuilder.name(Topics.PIX_REQUESTED_DLT)
                .partitions(pixRequestedPartitions)
                .replicas(replicationFactor)
                .build();
    }

    @Bean
    public NewTopic pixProcessedTopic() {
        return TopicBuilder.name(Topics.PIX_PROCESSED)
                .partitions(6)
                .replicas(replicationFactor)
                .build();
    }
}
