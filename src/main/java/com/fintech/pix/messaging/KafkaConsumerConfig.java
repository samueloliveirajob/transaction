package com.fintech.pix.messaging;

import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Two retry horizons work together here: Resilience4j (see {@code ResilientPartnerCaller})
 * retries within a single message delivery over milliseconds/seconds; this container-level
 * backoff retries whole message deliveries over seconds, for failures that outlast the
 * in-process retry budget (e.g. a sustained partner outage). Only after both are exhausted
 * does a message reach the DLT.
 */
@Configuration
@EnableKafka
@Profile({"worker", "all"})
public class KafkaConsumerConfig {

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(KafkaOperations<Object, Object> kafkaOperations) {
        return new DeadLetterPublishingRecoverer(kafkaOperations,
                (record, ex) -> new TopicPartition(Topics.PIX_REQUESTED_DLT, -1));
    }

    @Bean
    public CommonErrorHandler kafkaErrorHandler(
            ConsumerRecordRecoverer reconciliationDeadLetterRecoverer,
            @Value("${app.kafka.consumer-error-backoff-interval-ms:2000}") long backoffIntervalMs,
            @Value("${app.kafka.consumer-error-max-attempts:4}") long maxAttempts) {
        FixedBackOff backOff = new FixedBackOff(backoffIntervalMs, maxAttempts - 1);
        return new DefaultErrorHandler(reconciliationDeadLetterRecoverer, backOff);
    }
}
