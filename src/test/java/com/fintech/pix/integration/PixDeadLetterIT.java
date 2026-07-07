package com.fintech.pix.integration;

import com.fintech.pix.api.dto.PixRequest;
import com.fintech.pix.api.dto.PixResponse;
import com.fintech.pix.domain.PixStatus;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Forces every partner call to fail transiently so retries exhaust and the message lands
 * on the DLT. Verifies the reconciliation recoverer marks the transaction FAILED_RETRYABLE
 * instead of leaving it stuck invisibly in PROCESSING — the operational failure mode a real
 * partner outage would produce.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("all")
@Testcontainers
class PixDeadLetterIT {

    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("pix").withUsername("pix").withPassword("pix");

    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @BeforeAll
    static void startContainers() {
        POSTGRES.start();
        KAFKA.start();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("app.partner.latency-ms", () -> "10");
        registry.add("app.partner.failure-rate", () -> "1.0");
        registry.add("app.partner.permanent-rejection-share", () -> "0.0");
        registry.add("app.outbox.relay-fixed-delay-ms", () -> "50");
        registry.add("app.kafka.consumer-error-backoff-interval-ms", () -> "200");
        registry.add("app.kafka.consumer-error-max-attempts", () -> "2");
        registry.add("resilience4j.retry.instances.partnerFi.max-attempts", () -> "1");
        registry.add("resilience4j.circuitbreaker.instances.partnerFi.minimum-number-of-calls", () -> "100");
    }

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void sustainedPartnerFailureRoutesToDltAndMarksFailedRetryable() {
        String transactionId = "tx-" + UUID.randomUUID();
        PixRequest request = new PixRequest(transactionId, new BigDecimal("10.00"), "sempre-falha@email.com", "forcado a falhar");

        ResponseEntity<PixResponse> postResponse = restTemplate.postForEntity("/pix", request, PixResponse.class);
        assertThat(postResponse.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            PixResponse status = restTemplate.getForObject("/pix/" + transactionId, PixResponse.class);
            assertThat(status.status()).isEqualTo(PixStatus.FAILED_RETRYABLE);
        });
    }
}
