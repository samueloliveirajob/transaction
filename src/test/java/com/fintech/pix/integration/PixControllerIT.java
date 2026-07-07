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
 * Exercises the full async pipeline end-to-end: HTTP -> Postgres -> outbox -> Kafka ->
 * worker -> partner mock -> Postgres, against real Postgres and Kafka containers.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("all")
@Testcontainers
class PixControllerIT {

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
        registry.add("app.partner.latency-ms", () -> "50");
        registry.add("app.partner.failure-rate", () -> "0.0");
        registry.add("app.outbox.relay-fixed-delay-ms", () -> "50");
    }

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void happyPathGoesFromReceivedToConfirmed() {
        String transactionId = "tx-" + UUID.randomUUID();
        PixRequest request = new PixRequest(transactionId, new BigDecimal("150.75"), "cliente@email.com", "Pagamento de fatura");

        ResponseEntity<PixResponse> postResponse = restTemplate.postForEntity("/pix", request, PixResponse.class);

        assertThat(postResponse.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(postResponse.getBody().status()).isEqualTo(PixStatus.RECEIVED);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            PixResponse status = restTemplate.getForObject("/pix/" + transactionId, PixResponse.class);
            assertThat(status.status()).isEqualTo(PixStatus.CONFIRMED);
        });
    }

    @Test
    void duplicatePostIsIdempotentAndDoesNotReprocess() {
        String transactionId = "tx-" + UUID.randomUUID();
        PixRequest request = new PixRequest(transactionId, new BigDecimal("42.00"), "outro@email.com", "teste idempotencia");

        ResponseEntity<PixResponse> first = restTemplate.postForEntity("/pix", request, PixResponse.class);
        ResponseEntity<PixResponse> second = restTemplate.postForEntity("/pix", request, PixResponse.class);

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(second.getBody().transactionId()).isEqualTo(first.getBody().transactionId());
        assertThat(second.getBody().createdAt()).isEqualTo(first.getBody().createdAt());
    }

    @Test
    void unknownTransactionReturns404() {
        // Response type is the generic error body here, not PixResponse — its "status" field
        // is an HTTP status code (int), which would collide with PixResponse's PixStatus enum field.
        ResponseEntity<String> response = restTemplate.getForEntity("/pix/does-not-exist", String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }
}
