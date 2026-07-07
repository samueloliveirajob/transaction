package com.fintech.pix.observability;

import com.fintech.pix.domain.PixStatus;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

/**
 * Custom business metrics, exported alongside Spring's HTTP/JVM metrics and Resilience4j's
 * own auto-bound circuit-breaker/retry metrics at {@code /actuator/prometheus}.
 */
@Component
public class PixMetrics {

    private final MeterRegistry registry;

    public PixMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public void incrementReceived() {
        Counter.builder("pix.transactions")
                .tag("status", PixStatus.RECEIVED.name())
                .register(registry)
                .increment();
    }

    /** Records both the terminal-status counter and the RECEIVED-to-terminal processing duration. */
    public void recordTerminal(PixStatus status, Instant receivedAt) {
        Counter.builder("pix.transactions")
                .tag("status", status.name())
                .register(registry)
                .increment();
        Timer.builder("pix.processing.duration")
                .description("time from ingestion to a terminal outcome")
                .register(registry)
                .record(Duration.between(receivedAt, Instant.now()));
    }

    public Timer.Sample startPartnerCall() {
        return Timer.start(registry);
    }

    public void stopPartnerCall(Timer.Sample sample, boolean success) {
        sample.stop(Timer.builder("pix.partner.call.duration")
                .description("latency of one partner call attempt, including this library's own retries")
                .tag("outcome", success ? "success" : "failure")
                .register(registry));
    }
}
