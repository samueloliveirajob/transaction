package com.fintech.pix.partner;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Stand-in for the real partner institution integration, which the prompt states cannot be
 * altered. Simulates its ~2s latency and a configurable failure rate, split between transient
 * (retryable) and permanent (business rejection) outcomes, so the rest of the pipeline's
 * resilience behavior is reproducible and tunable via {@code app.partner.*} without a real
 * external dependency.
 */
@Component
@Profile({"worker", "all"})
@Slf4j
public class MockPartnerFinancialInstitutionClient implements PartnerFinancialInstitutionClient {

    private final long latencyMs;
    private final double failureRate;
    private final double permanentRejectionShare;

    public MockPartnerFinancialInstitutionClient(
            @Value("${app.partner.latency-ms:2000}") long latencyMs,
            @Value("${app.partner.failure-rate:0.15}") double failureRate,
            @Value("${app.partner.permanent-rejection-share:0.2}") double permanentRejectionShare) {
        this.latencyMs = latencyMs;
        this.failureRate = failureRate;
        this.permanentRejectionShare = permanentRejectionShare;
    }

    @Override
    public void transfer(String transactionId, BigDecimal amount, String pixKey) {
        simulateNetworkLatency();

        double roll = ThreadLocalRandom.current().nextDouble();
        if (roll >= failureRate) {
            log.debug("partner confirmed transactionId={}", transactionId);
            return;
        }

        if (roll < failureRate * permanentRejectionShare) {
            throw new PartnerRejectedException("partner rejected pixKey '%s' for transactionId=%s"
                    .formatted(pixKey, transactionId));
        }
        throw new TransientPartnerException("partner timed out / transient error for transactionId=" + transactionId);
    }

    private void simulateNetworkLatency() {
        try {
            Thread.sleep(latencyMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TransientPartnerException("interrupted while calling partner", e);
        }
    }
}
