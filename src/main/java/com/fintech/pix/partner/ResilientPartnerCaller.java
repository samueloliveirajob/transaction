package com.fintech.pix.partner;

import com.fintech.pix.observability.PixMetrics;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Wraps {@link PartnerFinancialInstitutionClient} with retry, circuit breaking, and a hard
 * timeout. Ordering matters: each {@code @Retry} attempt passes back through the
 * {@code @CircuitBreaker} (Resilience4j's default aspect order), so once the breaker trips
 * OPEN, further attempts fail fast with {@code CallNotPermittedException} instead of each
 * paying the full call timeout — this is what protects worker threads from piling up behind
 * a struggling or down partner.
 */
@Component
@Profile({"worker", "all"})
@RequiredArgsConstructor
public class ResilientPartnerCaller {

    private final PartnerFinancialInstitutionClient partnerClient;
    private final ExecutorService partnerCallExecutor;
    private final PixMetrics metrics;

    @Value("${app.partner.call-timeout-ms:5000}")
    private long callTimeoutMs;

    @Retry(name = "partnerFi")
    @CircuitBreaker(name = "partnerFi")
    public void transfer(String transactionId, BigDecimal amount, String pixKey) {
        Timer.Sample sample = metrics.startPartnerCall();
        boolean success = false;
        try {
            partnerCallExecutor.submit(() -> partnerClient.transfer(transactionId, amount, pixKey))
                    .get(callTimeoutMs, TimeUnit.MILLISECONDS);
            success = true;
        } catch (TimeoutException e) {
            throw new TransientPartnerException("partner call timed out for transactionId=" + transactionId, e);
        } catch (ExecutionException e) {
            switch (e.getCause()) {
                case PartnerRejectedException rejected -> throw rejected;
                case TransientPartnerException transient_ -> throw transient_;
                case null, default -> throw new TransientPartnerException(
                        "partner call failed for transactionId=" + transactionId, e.getCause());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TransientPartnerException("interrupted while calling partner for transactionId=" + transactionId, e);
        } finally {
            metrics.stopPartnerCall(sample, success);
        }
    }
}
