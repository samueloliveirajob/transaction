package com.fintech.pix.worker;

import com.fintech.pix.domain.PixTransaction;
import com.fintech.pix.messaging.PixRequestedEvent;
import com.fintech.pix.partner.PartnerRejectedException;
import com.fintech.pix.partner.ResilientPartnerCaller;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.util.Optional;

/**
 * Orchestrates one attempt at settling a PIX transfer. Transient failures (including a tripped
 * circuit breaker) are left to propagate out of {@link #process} — the Kafka listener's error
 * handler is what applies container-level backoff and, eventually, routes to the DLT. Only
 * the two known-terminal outcomes (confirmed, permanently rejected) are handled here.
 */
@Service
@Profile({"worker", "all"})
@RequiredArgsConstructor
@Slf4j
public class PixProcessingService {

    private final PixTransactionStateService stateService;
    private final ResilientPartnerCaller partnerCaller;

    public void process(PixRequestedEvent event) {
        Optional<PixTransaction> maybeTx = stateService.beginProcessing(event.transactionId());
        if (maybeTx.isEmpty()) {
            return;
        }
        PixTransaction tx = maybeTx.get();

        try {
            partnerCaller.transfer(tx.getTransactionId(), tx.getAmount(), tx.getPixKey());
            stateService.confirm(tx.getTransactionId());
            log.info("transactionId={} CONFIRMED", tx.getTransactionId());
        } catch (PartnerRejectedException rejected) {
            stateService.reject(tx.getTransactionId(), rejected.getMessage());
            log.info("transactionId={} FAILED (rejected): {}", tx.getTransactionId(), rejected.getMessage());
        }
    }
}
