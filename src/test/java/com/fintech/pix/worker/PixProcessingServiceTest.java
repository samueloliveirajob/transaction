package com.fintech.pix.worker;

import com.fintech.pix.domain.PixTransaction;
import com.fintech.pix.messaging.PixRequestedEvent;
import com.fintech.pix.partner.PartnerRejectedException;
import com.fintech.pix.partner.ResilientPartnerCaller;
import com.fintech.pix.partner.TransientPartnerException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class PixProcessingServiceTest {

    private final PixTransactionStateService stateService = mock(PixTransactionStateService.class);
    private final ResilientPartnerCaller partnerCaller = mock(ResilientPartnerCaller.class);
    private final PixProcessingService service = new PixProcessingService(stateService, partnerCaller);

    private final PixRequestedEvent event =
            new PixRequestedEvent("tx-1", new BigDecimal("150.75"), "cliente@email.com", "fatura");

    @Test
    void confirmsWhenThePartnerAcceptsTheTransfer() {
        PixTransaction tx = PixTransaction.receive("tx-1", new BigDecimal("150.75"), "cliente@email.com", "fatura");
        when(stateService.beginProcessing("tx-1")).thenReturn(Optional.of(tx));

        service.process(event);

        verify(partnerCaller).transfer("tx-1", new BigDecimal("150.75"), "cliente@email.com");
        verify(stateService).confirm("tx-1");
        verify(stateService, never()).reject(any(), any());
    }

    @Test
    void rejectsWhenThePartnerPermanentlyRejects() {
        PixTransaction tx = PixTransaction.receive("tx-1", new BigDecimal("150.75"), "cliente@email.com", "fatura");
        when(stateService.beginProcessing("tx-1")).thenReturn(Optional.of(tx));
        doThrow(new PartnerRejectedException("invalid pixKey"))
                .when(partnerCaller).transfer(eq("tx-1"), any(), any());

        service.process(event);

        verify(stateService).reject("tx-1", "invalid pixKey");
        verify(stateService, never()).confirm(any());
    }

    @Test
    void skipsProcessingWhenTheTransactionIsAlreadyTerminal() {
        when(stateService.beginProcessing("tx-1")).thenReturn(Optional.empty());

        service.process(event);

        verifyNoInteractions(partnerCaller);
        verify(stateService, never()).confirm(any());
        verify(stateService, never()).reject(any(), any());
    }

    @Test
    void letsTransientFailuresPropagateForKafkaLevelRetryAndDlt() {
        PixTransaction tx = PixTransaction.receive("tx-1", new BigDecimal("150.75"), "cliente@email.com", "fatura");
        when(stateService.beginProcessing("tx-1")).thenReturn(Optional.of(tx));
        doThrow(new TransientPartnerException("timed out"))
                .when(partnerCaller).transfer(eq("tx-1"), any(), any());

        assertThatThrownBy(() -> service.process(event)).isInstanceOf(TransientPartnerException.class);

        verify(stateService, never()).confirm(any());
        verify(stateService, never()).reject(any(), any());
    }
}
