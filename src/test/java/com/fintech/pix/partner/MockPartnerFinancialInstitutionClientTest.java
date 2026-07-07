package com.fintech.pix.partner;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MockPartnerFinancialInstitutionClientTest {

    @Test
    void neverFailsWhenFailureRateIsZero() {
        var client = new MockPartnerFinancialInstitutionClient(0, 0.0, 0.2);

        assertThatCode(() -> client.transfer("tx-1", BigDecimal.TEN, "key@bank.com")).doesNotThrowAnyException();
    }

    @Test
    void alwaysThrowsWhenFailureRateIsOne() {
        var client = new MockPartnerFinancialInstitutionClient(0, 1.0, 0.0);

        assertThatThrownBy(() -> client.transfer("tx-1", BigDecimal.TEN, "key@bank.com"))
                .isInstanceOf(TransientPartnerException.class);
    }

    @Test
    void permanentRejectionShareControlsTheSplitBetweenTransientAndPermanentFailures() {
        var client = new MockPartnerFinancialInstitutionClient(0, 1.0, 1.0);

        assertThatThrownBy(() -> client.transfer("tx-1", BigDecimal.TEN, "key@bank.com"))
                .isInstanceOf(PartnerRejectedException.class);
    }
}
