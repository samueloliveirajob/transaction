package com.fintech.pix.partner;

import java.math.BigDecimal;

/**
 * Boundary to the partner financial institution. Per the prompt, this integration cannot be
 * changed and keeps its current ~2s latency — the whole point of this platform's redesign is
 * to keep that latency off the client's synchronous request path, not to speed up the call itself.
 */
public interface PartnerFinancialInstitutionClient {

    /**
     * Attempts the transfer.
     *
     * @throws TransientPartnerException if the failure is likely transient and worth retrying
     * @throws PartnerRejectedException  if the partner permanently rejected the transfer
     */
    void transfer(String transactionId, BigDecimal amount, String pixKey);
}
