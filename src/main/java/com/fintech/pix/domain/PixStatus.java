package com.fintech.pix.domain;

public enum PixStatus {
    /** Persisted by the API, not yet handed off to the worker. */
    RECEIVED,
    /** Picked up by a worker, partner call in flight (including retries). */
    PROCESSING,
    /** Partner confirmed the transfer. Terminal. */
    CONFIRMED,
    /** Partner permanently rejected the transaction (business rule). Terminal. */
    FAILED,
    /** Retries against the partner were exhausted; routed to the DLT. Needs manual reconciliation. */
    FAILED_RETRYABLE
}
