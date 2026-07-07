package com.fintech.pix.partner;

/** Retryable: timeout, connection reset, 5xx — the kind of failure that might succeed next attempt. */
public class TransientPartnerException extends RuntimeException {

    public TransientPartnerException(String message) {
        super(message);
    }

    public TransientPartnerException(String message, Throwable cause) {
        super(message, cause);
    }
}
