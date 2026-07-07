package com.fintech.pix.partner;

/** Terminal: the partner explicitly rejected the transfer (e.g. invalid key). Never retried. */
public class PartnerRejectedException extends RuntimeException {

    public PartnerRejectedException(String message) {
        super(message);
    }
}
