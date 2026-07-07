package com.fintech.pix.api.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;

public record PixRequest(
        @NotBlank(message = "transactionId is required")
        @Size(max = 128)
        String transactionId,

        @NotNull(message = "amount is required")
        @DecimalMin(value = "0.01", message = "amount must be greater than zero")
        BigDecimal amount,

        @NotBlank(message = "pixKey is required")
        @Size(max = 256)
        String pixKey,

        @Size(max = 512)
        String description
) {
}
