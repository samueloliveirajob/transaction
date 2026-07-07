package com.fintech.pix.api;

import com.fintech.pix.api.dto.PixRequest;
import com.fintech.pix.api.dto.PixResponse;
import com.fintech.pix.domain.PixStatus;
import com.fintech.pix.domain.PixTransaction;
import com.fintech.pix.observability.Mdc;
import com.fintech.pix.service.PixIngestionService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/pix")
@RequiredArgsConstructor
@Profile({"api", "all"})
public class PixController {

    private final PixIngestionService ingestionService;

    @PostMapping
    public ResponseEntity<PixResponse> submit(@Valid @RequestBody PixRequest request) {
        PixTransaction transaction = Mdc.call(request.transactionId(), () -> ingestionService.ingest(request));
        // Newly-created transactions are RECEIVED; an idempotent replay of an already-settled
        // transaction returns its current (possibly terminal) status with the same 202 —
        // the client already has that receipt, this just repeats it safely.
        HttpStatus status = transaction.getStatus() == PixStatus.RECEIVED ? HttpStatus.ACCEPTED : HttpStatus.OK;
        return ResponseEntity.status(status).body(PixResponse.from(transaction));
    }

    @GetMapping("/{transactionId}")
    public ResponseEntity<PixResponse> getStatus(@PathVariable String transactionId) {
        PixTransaction transaction = Mdc.call(transactionId, () -> ingestionService.findByTransactionId(transactionId));
        return ResponseEntity.ok(PixResponse.from(transaction));
    }
}
