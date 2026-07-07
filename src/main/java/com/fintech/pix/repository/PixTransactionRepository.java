package com.fintech.pix.repository;

import com.fintech.pix.domain.PixTransaction;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface PixTransactionRepository extends JpaRepository<PixTransaction, UUID> {

    Optional<PixTransaction> findByTransactionId(String transactionId);
}
