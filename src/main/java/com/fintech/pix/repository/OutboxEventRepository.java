package com.fintech.pix.repository;

import com.fintech.pix.domain.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    /**
     * {@code FOR UPDATE SKIP LOCKED} lets multiple API replicas run the relay concurrently
     * without fighting over the same rows: each instance claims a disjoint batch instead of
     * blocking on (or duplicating) another instance's in-flight rows.
     */
    @Query(value = """
            select * from outbox_event
            where published_at is null
            order by created_at asc
            limit :limit
            for update skip locked
            """, nativeQuery = true)
    List<OutboxEvent> claimUnpublishedBatch(@Param("limit") int limit);
}
