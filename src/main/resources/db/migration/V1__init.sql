CREATE TABLE pix_transaction (
    id              UUID PRIMARY KEY,
    transaction_id  VARCHAR(128) NOT NULL,
    amount          NUMERIC(19, 2) NOT NULL,
    pix_key         VARCHAR(256) NOT NULL,
    description     VARCHAR(512),
    status          VARCHAR(32) NOT NULL,
    failure_reason  VARCHAR(512),
    version         BIGINT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL,
    CONSTRAINT uq_pix_transaction_transaction_id UNIQUE (transaction_id)
);

CREATE INDEX idx_pix_transaction_status ON pix_transaction (status);

CREATE TABLE outbox_event (
    id              UUID PRIMARY KEY,
    topic           VARCHAR(256) NOT NULL,
    aggregate_key   VARCHAR(128) NOT NULL,
    payload         TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    published_at    TIMESTAMPTZ
);

-- Partial index: the relay only ever scans unpublished rows, so keep that scan cheap
-- regardless of how large the (append-only, rarely-deleted) outbox table grows.
CREATE INDEX idx_outbox_event_unpublished ON outbox_event (created_at) WHERE published_at IS NULL;
