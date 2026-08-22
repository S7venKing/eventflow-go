CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY,

    event_id UUID NOT NULL,

    event_type VARCHAR(100) NOT NULL,

    payload JSONB NOT NULL,

    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',

    attempts INT NOT NULL DEFAULT 0,

    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    published_at TIMESTAMPTZ NULL,

    last_error TEXT NULL,

    CONSTRAINT uq_outbox_events_event_id
        UNIQUE (event_id),

    CONSTRAINT fk_outbox_events_event
        FOREIGN KEY (event_id)
        REFERENCES events(event_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_outbox_events_pending
ON outbox_events (
    status,
    available_at
);

ALTER TABLE outbox_events
ADD COLUMN IF NOT EXISTS processing_at TIMESTAMPTZ;

ALTER TABLE outbox_events
DROP CONSTRAINT IF EXISTS chk_outbox_events_status;

ALTER TABLE outbox_events
ADD CONSTRAINT chk_outbox_events_status
CHECK (
    status IN (
        'PENDING',
        'PROCESSING',
        'PUBLISHED',
        'CLOSE'
    )
);