CREATE TABLE IF NOT EXISTS events (
    id UUID PRIMARY KEY,

    event_id UUID UNIQUE NOT NULL,

    type VARCHAR(100) NOT NULL,

    version INTEGER NOT NULL,

    source VARCHAR(100) NOT NULL,

    user_id VARCHAR(255),

    anonymous_id VARCHAR(255),

    session_id VARCHAR(255),

    timestamp TIMESTAMPTZ NOT NULL,

    properties JSONB NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_type
    ON events(type);

CREATE INDEX IF NOT EXISTS idx_events_timestamp
    ON events(timestamp);

CREATE INDEX IF NOT EXISTS idx_events_user_id
    ON events(user_id);

CREATE INDEX IF NOT EXISTS idx_events_session_id
    ON events(session_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_events_event_id
ON events(event_id);