CREATE TABLE events (
    id UUID PRIMARY KEY,
    type VARCHAR(100) NOT NULL,
    version INT NOT NULL,
    source VARCHAR(100) NOT NULL,

    user_id VARCHAR(255),
    anonymous_id VARCHAR(255),
    session_id VARCHAR(255),

    timestamp TIMESTAMPTZ NOT NULL,
    properties JSONB NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_events_type
ON events(type);

CREATE INDEX idx_events_timestamp
ON events(timestamp);

CREATE INDEX idx_events_user_id
ON events(user_id);

CREATE INDEX idx_events_session_id
ON events(session_id);