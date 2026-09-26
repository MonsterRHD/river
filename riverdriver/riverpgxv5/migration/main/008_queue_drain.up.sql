--
-- Queue drain (handoff).
--
-- A row represents a single drain handoff for a queue: 'draining' while the
-- queue's running jobs are being drained, 'drained' once the running count has
-- reached zero (the queue remains gated for fetching), and 'resumed' once
-- fetching has been explicitly restored. The partial unique index below
-- enforces that at most one active ('draining' or 'drained') handoff exists
-- per queue, while the (queue, key) primary key makes retries carrying the
-- same idempotency key idempotent and preserves completed handoffs.
--

CREATE TABLE /* TEMPLATE: schema */river_queue_drain (
    queue text NOT NULL,
    key text NOT NULL,
    state text NOT NULL CHECK (state IN ('draining', 'drained', 'resumed')),
    created_at timestamptz NOT NULL DEFAULT now(),
    drained_at timestamptz,
    resumed_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (queue, key),
    CONSTRAINT river_queue_drain_queue_length CHECK (char_length(queue) > 0 AND char_length(queue) < 128),
    CONSTRAINT river_queue_drain_key_length CHECK (char_length(key) > 0 AND char_length(key) <= 128)
);

CREATE UNIQUE INDEX river_queue_drain_active_queue_idx ON /* TEMPLATE: schema */river_queue_drain (queue)
    WHERE state IN ('draining', 'drained');
