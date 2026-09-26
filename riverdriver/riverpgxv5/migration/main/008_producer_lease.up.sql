--
-- Persistent, renewable leases for producers (clients working jobs).
--
-- There is one row per (queue, client) producer slot. `generation` is a
-- fencing token that starts at one and is incremented every time the slot is
-- acquired, so a stale process can't heart beat a newer generation into a
-- stale state. `reaped_at` is set when the lease is released gracefully or
-- when the leader reaps it after `expires_at` has passed; rows with it set are
-- retained for a period so leader changes can't duplicate an offline
-- notification, then are deleted by the producer reaper.
--
-- Like `river_leader`, this is unlogged because it contains only ephemeral
-- state that producers re-register on startup.
--

CREATE UNLOGGED TABLE /* TEMPLATE: schema */river_producer (
    queue_name  text        NOT NULL,
    client_id   text        NOT NULL,
    producer_id bigint      NOT NULL,
    generation  bigint      NOT NULL DEFAULT 1,
    max_workers bigint      NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    expires_at  timestamptz NOT NULL,
    reaped_at   timestamptz,
    PRIMARY KEY (queue_name, client_id),
    CONSTRAINT river_producer_queue_name_length CHECK (char_length(queue_name) > 0 AND char_length(queue_name) < 128),
    CONSTRAINT river_producer_client_id_length CHECK (char_length(client_id) > 0 AND char_length(client_id) < 128),
    CONSTRAINT river_producer_generation_positive CHECK (generation > 0),
    CONSTRAINT river_producer_max_workers_positive CHECK (max_workers > 0)
);

CREATE INDEX river_producer_active_expires_at_idx ON /* TEMPLATE: schema */river_producer (expires_at)
    WHERE reaped_at IS NULL;
