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
-- As with the other River tables, this is ordinary SQLite table; lease state
-- is ephemeral and producers re-register on startup.
--

CREATE TABLE /* TEMPLATE: schema */river_producer (
    queue_name  text      NOT NULL,
    client_id   text      NOT NULL,
    producer_id integer   NOT NULL,
    generation  integer   NOT NULL DEFAULT 1,
    max_workers integer   NOT NULL,
    created_at  timestamp NOT NULL DEFAULT (datetime('now', 'subsec')),
    updated_at  timestamp NOT NULL DEFAULT (datetime('now', 'subsec')),
    expires_at  timestamp NOT NULL,
    reaped_at   timestamp,
    PRIMARY KEY (queue_name, client_id),
    CONSTRAINT river_producer_queue_name_length CHECK (length(queue_name) > 0 AND length(queue_name) < 128),
    CONSTRAINT river_producer_client_id_length CHECK (length(client_id) > 0 AND length(client_id) < 128),
    CONSTRAINT river_producer_generation_positive CHECK (generation > 0),
    CONSTRAINT river_producer_max_workers_positive CHECK (max_workers > 0)
);

CREATE INDEX /* TEMPLATE: schema */river_producer_active_expires_at_idx ON river_producer (expires_at)
    WHERE reaped_at IS NULL;
