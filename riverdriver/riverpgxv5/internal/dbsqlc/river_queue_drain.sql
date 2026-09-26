CREATE TABLE river_queue_drain (
    queue text NOT NULL,
    key text NOT NULL,
    state text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    drained_at timestamptz,
    resumed_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (queue, key)
);

-- name: QueueDrainInsert :one
INSERT INTO /* TEMPLATE: schema */river_queue_drain (
    queue,
    key,
    state,
    created_at,
    updated_at
) VALUES (
    @queue::text,
    @key::text,
    'draining',
    coalesce(sqlc.narg('now')::timestamptz, now()),
    coalesce(sqlc.narg('now')::timestamptz, now())
) ON CONFLICT DO NOTHING
RETURNING queue, key, state, created_at, drained_at, resumed_at, updated_at;

-- name: QueueDrainGetActive :one
SELECT
    d.queue,
    d.key,
    d.state,
    d.created_at,
    d.drained_at,
    d.resumed_at,
    d.updated_at,
    (
        SELECT count(*)
        FROM /* TEMPLATE: schema */river_job
        WHERE river_job.queue = d.queue
            AND river_job.state = 'running'
    ) AS running_count
FROM /* TEMPLATE: schema */river_queue_drain d
WHERE d.queue = @queue::text
    AND d.state IN ('draining', 'drained');

-- name: QueueDrainGetByKey :one
SELECT
    d.queue,
    d.key,
    d.state,
    d.created_at,
    d.drained_at,
    d.resumed_at,
    d.updated_at,
    (
        SELECT count(*)
        FROM /* TEMPLATE: schema */river_job
        WHERE river_job.queue = d.queue
            AND river_job.state = 'running'
    ) AS running_count
FROM /* TEMPLATE: schema */river_queue_drain d
WHERE d.queue = @queue::text
    AND d.key = @key::text;

-- name: QueueDrainComplete :execrows
UPDATE /* TEMPLATE: schema */river_queue_drain
SET
    state = 'drained',
    drained_at = coalesce(sqlc.narg('now')::timestamptz, now()),
    updated_at = coalesce(sqlc.narg('now')::timestamptz, now())
WHERE queue = @queue::text
    AND state = 'draining'
    AND NOT EXISTS (
        SELECT 1
        FROM /* TEMPLATE: schema */river_job
        WHERE river_job.queue = river_queue_drain.queue
            AND river_job.state = 'running'
    );

-- name: QueueDrainResume :execrows
UPDATE /* TEMPLATE: schema */river_queue_drain
SET
    state = 'resumed',
    resumed_at = coalesce(sqlc.narg('now')::timestamptz, now()),
    updated_at = coalesce(sqlc.narg('now')::timestamptz, now())
WHERE queue = @queue::text
    AND state = 'drained';

-- name: QueueDrainDeleteResumed :execrows
DELETE FROM /* TEMPLATE: schema */river_queue_drain
WHERE state = 'resumed'
    AND updated_at < @updated_at_horizon::timestamptz;
