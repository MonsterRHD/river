CREATE TABLE river_queue_drain (
    queue text NOT NULL,
    key text NOT NULL,
    state text NOT NULL,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    drained_at timestamp,
    resumed_at timestamp,
    updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (queue, key)
);

-- name: QueueDrainComplete :execrows
UPDATE /* TEMPLATE: schema */river_queue_drain
SET
    state = 'drained',
    drained_at = coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec')),
    updated_at = coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec'))
WHERE cast(queue AS text) = cast(@queue AS text)
    AND state = 'draining'
    AND NOT EXISTS (
        SELECT 1
        FROM /* TEMPLATE: schema */river_job
        WHERE river_job.queue = river_queue_drain.queue
            AND river_job.state = 'running'
    );

-- name: QueueDrainDeleteResumed :execrows
DELETE FROM /* TEMPLATE: schema */river_queue_drain
WHERE state = 'resumed'
    AND updated_at < cast(@updated_at_horizon AS text);

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
WHERE cast(d.queue AS text) = cast(@queue AS text)
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
WHERE cast(d.queue AS text) = cast(@queue AS text)
    AND cast(d.key AS text) = cast(@key AS text);

-- name: QueueDrainInsert :one
INSERT INTO /* TEMPLATE: schema */river_queue_drain (
    queue,
    key,
    state,
    created_at,
    updated_at
) VALUES (
    cast(@queue AS text),
    cast(@key AS text),
    'draining',
    coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec')),
    coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec'))
) ON CONFLICT DO NOTHING
RETURNING *;

-- name: QueueDrainResume :execrows
UPDATE /* TEMPLATE: schema */river_queue_drain
SET
    state = 'resumed',
    resumed_at = coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec')),
    updated_at = coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec'))
WHERE cast(queue AS text) = cast(@queue AS text)
    AND state = 'drained';
