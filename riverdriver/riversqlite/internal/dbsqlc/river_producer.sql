CREATE TABLE river_producer (
    queue_name text NOT NULL,
    client_id text NOT NULL,
    producer_id integer NOT NULL,
    generation integer NOT NULL DEFAULT 1,
    max_workers integer NOT NULL,
    created_at timestamp NOT NULL DEFAULT (datetime('now', 'subsec')),
    updated_at timestamp NOT NULL DEFAULT (datetime('now', 'subsec')),
    expires_at timestamp NOT NULL,
    reaped_at timestamp,
    PRIMARY KEY (queue_name, client_id)
);

-- name: ProducerDeleteReaped :many
DELETE FROM /* TEMPLATE: schema */river_producer
WHERE (queue_name, client_id) IN (
    SELECT
        queue_name,
        client_id
    FROM /* TEMPLATE: schema */river_producer
    WHERE reaped_at < cast(@reaped_at_horizon AS text)
    ORDER BY
        reaped_at ASC,
        queue_name ASC,
        client_id ASC
    LIMIT @max
)
    AND reaped_at < cast(@reaped_at_horizon AS text)
RETURNING *;

-- name: ProducerFinish :one
UPDATE /* TEMPLATE: schema */river_producer
SET
    reaped_at = coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec'))
WHERE
    queue_name = @queue_name
    AND client_id = @client_id
    AND generation = @generation
    AND reaped_at IS NULL
RETURNING *;

-- name: ProducerGet :one
SELECT *
FROM /* TEMPLATE: schema */river_producer
WHERE
    queue_name = @queue_name
    AND client_id = @client_id;

-- name: ProducerInsert :one
INSERT INTO /* TEMPLATE: schema */river_producer (
    queue_name,
    client_id,
    producer_id,
    generation,
    max_workers,
    created_at,
    updated_at,
    expires_at,
    reaped_at
) VALUES (
    @queue_name,
    @client_id,
    @producer_id,
    1,
    @max_workers,
    coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec')),
    coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec')),
    datetime(coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec')), 'subsec', cast(@ttl AS text)),
    NULL
)
ON CONFLICT (queue_name, client_id) DO UPDATE
SET
    producer_id = EXCLUDED.producer_id,
    generation = river_producer.generation + 1,
    max_workers = EXCLUDED.max_workers,
    updated_at = EXCLUDED.updated_at,
    expires_at = EXCLUDED.expires_at,
    reaped_at = NULL
RETURNING *;

-- name: ProducerKeepAlive :one
UPDATE /* TEMPLATE: schema */river_producer
SET
    updated_at = coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec')),
    expires_at = datetime(coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec')), 'subsec', cast(@ttl AS text))
WHERE
    queue_name = @queue_name
    AND client_id = @client_id
    AND generation = @generation
    AND reaped_at IS NULL
RETURNING *;

-- name: ProducerReapExpired :many
UPDATE /* TEMPLATE: schema */river_producer
SET
    reaped_at = coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec'))
WHERE (queue_name, client_id) IN (
    SELECT
        queue_name,
        client_id
    FROM /* TEMPLATE: schema */river_producer
    WHERE
        expires_at < coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec'))
        AND reaped_at IS NULL
    ORDER BY
        expires_at ASC,
        queue_name ASC,
        client_id ASC
    LIMIT @max
)
    AND expires_at < coalesce(cast(sqlc.narg('now') AS text), datetime('now', 'subsec'))
    AND reaped_at IS NULL
RETURNING *;
