CREATE UNLOGGED TABLE river_producer (
    queue_name  text        NOT NULL,
    client_id   text        NOT NULL,
    producer_id bigint      NOT NULL,
    generation  bigint      NOT NULL DEFAULT 1,
    max_workers bigint      NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    expires_at  timestamptz NOT NULL,
    reaped_at   timestamptz,
    PRIMARY KEY (queue_name, client_id)
);

-- name: ProducerDeleteReaped :many
WITH reaped_producers AS (
    SELECT
        queue_name,
        client_id
    FROM /* TEMPLATE: schema */river_producer
    WHERE reaped_at < @reaped_at_horizon::timestamptz
    ORDER BY
        reaped_at ASC,
        queue_name ASC,
        client_id ASC
    LIMIT @max::bigint
    FOR UPDATE
    SKIP LOCKED
)
DELETE FROM /* TEMPLATE: schema */river_producer
USING reaped_producers
WHERE
    river_producer.queue_name = reaped_producers.queue_name
    AND river_producer.client_id = reaped_producers.client_id
    AND river_producer.reaped_at < @reaped_at_horizon::timestamptz
RETURNING river_producer.*;

-- name: ProducerFinish :one
UPDATE /* TEMPLATE: schema */river_producer
SET
    reaped_at = coalesce(sqlc.narg('now')::timestamptz, now())
WHERE
    queue_name = @queue_name::text
    AND client_id = @client_id::text
    AND generation = @generation::bigint
    AND reaped_at IS NULL
RETURNING *;

-- name: ProducerGet :one
SELECT *
FROM /* TEMPLATE: schema */river_producer
WHERE
    queue_name = @queue_name::text
    AND client_id = @client_id::text;

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
    @queue_name::text,
    @client_id::text,
    @producer_id::bigint,
    1,
    @max_workers::bigint,
    coalesce(sqlc.narg('now')::timestamptz, now()),
    coalesce(sqlc.narg('now')::timestamptz, now()),
    coalesce(sqlc.narg('now')::timestamptz, now()) + make_interval(secs => @ttl),
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
    updated_at = coalesce(sqlc.narg('now')::timestamptz, now()),
    expires_at = coalesce(sqlc.narg('now')::timestamptz, now()) + make_interval(secs => @ttl)
WHERE
    queue_name = @queue_name::text
    AND client_id = @client_id::text
    AND generation = @generation::bigint
    AND reaped_at IS NULL
RETURNING *;

-- name: ProducerReapExpired :many
WITH expired_producers AS (
    SELECT
        queue_name,
        client_id
    FROM /* TEMPLATE: schema */river_producer
    WHERE
        expires_at < coalesce(sqlc.narg('now')::timestamptz, now())
        AND reaped_at IS NULL
    ORDER BY
        expires_at ASC,
        queue_name ASC,
        client_id ASC
    LIMIT @max::bigint
    FOR UPDATE
    SKIP LOCKED
)
UPDATE /* TEMPLATE: schema */river_producer
SET
    reaped_at = coalesce(sqlc.narg('now')::timestamptz, now())
FROM expired_producers
WHERE
    river_producer.queue_name = expired_producers.queue_name
    AND river_producer.client_id = expired_producers.client_id
    AND river_producer.expires_at < coalesce(sqlc.narg('now')::timestamptz, now())
    AND river_producer.reaped_at IS NULL
RETURNING river_producer.*;
