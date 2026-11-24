-- +goose Up
CREATE TABLE images (
    id uuid PRIMARY KEY,
    original_name text NOT NULL,
    original_key text NOT NULL UNIQUE CHECK (original_key <> ''),
    content_type text NOT NULL CHECK (content_type IN ('image/jpeg', 'image/png')),
    size_bytes bigint NOT NULL CHECK (size_bytes > 0),
    width integer NOT NULL CHECK (width > 0),
    height integer NOT NULL CHECK (height > 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    deleted_at timestamptz,
    cleanup_after timestamptz,
    cleanup_lease_until timestamptz,
    cleanup_token uuid,
    last_cleanup_at timestamptz,
    CONSTRAINT images_cleanup_claim CHECK (
        (cleanup_lease_until IS NULL) = (cleanup_token IS NULL)
    ),
    CONSTRAINT images_cleanup_requires_tombstone CHECK (
        deleted_at IS NOT NULL OR (
            cleanup_after IS NULL AND cleanup_lease_until IS NULL
            AND cleanup_token IS NULL AND last_cleanup_at IS NULL
        )
    )
);

CREATE INDEX images_active_created_idx ON images (created_at DESC, id DESC)
    WHERE deleted_at IS NULL;
CREATE INDEX images_cleanup_idx ON images (cleanup_after)
    WHERE deleted_at IS NOT NULL;

CREATE TABLE jobs (
    id uuid PRIMARY KEY,
    image_id uuid NOT NULL REFERENCES images (id) ON DELETE RESTRICT,
    operation text NOT NULL CHECK (operation IN ('resize', 'thumbnail', 'watermark')),
    params jsonb NOT NULL CHECK (jsonb_typeof(params) = 'object'),
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'processing', 'retrying', 'completed', 'failed', 'cancelled')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    attempt_token uuid,
    lease_until timestamptz,
    next_retry_at timestamptz,
    next_dispatch_at timestamptz,
    result_key text,
    result_width integer,
    result_height integer,
    result_size_bytes bigint,
    last_error_code text,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    CONSTRAINT jobs_image_operation_unique UNIQUE (image_id, operation),
    CONSTRAINT jobs_processing_claim CHECK (
        (status = 'processing' AND attempt_token IS NOT NULL AND lease_until IS NOT NULL AND attempts > 0)
        OR (status <> 'processing' AND attempt_token IS NULL AND lease_until IS NULL)
    ),
    CONSTRAINT jobs_retry_schedule CHECK (
        (status = 'retrying') = (next_retry_at IS NOT NULL)
    ),
    CONSTRAINT jobs_terminal_time CHECK (
        (status IN ('completed', 'failed', 'cancelled')) = (completed_at IS NOT NULL)
    ),
    CONSTRAINT jobs_result CHECK (
        (status = 'completed' AND result_key IS NOT NULL AND result_key <> ''
            AND result_width IS NOT NULL AND result_width > 0
            AND result_height IS NOT NULL AND result_height > 0
            AND result_size_bytes IS NOT NULL AND result_size_bytes > 0)
        OR (status <> 'completed' AND result_key IS NULL AND result_width IS NULL
            AND result_height IS NULL AND result_size_bytes IS NULL)
    )
);

CREATE INDEX jobs_expired_lease_idx ON jobs (lease_until) WHERE status = 'processing';
CREATE INDEX jobs_dispatch_idx ON jobs (next_dispatch_at) WHERE status IN ('pending', 'retrying');

CREATE TABLE outbox (
    id uuid PRIMARY KEY,
    job_id uuid REFERENCES jobs (id) ON DELETE RESTRICT,
    event_type text NOT NULL CHECK (event_type <> ''),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    topic text NOT NULL CHECK (topic <> ''),
    message_key text NOT NULL CHECK (message_key <> ''),
    dedupe_key text UNIQUE,
    available_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    lease_until timestamptz,
    claim_token uuid,
    publish_attempts integer NOT NULL DEFAULT 0 CHECK (publish_attempts >= 0),
    last_error_code text,
    CONSTRAINT outbox_requested_job CHECK (
        event_type <> 'image.processing.requested' OR job_id IS NOT NULL
    ),
    CONSTRAINT outbox_claim CHECK ((lease_until IS NULL) = (claim_token IS NULL)),
    CONSTRAINT outbox_published_claim CHECK (
        published_at IS NULL OR (lease_until IS NULL AND claim_token IS NULL)
    )
);

CREATE INDEX outbox_available_idx ON outbox (available_at, id) WHERE published_at IS NULL;
CREATE INDEX outbox_unpublished_job_idx ON outbox (job_id) WHERE published_at IS NULL;
CREATE INDEX outbox_published_idx ON outbox (published_at) WHERE published_at IS NOT NULL;
CREATE UNIQUE INDEX outbox_pending_request_unique ON outbox (job_id)
    WHERE published_at IS NULL AND event_type = 'image.processing.requested';

CREATE TABLE storage_objects (
    object_key text PRIMARY KEY CHECK (object_key <> ''),
    -- An original write intent precedes the images row: intentionally no image FK.
    image_id uuid NOT NULL,
    job_id uuid REFERENCES jobs (id) ON DELETE RESTRICT,
    attempt_token uuid,
    state text NOT NULL DEFAULT 'writing' CHECK (state IN ('writing', 'retained', 'discarded')),
    write_until timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT storage_objects_attempt CHECK ((job_id IS NULL) = (attempt_token IS NULL))
);

CREATE INDEX storage_objects_state_write_idx ON storage_objects (state, write_until);
CREATE INDEX storage_objects_image_idx ON storage_objects (image_id);
CREATE INDEX storage_objects_job_idx ON storage_objects (job_id) WHERE job_id IS NOT NULL;

-- +goose Down
DROP TABLE storage_objects;
DROP TABLE outbox;
DROP TABLE jobs;
DROP TABLE images;
