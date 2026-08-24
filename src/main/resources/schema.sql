-- All event_time_text values are stored as UTC ISO-8601 strings.
-- All *_epoch_ms values are the authoritative values for comparison and ordering.
-- The application must write both representations from the same Instant.


CREATE TABLE IF NOT EXISTS attributed_page_view (
                                                    page_view_id TEXT PRIMARY KEY NOT NULL,

                                                    user_id TEXT NOT NULL,

                                                    event_time TEXT NOT NULL,
                                                    event_time_epoch_ms INTEGER NOT NULL,

                                                    url TEXT NOT NULL,

                                                    attributed_campaign_id TEXT,
                                                    attributed_click_id TEXT,

                                                    created_at TEXT NOT NULL
                                                    DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    created_at_epoch_ms INTEGER NOT NULL
    DEFAULT (CAST(strftime('%s', 'now') AS INTEGER) * 1000)
    );

CREATE INDEX IF NOT EXISTS idx_attributed_page_view_event_time
    ON attributed_page_view(event_time_epoch_ms, page_view_id);


CREATE TABLE IF NOT EXISTS processed_input (
                                               topic TEXT NOT NULL,
                                               partition_no INTEGER NOT NULL,
                                               offset_no INTEGER NOT NULL,

                                               event_type TEXT NOT NULL
                                                CHECK (event_type IN ('page_views', 'ad_clicks')),

    -- PAGE_VIEWS uses event_id; AD_CLICKS uses click_id.
    event_key TEXT,

    event_time TEXT,
    event_time_epoch_ms INTEGER,

    payload_hash TEXT,

    processing_status TEXT NOT NULL
    CHECK (processing_status IN (
           'PROCESSED',
           'DROPPED_LATE',
           'DEAD_LETTER'
                                )),

    attempt_count INTEGER NOT NULL DEFAULT 1,

    received_at TEXT NOT NULL
    DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    received_at_epoch_ms INTEGER NOT NULL
    DEFAULT (CAST(strftime('%s', 'now') AS INTEGER) * 1000),

    processed_at TEXT,
    processed_at_epoch_ms INTEGER,

    PRIMARY KEY (topic, partition_no, offset_no)
    -- NULL event_key is allowed for malformed messages.
    );

CREATE INDEX IF NOT EXISTS idx_processed_input_status
    ON processed_input(processing_status);

CREATE INDEX IF NOT EXISTS idx_processed_input_event_time
    ON processed_input(event_time_epoch_ms);

CREATE INDEX IF NOT EXISTS idx_processed_input_event_key
    ON processed_input(topic, event_key);


CREATE TABLE IF NOT EXISTS processed_input_history (
                                                       topic TEXT NOT NULL,
                                                       partition_no INTEGER NOT NULL,
                                                       offset_no INTEGER NOT NULL,

                                                       event_type TEXT NOT NULL
                                                       CHECK (event_type IN ('page_views', 'ad_clicks')),

    event_key TEXT,

    event_time TEXT,
    event_time_epoch_ms INTEGER,

    payload_hash TEXT,

    processing_status TEXT NOT NULL
    CHECK (processing_status IN (
           'PROCESSED',
           'DROPPED_LATE',
           'DEAD_LETTER'
                                )),

    attempt_count INTEGER NOT NULL DEFAULT 1,

    received_at TEXT NOT NULL
    DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    received_at_epoch_ms INTEGER NOT NULL
    DEFAULT (CAST(strftime('%s', 'now') AS INTEGER) * 1000),

    processed_at TEXT,
    processed_at_epoch_ms INTEGER,

    archived_at TEXT NOT NULL
    DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    archived_at_epoch_ms INTEGER NOT NULL
    DEFAULT (CAST(strftime('%s', 'now') AS INTEGER) * 1000),

    PRIMARY KEY (topic, partition_no, offset_no)
    );

CREATE INDEX IF NOT EXISTS idx_processed_input_history_status
    ON processed_input_history(processing_status);

CREATE INDEX IF NOT EXISTS idx_processed_input_history_event_time
    ON processed_input_history(event_time_epoch_ms);

CREATE INDEX IF NOT EXISTS idx_processed_input_history_archived_at
    ON processed_input_history(archived_at_epoch_ms);

CREATE INDEX IF NOT EXISTS idx_processed_input_history_event_key
    ON processed_input_history(topic, event_key);

CREATE TABLE IF NOT EXISTS click_state (
                                           click_id TEXT PRIMARY KEY NOT NULL,

                                           user_id TEXT NOT NULL,

                                           event_time TEXT NOT NULL,
                                           event_time_epoch_ms INTEGER NOT NULL,

                                           campaign_id TEXT NOT NULL,

                                           source_topic TEXT NOT NULL,
                                           partition_no INTEGER NOT NULL,
                                           offset_no INTEGER NOT NULL,

    -- CONFLICT is not stored here; conflicting clicks go to dead_letter_event.
                                           state_status TEXT NOT NULL DEFAULT 'ACTIVE'
                                           CHECK (state_status IN ('ACTIVE', 'EVICTED')),

    created_at TEXT NOT NULL
    DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    created_at_epoch_ms INTEGER NOT NULL
    DEFAULT (CAST(strftime('%s', 'now') AS INTEGER) * 1000),

    updated_at TEXT NOT NULL
    DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at_epoch_ms INTEGER NOT NULL
    DEFAULT (CAST(strftime('%s', 'now') AS INTEGER) * 1000)
    );

CREATE INDEX IF NOT EXISTS idx_click_state_user_time
    ON click_state(user_id, event_time_epoch_ms DESC, click_id);

CREATE INDEX IF NOT EXISTS idx_click_state_status_time
    ON click_state(state_status, event_time_epoch_ms);


CREATE TABLE IF NOT EXISTS click_history (
                                             click_id TEXT PRIMARY KEY NOT NULL,

                                             user_id TEXT NOT NULL,

                                             event_time TEXT NOT NULL,
                                             event_time_epoch_ms INTEGER NOT NULL,

                                             campaign_id TEXT NOT NULL,

                                             source_topic TEXT NOT NULL,
                                             partition_no INTEGER NOT NULL,
                                             offset_no INTEGER NOT NULL,

                                             created_at TEXT NOT NULL
                                             DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                                             created_at_epoch_ms INTEGER NOT NULL
                                             DEFAULT (CAST(strftime('%s', 'now') AS INTEGER) * 1000),

                                             updated_at TEXT NOT NULL
                                             DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                                             updated_at_epoch_ms INTEGER NOT NULL
                                             DEFAULT (CAST(strftime('%s', 'now') AS INTEGER) * 1000),

                                             archived_at TEXT NOT NULL,
                                             archived_at_epoch_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_click_history_user_time
    ON click_history(user_id, event_time_epoch_ms DESC, click_id);

CREATE INDEX IF NOT EXISTS idx_click_history_event_time
    ON click_history(event_time_epoch_ms);

CREATE INDEX IF NOT EXISTS idx_click_history_archived_at
    ON click_history(archived_at_epoch_ms);


CREATE TABLE IF NOT EXISTS pending_page_view (
                                                 page_view_id TEXT PRIMARY KEY NOT NULL,

                                                 user_id TEXT NOT NULL,

                                                 event_time TEXT NOT NULL,
                                                 event_time_epoch_ms INTEGER NOT NULL,

                                                 url TEXT NOT NULL,

                                                 source_topic TEXT NOT NULL,
                                                 partition_no INTEGER NOT NULL,
                                                 offset_no INTEGER NOT NULL,

    -- The row exists while the page view is waiting for finalization.
                                                 pending_status TEXT NOT NULL DEFAULT 'PENDING'
                                                 CHECK (pending_status IN ('PENDING', 'EMITTED')),

    created_at TEXT NOT NULL
    DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    created_at_epoch_ms INTEGER NOT NULL
    DEFAULT (CAST(strftime('%s', 'now') AS INTEGER) * 1000),

    updated_at TEXT NOT NULL
    DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at_epoch_ms INTEGER NOT NULL
    DEFAULT (CAST(strftime('%s', 'now') AS INTEGER) * 1000)
    );

CREATE INDEX IF NOT EXISTS idx_pending_page_view_status_time
    ON pending_page_view(pending_status, event_time_epoch_ms);

CREATE INDEX IF NOT EXISTS idx_pending_page_view_user_time
    ON pending_page_view(user_id, event_time_epoch_ms);


CREATE TABLE IF NOT EXISTS watermark_state (
    -- The current design uses one shared watermark per numeric partition.
                                               partition_no INTEGER PRIMARY KEY NOT NULL,

                                               max_event_time TEXT NOT NULL,
                                               max_event_time_epoch_ms INTEGER NOT NULL,

                                               watermark_status TEXT NOT NULL DEFAULT 'OBSERVED'
                                               CHECK (watermark_status IN (
                                               'OBSERVED',
                                               'IDLE',
                                               'RECOVERING'
)),

    last_seen_at TEXT NOT NULL
    DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    last_seen_at_epoch_ms INTEGER NOT NULL
    DEFAULT (CAST(strftime('%s', 'now') AS INTEGER) * 1000),

    updated_at TEXT NOT NULL
    DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at_epoch_ms INTEGER NOT NULL
    DEFAULT (CAST(strftime('%s', 'now') AS INTEGER) * 1000)
    );


CREATE TABLE IF NOT EXISTS dead_letter_event (
                                                 topic TEXT NOT NULL,
                                                 partition_no INTEGER NOT NULL,
                                                 offset_no INTEGER NOT NULL,

                                                 event_type TEXT,
                                                 event_key TEXT,

                                                 event_time TEXT,
                                                 event_time_epoch_ms INTEGER,

                                                 payload TEXT,

                                                 error_type TEXT NOT NULL,
                                                 error_message TEXT,

                                                 attempt_count INTEGER NOT NULL DEFAULT 1,

                                                 created_at TEXT NOT NULL
                                                 DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    created_at_epoch_ms INTEGER NOT NULL
    DEFAULT (CAST(strftime('%s', 'now') AS INTEGER) * 1000),

    PRIMARY KEY (topic, partition_no, offset_no)
    );

CREATE INDEX IF NOT EXISTS idx_dead_letter_event_created_at
    ON dead_letter_event(created_at_epoch_ms);

CREATE INDEX IF NOT EXISTS idx_dead_letter_event_error_type
    ON dead_letter_event(error_type);
