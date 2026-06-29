ATTACH TABLE _ UUID '2db768c2-0fee-4410-8b60-f497309b1491'
(
    `time_unix_nano` UInt64,
    `scope_name` String,
    `severity_text` String,
    `body_str` String
)
ENGINE = MergeTree
ORDER BY time_unix_nano
SETTINGS index_granularity = 8192
