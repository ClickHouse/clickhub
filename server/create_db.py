from clickhouse_driver import Client


client = Client()

client.execute('CREATE DATABASE IF NOT EXISTS git')

req = """
    CREATE TABLE IF NOT EXISTS git.commits
(
    `hash`           String,
    `author`         LowCardinality(String),
    `time`           DateTime,
    `message`        String,
    `files_added`    UInt32,
    `files_deleted`  UInt32,
    `files_renamed`  UInt32,
    `files_modified` UInt32,
    `lines_added`    UInt32,
    `lines_deleted`  UInt32,
    `hunks_added`    UInt32,
    `hunks_removed`  UInt32,
    `hunks_changed`  UInt32,
    `repo_name`      LowCardinality(String),
    `updated_at`     DateTime MATERIALIZED now()
) ENGINE = ReplacingMergeTree
PARTITION BY repo_name
ORDER BY (repo_name, time, hash) """

client.execute(req)

req_1 = """
CREATE TABLE IF NOT EXISTS git.file_changes
(
    `change_type`           Enum8('Add' = 1, 'Delete' = 2, 'Modify' = 3, 'Rename' = 4, 'Copy' = 5, 'Type' = 6),
    `path`                  LowCardinality(String),
    `old_path`              LowCardinality(String),
    `file_extension`        LowCardinality(String),
    `lines_added`           UInt32,
    `lines_deleted`         UInt32,
    `hunks_added`           UInt32,
    `hunks_removed`         UInt32,
    `hunks_changed`         UInt32,
    `commit_hash`           String,
    `author`                LowCardinality(String),
    `time`                  DateTime,
    `commit_message`        String,
    `commit_files_added`    UInt32,
    `commit_files_deleted`  UInt32,
    `commit_files_renamed`  UInt32,
    `commit_files_modified` UInt32,
    `commit_lines_added`    UInt32,
    `commit_lines_deleted`  UInt32,
    `commit_hunks_added`    UInt32,
    `commit_hunks_removed`  UInt32,
    `commit_hunks_changed`  UInt32,
    `repo_name`             LowCardinality(String),
    `updated_at`            DateTime MATERIALIZED now()
) ENGINE = ReplacingMergeTree
PARTITION BY repo_name
ORDER BY (repo_name, time, commit_hash, path)
SETTINGS index_granularity = 8192 """

client.execute(req_1)

req_2 = """
CREATE TABLE IF NOT EXISTS git.line_changes
(
    `sign`                       Int8,
    `line_number_old`            UInt32,
    `line_number_new`            UInt32,
    `hunk_num`                   UInt32,
    `hunk_start_line_number_old` UInt32,
    `hunk_start_line_number_new` UInt32,
    `hunk_lines_added`           UInt32,
    `hunk_lines_deleted`         UInt32,
    `hunk_context`               LowCardinality(String),
    `line`                       LowCardinality(String),
    `indent`                     UInt8,
    `line_type`                  Enum8('Empty' = 0, 'Comment' = 1, 'Punct' = 2, 'Code' = 3),
    `prev_commit_hash`           String,
    `prev_author`                LowCardinality(String),
    `prev_time`                  DateTime,
    `file_change_type`           Enum8('Add' = 1, 'Delete' = 2, 'Modify' = 3, 'Rename' = 4, 'Copy' = 5, 'Type' = 6),
    `path`                       LowCardinality(String),
    `old_path`                   LowCardinality(String),
    `file_extension`             LowCardinality(String),
    `file_lines_added`           UInt32,
    `file_lines_deleted`         UInt32,
    `file_hunks_added`           UInt32,
    `file_hunks_removed`         UInt32,
    `file_hunks_changed`         UInt32,
    `commit_hash`                String,
    `author`                     LowCardinality(String),
    `time`                       DateTime,
    `commit_message`             String,
    `commit_files_added`         UInt32,
    `commit_files_deleted`       UInt32,
    `commit_files_renamed`       UInt32,
    `commit_files_modified`      UInt32,
    `commit_lines_added`         UInt32,
    `commit_lines_deleted`       UInt32,
    `commit_hunks_added`         UInt32,
    `commit_hunks_removed`       UInt32,
    `commit_hunks_changed`       UInt32,
    `repo_name`                  LowCardinality(String),
    `updated_at`                 DateTime MATERIALIZED now()
) ENGINE = ReplacingMergeTree
PARTITION BY repo_name
ORDER BY (repo_name, time, commit_hash, path, line_number_old, line_number_new) """

client.execute(req_2)

req_3 = """
CREATE TABLE IF NOT EXISTS git.work_queue
(
    `repo_name` String,
    `scheduled` DateTime,
    `priority` Int32
)
ENGINE = KeeperMap('git_queue')
PRIMARY KEY repo_name """

client.execute(req_3)

req_4 = """
CREATE TABLE IF NOT EXISTS default.github_stars
(
    `repo_name` LowCardinality(String),
    `stars`     UInt64
) ENGINE = SummingMergeTree
ORDER BY repo_name """

client.execute(req_4)

req_5 = """ CREATE
MATERIALIZED VIEW github_stars_mv TO github_stars AS
SELECT repo_name,
       count() AS stars
FROM github_events
WHERE event_type = 'WatchEvent'
GROUP BY repo_name
    INSERT
INTO github_stars
SELECT repo_name, countIf(event_type = 'WatchEvent', 0) AS stars
FROM github_events
GROUP BY repo_name """

client.execute(req_5)