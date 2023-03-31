## Table Schemas

```sql
CREATE TABLE git.commits
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
ORDER BY (repo_name, time, hash)
```

```sql
CREATE TABLE git.file_changes
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
SETTINGS index_granularity = 8192
```

```sql
CREATE TABLE git.line_changes
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
ORDER BY (repo_name, time, commit_hash, path, line_number_old, line_number_new)
```

```sql
CREATE TABLE work_queue
(
    `repo_name` String,
    `scheduled` DateTime,
    `priority` Int32
)
ENGINE = KeeperMap('git_queue')
PRIMARY KEY repo_name


```

```sql
clickhouse
-local --query "SELECT c1::String as hash, c2::String as author, c3::DateTime('utc') as time, c4::String as message, c5::UInt32 as files_added, c6::UInt32 as files_deleted, c7::UInt32 as files_renamed, c8::UInt32 as files_modified, c9::UInt32 as lines_added, c10::UInt32 as lines_deleted, c11::UInt32 as hunks_added, c12::UInt32 as hunks_removed, c13::UInt32 as hunks_changed, 'ClickHouse/ClickHouse'::String as repo_name FROM file('commits.tsv') FORMAT Native" |  clickhouse-client --query "INSERT INTO git.commits FORMAT Native"
```

```sql
clickhouse
-local --query "SELECT c1::Enum8('Add' = 1, 'Delete' = 2, 'Modify' = 3, 'Rename' = 4, 'Copy' = 5, 'Type' = 6) as change_type, c2::String as path, c3::String as old_path, c4::String as file_extension, c5::UInt32 as lines_added, c6::UInt32 as lines_deleted, c7::UInt32 as hunks_added, c8::UInt32 as hunks_removed, c9::UInt32 as hunks_changed, c10::String as commit_hash, c11::String as author, c12::DateTime as time, c13::String as commit_message, c14::UInt32 as commit_files_added, c15::UInt32 as commit_files_deleted, c16::UInt32 as commit_files_renamed, c17::UInt32 as commit_files_modified, c18::UInt32 as commit_lines_added, c19::UInt32 as commit_lines_deleted, c20::UInt32 as commit_hunks_added, c21::UInt32 as commit_hunks_removed, c22::UInt32 as commit_hunks_changed, 'ClickHouse/ClickHouse'::String as repo_name FROM file('file_changes.tsv') FORMAT Native" |  clickhouse-client --query "INSERT INTO git.file_changes FORMAT Native"

```

```sql
clickhouse
-local --query "SELECT c1::Int8 as sign, c2::UInt32 as line_number_old, c3::UInt32 as line_number_new, c4::UInt32 as hunk_num, c5::UInt32 as hunk_start_line_number_old, c6::UInt32 as hunk_start_line_number_new, c7::UInt32 as hunk_lines_added, c8::UInt32 as hunk_lines_deleted,  c9::String as hunk_context, c10::String as line, c11::UInt8 as indent, c12::Enum8('Empty' = 0, 'Comment' = 1, 'Punct' = 2, 'Code' = 3) as line_type, c13::String as prev_commit_hash, c14::String as prev_author, c15::DateTime as prev_time, c16::Enum8('Add' = 1, 'Delete' = 2, 'Modify' = 3, 'Rename' = 4, 'Copy' = 5, 'Type' = 6) as file_change_type, c17::String as path, c18::String as old_path, c19::String as file_extension, c20::UInt32 as file_lines_added, c21::UInt32 as file_lines_deleted, c22::UInt32 as file_hunks_added, c23::UInt32 as file_hunks_removed, c24::UInt32 as file_hunks_changed, c25::String as commit_hash, c26::String as author, c27::DateTime as time, c28::String as commit_message, c29::UInt32 as commit_files_added, c30::UInt32 as commit_files_deleted, c31::UInt32 as commit_files_renamed, c32::UInt32 as commit_files_modified, c33::UInt32 as commit_lines_added, c34::UInt32 as commit_lines_deleted, c35::UInt32 as commit_hunks_added, c36::UInt32 as commit_hunks_removed, c37::UInt32 as commit_hunks_changed, 'ClickHouse/ClickHouse'::String as repo_name FROM file('line_changes.tsv') FORMAT Native"  | clickhouse-client --query "INSERT INTO git.line_changes FORMAT Native"
```

```sql
CREATE TABLE default.github_stars
(
    `repo_name` LowCardinality(String),
    `stars`     UInt64
) ENGINE = SummingMergeTree
ORDER BY repo_name



CREATE
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
GROUP BY repo_name

```


