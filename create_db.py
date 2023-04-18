from clickhouse import ClickHouse, RepoClickHouseClient
import argparse
from clickhub import load_config, load_types


parser = argparse.ArgumentParser(
    description="github importer",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)

parser.add_argument("-c", "--config", default="config.yml", help="config")
parser.add_argument("-d", "--debug", action="store_true", help="debug")

args = parser.parse_args()

config = load_config(args.config)
types = load_types()

clickhouse = ClickHouse(
    host=config["host"],
    port=config["port"],
    native_port=config["native_port"],
    username=config["username"],
    password=config["password"],
    secure=config["secure"],
)
client = RepoClickHouseClient(clickhouse)

client.query_row("CREATE DATABASE IF NOT EXISTS git")

client.query_row(
    """
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
ORDER BY (repo_name, time, hash)"""
)

client.query_row(
    """
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
ORDER BY (repo_name, time, commit_hash, path)
SETTINGS index_granularity = 8192"""
)

client.query_row(
    """
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
ORDER BY (repo_name, time, commit_hash, path, line_number_old, line_number_new)"""
)

client.query_row(
    """
CREATE TABLE git.work_queue
(
    `repo_name` String,
    `scheduled` DateTime,
    `priority` Int32,
    `worker_id` String,
    `started_time` DateTime,
)
ENGINE = KeeperMap('git_queue')
PRIMARY KEY repo_name

"""
)

client.query_row(
    """
CREATE TABLE git.new_queue
(
    `repo_name` String,
    `scheduled` DateTime,
    `priority` Int32,
    `worker_id` String,
    `started_time` DateTime,
)
ENGINE = KeeperMap('git_queue')
PRIMARY KEY repo_name

"""
)
