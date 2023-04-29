from clickhouse import ClickHouse, RepoClickHouseClient
import argparse
from clickhub import load_config, load_types
import types


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

client.query_row(types[0][3])
client.query_row(types[1][3])
client.query_row(types[2][3])

client.query_row(
    f"""
CREATE TABLE {config["task_table"]}
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
    f"""
CREATE TABLE {config["clone_table"]}
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
