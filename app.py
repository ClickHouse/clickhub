from flask import Flask, request
import argparse
from clickhouse import ClickHouse, RepoClickHouseClient
from clickhub import load_config, load_types
from repo import importer, schedule


parser = argparse.ArgumentParser(
    description="github importer",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)

parser.add_argument("-c", "--config", default="config.yml", help="config")
parser.add_argument("-d", "--debug", action="store_true", help="debug")
parser.add_argument("-s", "--size", default=5, help="queue_size")
parser.add_argument("--queue_name", default="", help="queue_name")

args = parser.parse_args()

config = load_config(args.config)
types = load_types()

if args.queue_name == "":
    args.queue_name = config["task_table"]

clickhouse = ClickHouse(
    host=config["host"],
    port=config["port"],
    native_port=config["native_port"],
    username=config["username"],
    password=config["password"],
    secure=config["secure"],
)
client = RepoClickHouseClient(clickhouse)

app = Flask(__name__)


@app.get("/add_new_repo")
def process():
    repo = request.args.get("repo")

    if not importer.is_valid_repo(repo):
        return "BAD REQUEST", 400

    repos_in_db = client.query_row(
        f"SELECT COUNT(repo_name) FROM git.commits WHERE repo_name = '{repo}'"
    )
    if repos_in_db[0] >= 0:
        return "ALREADY_PROCESSED", 200

    try:
        schedule.schedule_repo_job(client, args.queue_name, repo, 1)
    except schedule.AlreadyScheduled:
        return "ALREADY_PROCESSING", 200
    except schedule.FullQueue:
        return "QUEUE IS FULL", 403

    return "OK", 201


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
