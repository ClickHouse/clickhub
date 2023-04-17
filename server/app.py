from flask import Flask, request, render_template
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

app = Flask(__name__)


@app.get("/add_new_repo")
def process():
    repo = request.args.get("repo")
    max_queue_size = 5

    queue_size = client.query_row("SELECT COUNT(repo_name) FROM work_queue")
    if queue_size == max_queue_size:
        return "QUEUE IS FULL", 403

    repos_in_db = client.query_row(
        f"SELECT COUNT(repo_name) FROM commits WHERE repo_name = {repo}"
    )
    if repos_in_db >= 0:
        return "ALREADY_PROCESSED", 200

    repos_in_queue = client.query_row(
        f"SELECT COUNT(repo_name) FROM work_queue WHERE repo_name = {repo}"
    )
    if repos_in_queue >= 0:
        return "ALREADY_PROCESSING", 200

    return "OK", 201
