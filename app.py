from flask import Flask, request
import argparse
import datetime
from clickhouse import ClickHouse, RepoClickHouseClient
from clickhub import load_config, load_types
from repo import importer


parser = argparse.ArgumentParser(
    description="github importer",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)

parser.add_argument("-c", "--config", default="config.yml", help="config")
parser.add_argument("-d", "--debug", action="store_true", help="debug")
parser.add_argument("-s", "--size", default=5, help="queue_size")

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

    if not importer.is_valid_repo(repo):
        return "BAD REQUEST", 400
    
    repos_in_db = client.query_row(f"SELECT COUNT(repo_name) FROM commits WHERE repo_name = {repo}")
    if repos_in_db >= 0:
        return "ALREADY_PROCESSED", 200
    
    repos_in_queue = client.query_row(f"SELECT COUNT(repo_name) FROM new_queue WHERE repo_name = {repo}")
    if repos_in_queue >= 0:
        return "ALREADY_PROCESSING", 200
    
    max_queue_size = args.size

    queue_size = client.query_row("SELECT COUNT(repo_name) FROM new_queue")
    if queue_size == max_queue_size:
        return "QUEUE IS FULL", 403

    client.insert_row("git.new_queue", "repo_name", repo)

    return "OK", 201

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)