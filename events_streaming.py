from clickhouse import ClickHouse, RepoClickHouseClient
import argparse
from clickhub import load_config
from repo.pull_repo import pull_data, push_data, parse_data, collect_data
import threading


parser = argparse.ArgumentParser(
    description="github importer",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)

parser.add_argument("-c", "--config", default="config.yml", help="config")
parser.add_argument("-d", "--debug", action="store_true", help="debug")
parser.add_argument("-s", "--size", default=100, help="batch_size")

args = parser.parse_args()

config = load_config(args.config)

clickhouse = ClickHouse(
    host=config["host"],
    port=config["port"],
    native_port=config["native_port"],
    username=config["username"],
    password=config["password"],
    secure=config["secure"],
)
client = RepoClickHouseClient(clickhouse)


if __name__ == "__main__":
    repo_names = client.query_rows("SELECT DISTINCT repo_name FROM git.commits")
    repos = [repo[0].split("/") for repo in repo_names]
    queue = []

    for owner, repo in repos:
        url = f"https://api.github.com/repos/{owner}/{repo}/events"
        row_data, next_link, f = pull_data(url)
        data = parse_data(row_data)

        lock = threading.Lock()

        t1 = threading.Thread(target=collect_data(lock, queue, data))
        t2 = threading.Thread(target=push_data(lock, client, queue, args.size))

        t1.start()
        t2.start()

        while f:
            row_data, next_link, f = pull_data(next_link)
            data = parse_data(row_data)
