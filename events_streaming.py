from clickhouse import ClickHouse, RepoClickHouseClient
import argparse
from clickhub import load_config
from repo.pull_repo import pull_data, push_data, parse_data, collect_data
import threading
from tests import timing


parser = argparse.ArgumentParser(
    description="github importer",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)

parser.add_argument("-c", "--config", default="config.yml", help="config")
parser.add_argument("-d", "--debug", action="store_true", help="debug")
parser.add_argument("-s", "--size", default=10, help="batch_size")

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


def events_streaming(repos, lock, queue):
    for owner, repo in repos:
        url = f"https://api.github.com/repos/{owner}/{repo}/events"
        row_data, next_link, f = pull_data(url)
        data = parse_data(row_data)
        collect_data(lock, queue, data)

        while f:
            row_data, next_link, f = pull_data(next_link)
            data = parse_data(row_data)
            collect_data(lock, queue, data)


@timing.timeit
def main():
    repo_names = client.query_rows("SELECT DISTINCT repo_name FROM git.commits")
    repos = [repo[0].split("/") for repo in repo_names]

    queue = []
    lock = threading.Lock()

    t1 = threading.Thread(target=events_streaming, args=(repos, lock, queue))
    t2 = threading.Thread(target=push_data, args=(lock, client, queue, args.size))

    t1.start()
    t2.start()

    t1.join()
    t2.join()


if __name__ == "__main__":
    main()
