from clickhouse import ClickHouse, RepoClickHouseClient
import argparse
from clickhub import load_config
from repo.pull_repo import pull_data, push_data, parse_data


parser = argparse.ArgumentParser(
    description="github importer",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)

parser.add_argument("-c", "--config", default="config.yml", help="config")
parser.add_argument("-d", "--debug", action="store_true", help="debug")

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
    #print(repos)

    for owner, repo in repos:
        row_data = pull_data(owner, repo)
        #print(row_data)
        data = parse_data(row_data)
        push_data(client, "event_type", data)