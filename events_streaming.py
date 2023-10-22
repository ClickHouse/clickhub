from clickhouse import ClickHouse, RepoClickHouseClient
import argparse
from clickhub import load_config
from repo.pull_repo import pull_data, parse_data, collect_data
from repo.pull_repo import EmptyResponse, ForbiddenException, ServiceUnavailable
import threading

# from tests import timing
import time
from queueing import Queue


GITHUB_API_URL = "https://api.github.com/events"

parser = argparse.ArgumentParser(
    description="github importer",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)

parser.add_argument("-c", "--config", default="config.yml", help="config")
parser.add_argument("-d", "--debug", action="store_true", help="debug")
parser.add_argument("-s", "--size", default=50, help="batch_size")

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


def events_streaming(queue, max_size):
    f = True
    next_link = GITHUB_API_URL
    etag = None

    while f:
        try:
            row_data, next_link, f, etag = pull_data(next_link, etag)
        except EmptyResponse:
            # no changes in events
            time.sleep(60)
            continue
        except ForbiddenException:
            # mostly becasuse of rate limit
            time.sleep(60)
            continue
        except ServiceUnavailable:
            print("Service unavailible")
            continue
        except Exception as e:
            print(e)
            continue
        data = parse_data(row_data)
        collect_data(queue, data, max_size)


# @timing.timeit
def main():
    queue = Queue(client)

    t1 = threading.Thread(target=events_streaming, args=(queue, args.size))
    t1.start()

    t1.join()
    queue.stop()


if __name__ == "__main__":
    main()
