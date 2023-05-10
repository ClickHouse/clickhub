import unittest
import requests
from clickhub import load_config, load_types
from clickhouse import ClickHouse, RepoClickHouseClient

config = load_config("config.yml")
types = load_types()

clickhouse = ClickHouse(
    host=config["host"],
    port=config["port"],
    native_port=config["native_port"],
    username=config["username"],
    password=config["password"],
    secure=True,
)
client = RepoClickHouseClient(clickhouse)


# child class derived from unittest.TestCase
class TestApp(unittest.TestCase):
    def test_invalid_repo(self):
        responce = requests.get("http://localhost:5000/add_new_repo?repo=testing_repo")
        self.assertEqual(400, responce.status_code)

    def test_repos_in_db(self):
        # ClickHouse/opentelemetry-demo уже есть в БД
        responce = requests.get(
            "http://localhost:5000/add_new_repo?repo=ClickHouse/opentelemetry-demo"
        )
        self.assertEqual(200, responce.status_code)

    def test_repos_in_queue(self):
        res = client.query_row(
            "INSERT INTO git.new_queue (repo_name) VALUES ('ClickHouse/opentelemetry-demo')"
        )
        responce = requests.get(
            "http://localhost:5000/add_new_repo?repo=ClickHouse/opentelemetry-demo"
        )
        res = client.query_row(
            "DELETE FROM git.new_queue WHERE repo_name='ClickHouse/opentelemetry-demo'"
        )
        self.assertEqual(200, responce.status_code)


# Executing the tests in the above test case class
if __name__ == "__main__":
    unittest.main()
