from collections import defaultdict
from dataclasses import dataclass, field
import clickhouse_connect


@dataclass
class DataType:
    name: str
    table: str
    statement: str


@dataclass
class ClickHouse:
    host: str = "localhost"
    port: int = 8123
    native_port: int = 9000
    username: str = "default"
    password: str = ""
    secure: bool = True
    connect_timeout: int = 10
    settings: defaultdict[dict] = field(default_factory=lambda: defaultdict(dict))


class RepoClickHouseClient:
    def __init__(self, clickhouse: ClickHouse):
        self.config = clickhouse
        self._client = clickhouse_connect.get_client(
            host=clickhouse.host,
            username=clickhouse.username,
            password=clickhouse.password,
            port=clickhouse.port,
            secure=clickhouse.secure,
            connect_timeout=clickhouse.connect_timeout,
        )
        if not self._client.ping():
            raise Exception(f"unable to connect to cluster at {clickhouse.host}")

    def query_row(self, statement):
        for result in self._client.query(statement).result_set:
            return result
        return None

    def insert_row(self, table, columns, data):
        self._client.insert(table, [data], columns)

    def query_rows(self, statement):
        return self._client.query(statement).result_set

    def insert_rows(self, table, columns, data, **kwargs):
        self._client.insert(table, data, columns, **kwargs)

    def close(self):
        self._client.close()

