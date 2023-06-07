import requests
import json
from datetime import datetime
import os


class EmptyResponse(Exception):
    pass


class ServiceUnavailable(Exception):
    pass


class ForbiddenException(Exception):
    pass


batch = {}


def parse_link(string):
    data = string.split()
    next_link = data[0][1:-2]

    if "last" in string:
        return next_link, True
    else:
        return "", False


def get_token():
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        raise Exception("No token in env")

    return token


def pull_data(url, etag=None):
    # User access token requests are subject to a higher limit of 15,000 requests per hour
    headers = {"Authorization": f"Bearer {get_token()}"}

    if etag:
        headers["ETag"] = etag

    data = requests.get(
        url,
        params={"per_page": 100},
        headers=headers,
    )
    next_link = ""
    f = False

    if data.status_code == 200:
        etag = data.headers["ETag"]

        if "Link" in data.headers.keys():
            next_link, f = parse_link(data.headers["Link"])

        return data.text, next_link, f, etag

    if data.status_code == 304:
        raise EmptyResponse
    elif data.status_code == 403:
        raise ForbiddenException
    elif data.status_code == 503:
        raise ServiceUnavailable

    raise Exception(f"Unknown error code {data.status_code}")


def get_value(data, path, default_value):
    for p in path:
        # not all responces have all fields
        try:
            data = data[p]
        except:
            return default_value

    if data is None:
        return default_value

    return data


def parse_data(data):
    data_list = json.loads(data)

    structure = {
        "event_type": (["type"], 0),
        "actor_login": (["actor", "login"], ""),
        "repo_name": (["repo", "name"], ""),
        "created_at": (["created_at"], "1970-01-01T00:00:00Z"),
        # "updated_at": (["payload", "pull_request", "updated_at"], "1970-01-01T00:00:00Z"),
        # "merged_at": (["payload", "pull_request", "merged_at"], "1970-01-01T00:00:00Z"),
        "action": (["payload", "action"], ""),
        # "number": (["payload", "issue", "number"], 0),
    }
    res_data = {}

    for column in structure.keys():
        res_list = []
        for element in data_list:
            path = structure[column][0]
            res = get_value(element, path, structure[column][1])
            if column == "created_at" or column == "merged_at":
                res = datetime.strptime(res, "%Y-%m-%dT%H:%M:%SZ")
            res_list.append(res)

        res_data[column] = res_list

    return res_data


def collect_data(queue, data, max_size):
    global batch

    for column in data.keys():
        a = column
        if column in batch.keys():
            batch[column].append(data[column])
        else:
            batch[column] = data[column]

    if len(batch[a]) >= max_size:
        queue.add(batch)
        batch = {}
