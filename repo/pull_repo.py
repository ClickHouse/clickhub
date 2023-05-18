import requests
import json
import time
from datetime import datetime


def parse_link(string):
    data = string.split()
    next_link = data[0][1:-2]

    if "last" in string:
        return next_link, True
    else:
        return "", False


def pull_data(url):
    # User access token requests are subject to a higher limit of 15,000 requests per hour
    data = requests.get(
        url, params={"per_page": 100}, headers={"Authorization": "Bearer <Your token>"}
    )
    next_link = ""
    f = False

    if "Link" in data.headers.keys():
        next_link, f = parse_link(data.headers["Link"])

    return data.text, next_link, f


def get_value(data, path, default_value):
    for p in path:
        # not all responces have all fields
        try:
            data = data[p]
        except:
            data = default_value
    return data


def parse_data(data):
    data_list = json.loads(data)

    structure = {
        "event_type": (["type"], 0),
        "actor_login": (["actor", "login"], ""),
        "repo_name": (["repo", "name"], ""),
        "created_at": (["created_at"], ""),
        "action": (["payload", "action"], ""),
    }
    res_data = {}

    for column in structure.keys():
        res_list = []
        for element in data_list:
            path = structure[column][0]
            res = get_value(element, path, structure[column][1])
            if column == "created_at":
                res = datetime.strptime(res, "%Y-%m-%dT%H:%M:%SZ")
            res_list.append(res)

        res_data[column] = res_list

    return res_data


def collect_data(lock, queue, data):
    lock.acquire()

    try:
        for field in data.keys():
            try:
                queue[field] += data[field]
            except:
                queue[field] = data[field]
    finally:
        lock.release()


def push_data(lock, client, queue, max_size):
    while True:
        time.sleep(10)

        lock.acquire()

        try:
            # print(len(queue[list(queue.keys())[0]]))
            if len(queue[list(queue.keys())[0]]) >= max_size:
                columns = list(queue.keys())
                rows = [z for z in zip(*queue.values())]
                client.insert_rows("git.github_events", columns, rows)
                queue.clear()
        finally:
            lock.release()
