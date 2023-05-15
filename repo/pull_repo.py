import requests
import json
import threading


def parse_link(string):
    data = string.split()
    next_link = data[0][1:-2]
    
    if "last" in string:
        return next_link, True
    else:
        return "", False

def pull_data(url):
    # User access token requests are subject to a higher limit of 15,000 requests per hour
    data = requests.get(url, params={"per_page":100}, headers={'Authorization':"Bearer <Your token>"})

    next_link = ""
    f = False

    if "Link" in data.headers.keys():
        next_link, f = parse_link(data.headers["Link"])
    
    return data.text, next_link, f

def get_value(data, path):
    for p in path:
        # not all responces have all fields
        try:
            data = data[p]
        except:
            data = None
    return data

def parse_data(data):
    data_list = json.loads(data)

    structure = {"event_type":["type"],
                 "actor_login":["actor", "login"],
                 "repo_name":["repo", "name"],
                 "created_at":["created_at"],
                 "action":["payload", "action"]}
    res_data = {}

    for column in structure.keys():
        res_list = []
        for element in data_list:
            path = structure[column]
            res = get_value(element, path)
            res_list.append(res) 

        res_data[column] = res_list

    return res_data

def collect_data(lock, queue, data):
    lock.acquire()

    try: 
        queue.append(data)
    finally:
        lock.release()

def push_data(lock, client, queue, max_size):
    lock.acquire()

    try:
        if len(queue) >= max_size:
            columns, values = queue[0].keys(), [x.values() for x in queue]
            client.insert("git.github_events", columns, values)
            queue
    finally:
        lock.release()
