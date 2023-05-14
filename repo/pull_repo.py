import requests
import json


def parse_link(string):
    data = string.split()
    next_link = data[0][1:-2]
    
    if "last" in string:
        return next_link, True
    else:
        return "", False

def pull_data(url):
    data = requests.get(url)

    next_link = ""
    f = False

    if "Link" in data.headers.keys():
        next_link, f = parse_link(data.headers["Link"])
    
    return data.text, next_link, f

def get_value(data, path):
    for p in path:
        data = data[p]
    return data

def parse_data(data):
    data_list = json.loads(data)

    structure = {"event_type":["type"],
                 "actor_login":["actor", "login"]}
    res_data = {}

    for column in structure.keys():
        res_list = []
        for element in data_list:
            path = structure[column]
            res = get_value(element, path)
            res_list.append(res) 

        res_data[column] = res_list

    return res_data

def collect_data():
    pass


def push_data(client, column, data):
    client.insert_row("git.github_events", column, data)


# result = pull_data("0Kee-Team", "WatchAD")
# data = parse_data(result)

# push_data("event_type", data)
