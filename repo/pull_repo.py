import requests
import json


def pull_data(owner, repo):
    url = f"https://api.github.com/repos/{owner}/{repo}/events"
    data = requests.get(url, params="--i")
    if data.status_code == 200:
        return data.text

def parse_data(data):
    data_list = json.loads(data)
    event_types = []

    for data_dict in data_list:
        event_types.append(data_dict["type"])

    return event_types

def collect_data():
    pass

def push_data(client, column, data):
    client.insert_row("git.github_events", data, column)


#result = pull_data("0Kee-Team", "WatchAD")
#data = parse_data(result)

#push_data("event_type", data)

