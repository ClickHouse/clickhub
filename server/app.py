from flask import Flask, request, render_template
from clickhouse import ClickHouse, RepoClickHouseClient, DataType
import argparse
from clickhub import load_config, load_types



parser = argparse.ArgumentParser(description='github importer',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)

parser.add_argument('-c', '--config', default='config.yml', help='config')
parser.add_argument('-d', '--debug', action='store_true', help='debug')

args = parser.parse_args()

config = load_config(args.config)
types = load_types()

clickhouse = ClickHouse(host=config['host'], port=config['port'], native_port=config['native_port'],
                        username=config['username'],
                        password=config['password'], secure=config['secure'])
client = RepoClickHouseClient(clickhouse)

app = Flask(__name__)

@app.get("/process")
def process():
    repo = request.args.get('repo')
    max_queue_size = 5

    rows_in_queue = client.query_rows(types[3].statement)
    queue_size = len(rows_in_queue)
    if queue_size == max_queue_size:
        return 'QUEUE IS FULL', 403
    
    repos_in_db = 0

    rows_in_commits = client.query_rows(types[1].statement)
    for row in rows_in_commits:
        if row[13] == repo:
             repos_in_db += 1
    
    if repos_in_db >= 0:
        return 'ALREADY_PROCESSED', 200
    
    repos_in_queue = 0

    for row in rows_in_queue:
         if row[0] == repo:
              repos_in_queue += 1
 
    if repos_in_queue >= 0:
            return 'ALREADY_PROCESSING', 200
    
    return 'OK', 201