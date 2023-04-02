from flask import Flask, request, render_template
from clickhouse_driver import Client


client = Client()
app = Flask(__name__)

@app.get("/process")
def process():
    repo = request.args.get('repo')
    max_queue_size = 5

    queue_size = client.execute('SELECT COUNT(repo_name) FROM work_queue')
    if queue_size == max_queue_size:
        return 'QUEUE IS FULL', 403
    
    repos_in_db = client.execute(f'SELECT COUNT(repo_name) FROM commits WHERE repo_name = {repo}')
    if repos_in_db >= 0:
        return 'ALREADY_PROCESSED', 200
    
    repos_in_queue = client.execute(f'SELECT COUNT(repo_name) FROM work_queue WHERE repo_name = {repo}')
    if repos_in_queue >= 0:
            return 'ALREADY_PROCESSING', 200
    
    return 'OK', 201