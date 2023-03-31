import json
import logging
import time
from botocore.client import BaseClient
from clickhouse import RepoClickHouseClient

from repo.importer import is_valid_repo


def _is_job_scheduled(client: RepoClickHouseClient, task_table, repo_name):
    response = client.query_row(f"SELECT * FROM {task_table} WHERE repo_name='{repo_name}'")
    if response is not None:
        return True
    return False


# we assume scheduler is single threaded at the moment (pending KeeperMap transactions).
# Note we could impose a limit here on jobs or new jobs. Priority also currently ignored.
def schedule_repo_job(client: RepoClickHouseClient, sqs: BaseClient, queue_url: str, task_table: str, repo_name: str,
                      priority: int):
    if not is_valid_repo(repo_name):
        raise Exception(f'cannot find remote repo {repo_name}')
    if _is_job_scheduled(client, task_table, repo_name):
        raise Exception(f'job already scheduled for {repo_name}')
    scheduled_time = int(time.time())
    job_id = f'{repo_name}-{scheduled_time}'
    client.insert_row(task_table, ['repo_name', 'scheduled', 'priority'],
                      [repo_name, scheduled_time, priority])
    # replace with keeper map in future once we have ALTER TABLE transactions and can guarantee workers wont
    # take same job
    try:
        response = sqs.send_message(
            QueueUrl=queue_url,
            DelaySeconds=0,
            MessageAttributes={
                'repo_name': {
                    'DataType': 'String',
                    'StringValue': repo_name
                },
                'job_id': {
                    'DataType': 'String',
                    'StringValue': job_id
                }
            },
            MessageBody=(
                json.dumps({'repo_name': repo_name, 'scheduled_time': scheduled_time, 'job_id': job_id})
            ),
            MessageDeduplicationId=job_id,
            MessageGroupId=repo_name
        )
        message_id = response['MessageId']
        logging.info(f'scheduled job for repo {repo_name} with message {message_id}')
    except:
        # unblock other workers
        client.query_row(f"DELETE FROM {task_table} WHERE repo_name='{repo_name}'")
        logging.exception('unable to schedule job')


# schedules all current repos based on repos in
def schedule_all_current_repos(client: RepoClickHouseClient, sqs: BaseClient, queue_url: str, repo_table, task_table, priority,
                               repo_column='repo_name', time_column='updated_at', limit=50000):
    rows = client.query_rows(
        f'SELECT {repo_column}, min({time_column}) as last_updated FROM {repo_table} GROUP BY {repo_column} '
        f'ORDER BY last_updated ASC LIMIT {limit}')
    # check we don't have a job scheduled find those that have been scheduled and scheduled new. We batch requests
    # and use IN as optimal against keeper map
    batch_size = 1000
    repo_names = [repo[0] for repo in rows]
    logging.info(f'scheduling {len(repo_names)}...')
    repo_name_batches = [repo_names[i:i + batch_size] for i in range(0, len(repo_names), batch_size)]
    for repo_batch in repo_name_batches:
        repos = ', '.join([f"'{repo_name}'" for repo_name in repo_batch])
        statement = f"SELECT repo_name FROM {task_table} WHERE repo_name IN ({repos})"
        rows = client.query_rows(statement)
        currently_scheduled = [row[0] for row in rows]
        logging.info(f'repos currently scheduled, will be ignored: {currently_scheduled}')
        to_schedule = set(repo_batch) - set(currently_scheduled)
        logging.info(f'scheduling: {to_schedule}')
        for repo_name in list(to_schedule):
            schedule_repo_job(client, sqs, queue_url, task_table, repo_name, priority)


def schedule_all_repos(client: RepoClickHouseClient, sqs: BaseClient, queue_url: str, task_table: str, filename: str,
                      priority: int):
    with open(filename, 'r') as repos:
        for repo_name in repos:
            repo_name = repo_name.strip()
            if _is_job_scheduled(client, task_table, repo_name):
                logging.warning(f'skipping {repo_name} as already scheduled')
                continue
            if not is_valid_repo(repo_name):
                logging.warning(f'skipping {repo_name} as not valid')
                continue
            schedule_repo_job(client, sqs, queue_url, task_table, repo_name.strip(), priority)
