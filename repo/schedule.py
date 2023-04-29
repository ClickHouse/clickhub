import logging
import sys
import time
from clickhouse_connect.driver.exceptions import DatabaseError
from clickhouse import RepoClickHouseClient
from repo.importer import is_valid_repo


class NoRepo(Exception):
    pass

class AlreadyScheduled(Exception):
    pass

def is_job_scheduled(client: RepoClickHouseClient, task_table, repo_name):
    response = client.query_row(f"SELECT * FROM {task_table} WHERE repo_name='{repo_name}'")
    if response is not None:
        return True
    return False


def queue_length(client: RepoClickHouseClient, task_table):
    response = client.query_row(f"SELECT count() FROM {task_table}")
    return int(response[0])


# we assume scheduler is single threaded at the moment (pending KeeperMap transactions).
# Note we impose a limit here on jobs. Priority also currently ignored.
def schedule_repo_job(client: RepoClickHouseClient, task_table: str, repo_name: str,
                      priority: int, max_queue_length=sys.maxsize):
    if queue_length(client, task_table) > max_queue_length:
        raise Exception(f'cannot schedule [{repo_name}]. Max queue size [{max_queue_length}] exceeded.')
    if not is_valid_repo(repo_name):
        raise NoRepo(f'cannot find remote repo {repo_name}')
    if is_job_scheduled(client, task_table, repo_name):
        raise AlreadyScheduled(f'job already scheduled for {repo_name}')
    scheduled_time = int(time.time())
    try:
        client.insert_row(task_table, ['repo_name', 'scheduled', 'priority', 'worker_id', 'started_time'],
                          [repo_name, scheduled_time, priority, '', 0])
        logging.info(f'[{repo_name}] scheduled')
    except DatabaseError as e:
        error = str(e)
        if 'KEEPER_EXCEPTION' in error and 'Transaction failed (Node exists)' in error:
            logging.error(f'[{repo_name}] is already scheduled')
        else:
            logging.exception(f'unable to schedule repo [{repo_name}]')
    except:
        logging.exception(f'unable to schedule repo [{repo_name}]')


# schedules all current repos based on repos in
def schedule_all_current_repos(client: RepoClickHouseClient, repo_table, task_table, priority,
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
        logging.info(f'repos currently scheduled, will be ignored: [{currently_scheduled}]')
        to_schedule = set(repo_batch) - set(currently_scheduled)
        logging.info(f'scheduling: [{to_schedule}]')
        for repo_name in list(to_schedule):
            try:
                # no max size here. We schedule all.
                schedule_repo_job(client, task_table, repo_name, priority)
            except:
                logging.exception(f'unable to schedule repo [{repo_name}]')


def bulk_schedule_repos(client: RepoClickHouseClient, task_table: str, filename: str,
                        priority: int, max_queue_length: int):
    with open(filename, 'r') as repos:
        for repo_name in repos:
            repo_name = repo_name.strip()
            try:
                schedule_repo_job(client, task_table, repo_name, priority, max_queue_length=max_queue_length)
            except Exception as e:
                logging.warning(f'unable to schedule repo [{repo_name}] - {str(e)}')
