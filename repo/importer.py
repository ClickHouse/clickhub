import json
import logging
import os.path
import asyncio
import os
import subprocess
import sys
import time
from asyncio.subprocess import PIPE
import git
from git import Repo, InvalidGitRepositoryError
from clickhouse import DataType, RepoClickHouseClient
from datetime import datetime

ON_POSIX = 'posix' in sys.builtin_module_names


def connect_repo(repo_name: str, repo_folder: str):
    logging.info(f'connecting to repo {repo_name} at {repo_folder}')
    if os.path.exists(repo_folder):
        if not os.path.isdir(repo_folder):
            return Exception(f'[{repo_folder}] is not a folder')
        try:
            return Repo(repo_folder)
        except InvalidGitRepositoryError:
            # clean up dir and re-clone
            logging.error(f'unable to connect to repository [{repo_name}]')
        os.rmdir(repo_folder)
    logging.info(f'cloning repo [{repo_name}] to [{repo_folder}]')
    return git.Repo.clone_from(f'git@github.com:{repo_name}', repo_folder)


def update_repo(data_cache: str, repo_name: str):
    repo_folder = os.path.join(data_cache, repo_name)
    repo = connect_repo(repo_name, repo_folder)
    status = repo.git.status()
    if not None:
        logging.info(status)
    repo.git.pull()
    return repo_folder


async def read_stream_and_display(stream, display):
    """Read from stream line by line until EOF, display
    """
    output = []
    while True:
        line = await stream.readline()
        if not line:
            break
        output.append(line)
        display(line)  # assume it doesn't block
    return b''.join(output)


async def read_and_display(*cmd, cwd=os.getcwd(), stdin=None):
    """Capture cmd's stdout, stderr while displaying them as they arrive
        (line by line).

        """
    # start process
    process = await asyncio.create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE, stdin=stdin, cwd=cwd)
    # read child's stdout/stderr concurrently (capture and display)
    try:
        stdout, stderr = await asyncio.gather(
            read_stream_and_display(process.stdout, sys.stdout.buffer.write),
            read_stream_and_display(process.stderr, sys.stderr.buffer.write))
    except Exception:
        process.kill()
        raise
    finally:
        # wait for the process to exit
        rc = await process.wait()
    return rc, stdout, stderr


def is_valid_repo(repo_name):
    g = git.cmd.Git()
    try:
        g.ls_remote('-h', f'git@github.com:{repo_name}')
    except:
        return False
    return True


def git_import(repo_path, custom_params=[]):
    logging.info(f'generating git history at {repo_path}')
    loop = asyncio.get_event_loop()
    rc, _, _ = loop.run_until_complete(read_and_display('clickhouse', 'git-import', cwd=repo_path))
    return rc == 0


def clickhouse_import(client: RepoClickHouseClient, repo_path: str, repo_name: str, data_type: DataType):
    logging.info(f'handling {data_type.name} for {repo_name}')
    max_time = client.query_row(statement=f"SELECT max(time) FROM {data_type.table} WHERE repo_name='{repo_name}'")[0]
    logging.info(f'max time for {data_type.name} is {max_time}')
    logging.info(f'importing {data_type.name} for {repo_name}')
    client_args = ['clickhouse', 'client', '--host', client.config.host, '--user',
                   client.config.username, '--password', client.config.password,
                   '--port', str(client.config.native_port), '--throw_if_no_data_to_insert', '0']
    if client.config.secure:
        client_args.append('--secure')
    client_insert = subprocess.Popen(client_args + ['--query',
                                                    f'INSERT INTO {data_type.table} FORMAT Native'],
                                     stdin=subprocess.PIPE)
    ps = subprocess.Popen(('clickhouse', 'local', '--query', f"{data_type.statement.format(repo_name=repo_name)} "
                                                             f"WHERE time > '{max_time}' FORMAT Native"),
                          stdout=client_insert.stdin, cwd=repo_path)
    client_insert.communicate()
    return client_insert.returncode


def _remove_file(file_path):
    if not os.path.exists(file_path):
        logging.warning(f'[{file_path}] does not exist. Cannot remove.')
    try:
        os.remove(file_path)
        logging.info(f'removed file [{file_path}]')
    except:
        logging.exception(f'unable to remove [{file_path}]')


def import_repo(client: RepoClickHouseClient, repo_name: str, data_cache: str, types: list[DataType], keep_files=False):
    if not is_valid_repo(repo_name):
        raise Exception(f'cannot find remote repo [{repo_name}]')
    repo_path = update_repo(data_cache, repo_name)
    if not git_import(repo_path, []):
        raise Exception(f'unable to git-import [{repo_name}]')
    for data_type in types:
        if clickhouse_import(client, repo_path, repo_name, data_type) != 0:
            raise Exception(f'unable to import [{data_type.name}] for [{repo_name}] to ClickHouse')
        if not keep_files:
            _remove_file(os.path.join(repo_path, f'{data_type.name}.tsv'))


def _claim_job(client: RepoClickHouseClient, worker_id: str, task_table: str, retries=2):
    # find highest priority, oldest job thats not assigned - grab retries
    jobs = client.query_rows(f"SELECT repo_name FROM {task_table} WHERE worker_id = '' ORDER BY priority DESC, "
                             f"started_time ASC LIMIT {retries}")
    for job in jobs:
        repo_name = job[0]
        logging.info(f'attempting to claim {repo_name}')
        scheduled_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            # keeper map doesn't allow two threads to set here
            client.query_row(f"ALTER TABLE {task_table} UPDATE worker_id = '{worker_id}', "
                              f"started_time = '{scheduled_time}' WHERE repo_name = '{repo_name}' AND worker_id = ''")
            # this may either throw an exception if another worker gets there first OR return 0 rows if the
            # job has already been processed and deleted or claimed successfully
            claimed = client.query_row(f"SELECT count() FROM {task_table} WHERE worker_id='{worker_id}' "
                                       f"AND repo_name = '{repo_name}'")
            if claimed[0] == 1:
                logging.info(f'[{worker_id}] claimed repo [{repo_name}]')
                return repo_name
            else:
                logging.info(f'unable to claim repo [{repo_name}]. maybe already claimed.')
        except:
            logging.exception(f'unable to claim repo [{repo_name}]. maybe already claimed.')
    return None


def worker_process(client: RepoClickHouseClient, data_cache: str, task_table: str, worker_id: str,
                   types: list[DataType], sleep_time=10, keep_files=False):
    logging.info(f'starting worker {worker_id}')
    while True:
        logging.info(f'{worker_id} polling for messages')
        repo_name = _claim_job(client, worker_id, task_table)
        if repo_name is not None:
            logging.info(f'{str(worker_id)} is handling repo {repo_name}')
            try:
                import_repo(client, repo_name, data_cache, types, keep_files=keep_files)
            except Exception:
                logging.exception(f'[{str(worker_id)}] failed on repo [{repo_name}]')
            try:
                logging.info(f'cleaning up job [{repo_name}]')
                # always release the job so it can be scheduled
                client.query_row(f"DELETE FROM {task_table} WHERE repo_name='{repo_name}'")
            except:
                logging.exception(f'unable to clean up job [{repo_name}]. Manually clean.')
        logging.info(f'{worker_id} sleeping {sleep_time}s till next poll')
        time.sleep(sleep_time)
