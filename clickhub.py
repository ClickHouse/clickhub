import argparse
import json
import logging
import os.path
import sys
import uuid
import boto3
import yaml
from clickhouse import ClickHouse, RepoClickHouseClient, DataType
from repo.importer import import_repo, worker_process
from repo.schedule import schedule_repo_job, schedule_all_current_repos, schedule_all_repos

parser = argparse.ArgumentParser(description='github importer',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)

parser.add_argument('-c', '--config', default='config.yml', help='config')
parser.add_argument('-d', '--debug', action='store_true', help='debug')
sub_parser = parser.add_subparsers(dest='command')

repo_name_parser = argparse.ArgumentParser(add_help=False)
repo_name_parser.add_argument('--repo_name', type=str, required=True)
repo_name_parser.add_argument('--keep_files', action='store_true', required=False, help='keep generated tsv files on '
                                                                                        'completion')

priority_parser = argparse.ArgumentParser(add_help=False)
priority_parser.add_argument('--priority', type=int, default=0)

schedule = sub_parser.add_parser('schedule', parents=[repo_name_parser, priority_parser],
                                 help='Schedule a repo for import (add queue to queue)')

worker = sub_parser.add_parser('start_worker', help='start a worker to consume from queue')
worker.add_argument('--id', type=str, default=str(uuid.uuid4()))

sub_parser.add_parser('import', parents=[repo_name_parser], help='import a repo')

sub_parser.add_parser('update_all_repos', parents=[priority_parser], help='schedule all current repos for update')

bulk_scheduler = sub_parser.add_parser('bulk_schedule', parents=[priority_parser],
                                       help='bulk schedule repos')
bulk_scheduler.add_argument('--file', type=str, default='repos.txt')

args = parser.parse_args()




def load_config(path):
    with open(path, 'r') as config_file:
        try:
            return yaml.safe_load(config_file)
        except yaml.YAMLError as exc:
            logging.error('invalid yaml configuration')
            raise exc


def load_types():
    with open('types.json', 'r') as types_file:
        body = json.load(types_file)
        types = []
        for type in body['types']:
            types.append(DataType(name=type['name'], table=type['table'], statement=type['statement']))
        return types


def is_tool(name):
    from shutil import which
    return which(name) is not None


if __name__ == '__main__':
    config = load_config(args.config)
    types = load_types()

    clickhouse = ClickHouse(host=config['host'], port=config['port'], native_port=config['native_port'],
                            username=config['username'],
                            password=config['password'], secure=config['secure'])
    client = RepoClickHouseClient(clickhouse)
    if args.command != 'start_worker':
        logging.basicConfig(encoding='utf-8', level=logging.DEBUG if args.debug else logging.INFO,
                            format='%(asctime)s %(levelname)s %(message)s')
    if args.command != 'import':
        # import doesn't need sqs
        sqs = boto3.client('sqs')
        try:
            queue = boto3.resource(
                'sqs',
                region_name=config['queue_region']
            ).get_queue_by_name(QueueName=config['queue_name'])
            pass
        except sqs.exceptions.QueueDoesNotExist:
            logging.fatal(f"queue {config['queue_name']} does not exist in region {config['queue_region']}")
            sys.exit(1)

    if args.command == 'schedule':
        logging.info(f'scheduling import of repo {args.repo_name}')
        try:
            schedule_repo_job(client, sqs, queue.url, config['task_table'], args.repo_name, args.priority)
        except Exception as e:
            logging.fatal(f'unable to schedule repo - {e}')
            sys.exit(1)
    elif args.command == 'bulk_schedule':
        logging.info(f'scheduling import of repos from file {args.file}')
        if os.path.exists(args.file) and os.path.isfile(args.file):
            try:
                schedule_all_repos(client, sqs, queue.url, config['task_table'], args.file, args.priority)
            except Exception as e:
                logging.fatal(f'unable to import repos from file {args.file} - {e}')
                sys.exit(1)
        else:
            logging.fatal(f'{args.file} does not exist or is not a file')
            sys.exit(1)
    elif args.command == 'update_all_repos':
        try:
            schedule_all_current_repos(client, sqs, queue.url, config['repo_lookup_table'], config['task_table'],
                                       args.priority)
        except Exception as e:
            logging.fatal(f'unable to update all repos - {e}')
            sys.exit(1)
    else:
        # these commands need clickhouse tooling
        if not is_tool('clickhouse'):
            logging.fatal('unable to find clickhouse on PATH')
            sys.exit(1)
        if args.command == 'import':
            import_repo(client, args.repo_name, config['data_cache'], types, keep_files=args.keep_files)
        elif args.command == 'start_worker':
            # workers log to file based on id
            logging.basicConfig(encoding='utf-8', level=logging.DEBUG if args.debug else logging.INFO,
                                format='%(asctime)s %(levelname)s %(message)s', filename=f'worker-log-{args.id}.log',
                                filemode='a')
            worker_process(client, sqs, queue.url, config['data_cache'], config['task_table'],
                           args.id, types, config['sleep_time'], keep_files=args.keep_files)
