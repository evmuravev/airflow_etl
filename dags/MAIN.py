
import concurrent.futures
import logging
import os
import pendulum
import yaml
from pathlib import Path
from typing import List, Dict
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset
from airflow.utils.dag_parsing_context import get_parsing_context

from dag_generator.utils import (
    read_jobs,
    get_leaf_tasks,
    get_root_tasks
)


logger = logging.getLogger('airflow.processor')
current_dag_id = get_parsing_context().dag_id


def process_yaml_file(file_path):
    with open(file_path, 'r') as file:
        try:
            data: Dict = yaml.safe_load(file)
            if data.get('dag_id'):
                return data
        except Exception as ex:
            logger.error(f'Error during yaml parsing {ex}')


def find_yaml_files(root_dir):
    yaml_files = []
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for file in filenames:
            if file.endswith(".yaml") or file.endswith(".yml"):
                yaml_files.append(os.path.join(dirpath, file))
    return yaml_files


with concurrent.futures.ThreadPoolExecutor() as executor:
    root_dir = config_path = Path(__file__).parent/'FLOWS'
    yaml_files = find_yaml_files(root_dir)
    dags_list: List[Dict] = list(executor.map(process_yaml_file, yaml_files))
    dags_list = [d for d in dags_list if d is not None]

for dag_config in dags_list:
    dag_id = dag_config.get('dag_id')

    if current_dag_id is not None and current_dag_id != dag_id:
        continue  # skip generation of non-selected DAG

    jobs_list = dag_config.get('jobs')
    schedule = dag_config.get('schedule')
    description = dag_config.get("description")
    tags = dag_config.get("tags")

    if isinstance(schedule, list):
        schedule = [Dataset(ds) for ds in schedule]

    @dag(
        dag_id=dag_id,
        start_date=pendulum.now(tz="Europe/Moscow"),
        schedule=schedule,
        description=description,
        tags=tags
    )
    def create_dag():
        try:
            jobs = read_jobs(jobs_list)
        except Exception as ex:
            jobs = {
                'bad_job': EmptyOperator(task_id="error", doc=ex)
            }
        else:
            start = EmptyOperator(task_id="start")
            end = EmptyOperator(task_id="end")
            root = get_root_tasks(jobs)
            leaf = get_leaf_tasks(jobs)

            start >> root
            leaf >> end

    create_dag()
