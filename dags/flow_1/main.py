import gc
import pendulum
from pathlib import Path
from utils import (
    read_jobs,
    read_config,
    get_leaf_tasks,
    get_root_tasks
)
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator


config_path = Path(__file__).parent/'config.yml'
config = read_config(config_path)
dag_id = config.get('dag_id')
jobs_ids = config.get('jobs')


@dag(
    dag_id=dag_id,
    start_date=pendulum.now(tz="Europe/Moscow"),
    schedule_interval=None,
)
def create_dag():
    jobs_paths = Path(__file__).parent.glob('**/')
    jobs = read_jobs(jobs_ids, jobs_paths)

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    start >> get_root_tasks(jobs)
    get_leaf_tasks(jobs) >> end
    gc.collect()


create_dag()
