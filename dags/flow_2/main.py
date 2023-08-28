import gc
import pendulum
from pathlib import Path
from utils.utils import (
    add_parent_jobs,
    read_jobs,
    read_config,
    register_dependencies,
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
    jobs_path = set()
    for path in Path(__file__).parent.glob('**/'):
        if path.name in jobs_ids:
            jobs_path.add(path)
            add_parent_jobs(path, jobs_path)

    jobs = read_jobs(jobs_path)
    jobs_config = {path.name: read_config(path/'config.yml') for path in jobs_path}
    register_dependencies(jobs, jobs_config)

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    start >> get_root_tasks(jobs)
    get_leaf_tasks(jobs) >> end
    gc.collect()


create_dag()
