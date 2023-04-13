from airflow.decorators import task_group, task
from pathlib import Path
from utils.utils import read_config
from airflow.operators.empty import EmptyOperator


config_path = Path(__file__).parent / 'config.yml'
config = read_config(config_path)
job_id = config.get('job_id')


@task_group(group_id=job_id)
def job():
    extract = EmptyOperator(task_id="extract")
    transform = EmptyOperator(task_id="transform")
    load = EmptyOperator(task_id="load")

    extract >> transform >> load
