from airflow.decorators import task_group, task
from pathlib import Path
from utils.utils import read_config, enable_job
from airflow.operators.empty import EmptyOperator


config_path = Path(__file__).parent / 'config.yml'
config = read_config(config_path)
job_id = config.get('job_id')
enable = config.get('enable', True)


@enable_job(enable=enable)
@task_group(
    group_id=job_id,
)
def job():
    extract = EmptyOperator(
        task_id="extract",
        inlets=[{'name': '123132', 'type': 'table'}, {'name': '4645645', 'type': 'view'}],
        outlets=[{'name': '76876', 'type': 'table'}]
    )
    transform = EmptyOperator(task_id="transform")
    load = EmptyOperator(
        task_id="load",
        inlets=[{'name': 'aaaa', 'type': 'table'}, {'name': 'bbbb', 'type': 'view'}],
        outlets=[{'name': 'cccc', 'type': 'table'}, {'name': 'dddd', 'type': 'table'}]
    )

    extract >> transform >> load
