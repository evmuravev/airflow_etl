from typing import Dict
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset


def run_dummy_op(config: Dict):
    empty = EmptyOperator(task_id=config['job_name'])

    if config.get('datasets'):
        outlets = []

        for dataset in config.get('datasets'):
            outlets.append(Dataset(dataset))

        yield_outlets = EmptyOperator(
            task_id="yield_outlets",
            outlets=outlets
        )
        empty >> yield_outlets
