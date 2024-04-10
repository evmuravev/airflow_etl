import functools
import gc
import inspect
import yaml
from typing import Dict, List, Callable

from airflow import DAG, AirflowException
from airflow.decorators import task_group
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from dag_generator.job_types import JOB_TYPES


def read_jobs(jobs: List) -> Dict[str, TaskGroup]:
    job_configs: Dict[str, Dict] = dict()
    for job_config in jobs:
        job_configs[job_config["job_name"]] = job_config

    res = {}
    for job_name, config in job_configs.items():
        enable = config.get('enable', True)

        @enable_job(enable=enable)
        @task_group(group_id=job_name)
        def job(config):
            job_type, *sub_type = config.get('job_type').split('.')
            if sub_type:
                job_type: Callable = JOB_TYPES[job_type][sub_type[0]]()
            else:
                job_type: Callable = JOB_TYPES[job_type]()
            job_type(config)

        try:
            res[job_name] = job(config)
        except Exception as ex:
            error_msg = f"Error in job: {ex}"
            # get dag object from parent module
            dag: DAG = inspect.stack()[2].frame.f_locals['dag_obj']
            try:
                for task in dag.task_group_dict[job_name]:
                    dag._remove_task(task.task_id)
            except Exception:
                gc.collect()
            del dag.task_group.children[job_name]

            @task_group(
                group_id=job_name+'_error',
                ui_color="#ff0000",
                tooltip=error_msg
            )
            def job():
                def bad_job():
                    raise AirflowException(error_msg)
                PythonOperator(task_id="bad_job", python_callable=bad_job)
            res[job_name] = job()

    register_dependencies(res, job_configs)
    return res


def read_config(config_path) -> dict:
    if config_path.exists():
        with open(config_path, "r") as config_file:
            config = yaml.safe_load(config_file)
        return config


def register_dependencies(jobs: Dict[str, TaskGroup], jobs_config: Dict[str, Dict]):
    for job_name in jobs_config:
        job = jobs_config[job_name]
        if job.get('depends_on'):
            for dependency in job['depends_on']:
                if dependency in jobs:
                    jobs[job_name].set_upstream(jobs[dependency])


def get_leaf_tasks(jobs):
    return [job for job_name, job in jobs.items() if len(job.downstream_list) == 0]


def get_root_tasks(jobs):
    return [job for job_name, job in jobs.items() if len(job.upstream_group_ids) == 0]


def enable_job(func=None, *, enable=True):
    if func is None:
        return functools.partial(enable_job, enable=enable)

    if not enable:
        func.tg_kwargs['ui_color'] = "#737373"
        func.tg_kwargs['tooltip'] = "Disabled job"

        def job(*args, **kwargs):
            def disabled_job():
                print('This job was disabled')
            PythonOperator(task_id="disabled_job", python_callable=disabled_job)
        func.function = job

    return func
