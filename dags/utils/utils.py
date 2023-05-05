import functools
import gc
import inspect
import typing
import yaml
import os
import glob
import importlib.util
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task_group
from airflow.operators.python import PythonOperator
from airflow import AirflowException


def clean_up_dag(dag):
    for k in list(dag.task_group.children.keys()):
        del dag.task_group.children[k]
    gc.collect()


def read_jobs(paths) -> typing.Dict[str, TaskGroup]:
    # get dag object from parent module)
    dag = inspect.stack()[2].frame.f_locals['dag_obj']
    jobs: typing.List[typing.Callable] = []
    for path in paths:
        for file_path in glob.glob(os.path.join(path, '*.py')):
            module_name = os.path.splitext(os.path.basename(file_path))[0]
            spec = importlib.util.spec_from_file_location(module_name, file_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            print(module)
            jobs += [module.job]
    result = {}

    for _job in jobs:
        job_id = _job.tg_kwargs['group_id']
        try:
            # test job
            _job.tg_kwargs['group_id'] = _job.tg_kwargs['group_id'] + '_test'
            _job()
        except Exception as ex:
            # If error - create empty task group (red)
            @task_group(
                group_id=job_id,
                ui_color="#ff0000",
                tooltip=f"Error in job: {ex}"
            )
            def job():
                def bad_job(ex):
                    raise AirflowException(f"Error in job: {ex}")
                PythonOperator(task_id="bad_job", python_callable=bad_job)

            result[job_id] = job
        else:
            _job.tg_kwargs['group_id'] = job_id
            result[job_id] = _job
    # remove all test jobs
    clean_up_dag(dag)

    return {job_id: job() for job_id, job in result.items()}


def read_config(config_path) -> dict:
    if config_path.exists():
        with open(config_path, "r") as config_file:
            config = yaml.safe_load(config_file)
        return config


def register_dependencies(jobs, jobs_config):
    for job_id in jobs_config:
        job = jobs_config[job_id]
        if job.get('depends_on'):
            for dependency in job['depends_on']:
                if dependency in jobs:
                    jobs[job_id].set_upstream(jobs[dependency])


def get_leaf_tasks(jobs):
    return [job for job_id, job in jobs.items() if len(job.downstream_list) == 0]


def get_root_tasks(jobs):
    return [job for job_id, job in jobs.items() if len(job.upstream_group_ids) == 0]


def enable_job(func=None, *, enable=True):
    if func is None:
        return functools.partial(enable_job, enable=enable)

    if not enable:
        func.tg_kwargs['ui_color'] = "#737373"
        func.tg_kwargs['tooltip'] = "Disabled job"
        def job(): ...
        func.function = job

    return func
