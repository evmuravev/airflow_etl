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
from pathlib import Path


def read_jobs(jobs_ids, jobs_paths) -> typing.Dict[str, TaskGroup]:
    jobs_path = set()
    for path in jobs_paths:
        if path.name in jobs_ids:
            jobs_path.add(path)
            add_parent_jobs(path, jobs_path)

    jobs: typing.List[typing.Callable] = []
    res = {}
    for path in jobs_path:
        for file_path in glob.glob(os.path.join(path, '*.py')):
            module_name = os.path.splitext(os.path.basename(file_path))[0]
            spec = importlib.util.spec_from_file_location(module_name, file_path)
            module = importlib.util.module_from_spec(spec)
            try:
                spec.loader.exec_module(module)
                jobs += [module.job]
            except Exception as ex:
                error_msg = f"Error in job: {ex}"
                # get dag object from parent module
                dag = inspect.stack()[2].frame.f_locals['dag_obj']

                @task_group(
                    group_id=path.name+'_error',
                    ui_color="#ff0000",
                    tooltip=error_msg
                )
                def job():
                    def bad_job():
                        raise AirflowException(error_msg)
                    PythonOperator(task_id="bad_job", python_callable=bad_job)
                res[path.name] = job()

    for _job in jobs:
        job_id =_job.tg_kwargs['group_id']
        try:
            res[job_id] = _job()
        except Exception as ex:
            error_msg = f"Error in job: {ex}"
            # get dag object from parent module
            dag = inspect.stack()[2].frame.f_locals['dag_obj']
            try:
                for task in dag.task_group_dict[job_id]:
                    dag._remove_task(task.task_id)
            except Exception:
                gc.collect()
            del dag.task_group.children[job_id]

            @task_group(
                group_id=job_id+'_error',
                ui_color="#ff0000",
                tooltip=error_msg
            )
            def job():
                def bad_job():
                    raise AirflowException(error_msg)
                PythonOperator(task_id="bad_job", python_callable=bad_job)
            res[job_id] = job()

    jobs_config = {path.name: read_config(path/'config.yml') for path in jobs_path}
    register_dependencies(res, jobs_config)

    return res


def read_config(config_path) -> dict:
    if config_path.exists():
        with open(config_path, "r") as config_file:
            config = yaml.safe_load(config_file)
        return config


def add_parent_jobs(path: Path, jobs_path: set):
    job_config = read_config(path/'config.yml')
    deps = job_config.get('depends_on', [])
    for dep in deps:
        parent_path = path.with_name(dep)
        jobs_path.add(parent_path)
        add_parent_jobs(parent_path, jobs_path)


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

        def job():
            def disabled_job():
                print('This job was disabled')
            PythonOperator(task_id="disabled_job", python_callable=disabled_job)

        func.function = job

    return func
