import functools
import typing
import yaml
import os
import glob
import importlib.util
from airflow.utils.task_group import TaskGroup


def read_jobs(paths) -> typing.Dict[str, TaskGroup]:
    jobs = []
    for path in paths:
        for file_path in glob.glob(os.path.join(path, '*.py')):
            module_name = os.path.splitext(os.path.basename(file_path))[0]
            spec = importlib.util.spec_from_file_location(module_name, file_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            jobs += [module.job]
    return {job.tg_kwargs['group_id']: job() for job in jobs}


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
