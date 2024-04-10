import glob
import json
import os
import re
import yaml

from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator
from airflow.operators.bash import BashOperator
from collections import defaultdict
from pathlib import Path
from typing import Dict, List
from urllib.parse import parse_qs, urlparse

from cosmos import (
    DbtRunLocalOperator,
    DbtTaskGroup,
    ProfileConfig,
    ProjectConfig,
    RenderConfig
)
from cosmos.constants import LoadMode, TestBehavior
from cosmos.dbt.selector import SelectorConfig
from dag_generator.custom_operators.custom_cosmos import get_profile_mapping


def parse_jdbc_connection_string(connection_string: str) -> dict:
    connection_string = connection_string.replace('jdbc:', '')
    parsed_url = urlparse(connection_string)

    dbname_mapping = {
        'postgres': 'dbname',
    }

    dbtype = parsed_url.scheme
    connection_dict = {
        'dbtype': parsed_url.scheme,
        'host': parsed_url.hostname,
        'port': parsed_url.port,
        dbname_mapping.get(dbtype, 'dbname'): parsed_url.path.strip('/')
    }

    query_params = parse_qs(parsed_url.query)
    for key, value in query_params.items():
        connection_dict[key] = value[0]
    print('CONNECTION DICT:', connection_dict)
    return connection_dict


def get_profile_config(config: Dict) -> ProfileConfig:
    profile: Dict[str, str] = config.get('profile')
    profile_args = {"schema": profile.get('schema')}

    connection_url = BaseHook.get_connection(profile['dbt_conn_id'])
    conn_type = profile.get('conn_type', connection_url.conn_type)
    if connection_url.conn_type == 'jdbc':
        parsed_jdbc = parse_jdbc_connection_string(connection_url.host)
        profile_args.update(parsed_jdbc)
        conn_type += f"_{parsed_jdbc['dbtype']}"

    profile_mapping = get_profile_mapping(
        conn_id=profile.get('dbt_conn_id'),
        conn_type=conn_type,
        profile_args=profile_args
    )

    profile_config = ProfileConfig(
        profile_name=f"{config['job_name']}_{profile.get('dbt_conn_id')}",
        target_name=profile.get('target'),
        profile_mapping=profile_mapping
    )
    return profile_config


def run_dbt_model(config: Dict):
    project: str = config.get('project')

    render_config = RenderConfig(
        load_method=LoadMode.AUTOMATIC,
        test_behavior=TestBehavior.NONE,
        select=config.get('select', []),
        emit_datasets=True,
    )

    project_config = ProjectConfig(
        dbt_project_path=project,
        models_relative_path='models',
    )
    profile_config = get_profile_config(config)

    DbtTaskGroup(
        group_id=f'dbt_{config["job_name"]}',
        project_config=project_config,
        profile_config=profile_config,
        render_config=render_config,
        operator_args={
            "install_deps": True,
            "vars": '{"debug_mode": false}'
        }
    )
