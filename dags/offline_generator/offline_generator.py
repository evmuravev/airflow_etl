import functools
import os
from pathlib import Path
import time
import concurrent.futures
from typing import Callable, Dict, List
import yaml
from prompt_toolkit import PromptSession
from prompt_toolkit.completion import WordCompleter
from consolemenu import SelectionMenu
from jinja2 import Template


def process_yaml_file(file_path):
    with open(file_path, 'r') as file:
        try:
            data: Dict = yaml.safe_load(file)
            if data.get('dag_id'):
                return data
        except Exception as ex:
            print(f'Error during yaml parsing {ex}')


def find_yaml_files(root_dir):
    yaml_files = []
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for file in filenames:
            if file.endswith(".yaml") or file.endswith(".yml"):
                yaml_files.append(os.path.join(dirpath, file))
    return yaml_files


def print_generated_dag(dag_template, dag_config):
    with open(dag_template, 'r') as j2:
        template = Template(j2.read())

    rendered_code = template.render(dag_config)
    print(rendered_code, '\n')

    input("Press ENTER to continue...")


def save_generated_dag(dag_template, dag_config):
    with open(dag_template, 'r') as j2:
        template = Template(j2.read())

    rendered_code = template.render(dag_config)
    file_name = f'{dag_config["dag_id"]}.py'
    with open(file_name, 'w') as python_file:
        python_file.write(rendered_code)
    print(f'The DAG was saved to {Path.cwd()/file_name}\n')

    input("Press ENTER to continue...")


def print_yml_content(dag_config):
    print(yaml.dump(dag_config, default_flow_style=False), '\n')
    input("Press ENTER to continue...")


def main():
    yaml_root_directory = "/home/eugene/devs/Airflow etl/dags/FLOWS"
    DAG_TEMPLATE = './offline_generator/templates/dag_template.py.j2'

    with concurrent.futures.ThreadPoolExecutor() as executor:
        root_dir = yaml_root_directory  # Path(__file__).parent/'FLOWS'
        yaml_files = find_yaml_files(root_dir)
        yaml_data_list: List[Dict] = list(executor.map(process_yaml_file, yaml_files))
        yaml_data_list = [d for d in yaml_data_list if d is not None]

    dag_ids = [data['dag_id'] for data in yaml_data_list]
    completer = WordCompleter(dag_ids)

    session = PromptSession()

    MENU_ITEMS = {
        0: {
            "name": "Print generated DAG",
            "action": functools.partial(print_generated_dag, DAG_TEMPLATE)
        },
        1: {
            "name": "Save generated DAG",
            "action":  functools.partial(save_generated_dag, DAG_TEMPLATE)
        },
        2: {"name": "Print YML Content", "action": print_yml_content},
        3: {"name": "Return to DAG selection", "action": None},
    }

    while True:
        dag_id = session.prompt('Select DAG ID: ', completer=completer)
        try:
            selected_yaml_data = next(
                (data for data in yaml_data_list if data['dag_id'] == dag_id)
            )
        except StopIteration:
            print("Invalid DAG ID. Please try again.")
            continue

        while True:
            menu = SelectionMenu(
                strings=[MENU_ITEMS[i]['name'] for i in MENU_ITEMS],
                title=f"Select action for the {dag_id}"
            )
            menu.show()
            selected_action = menu.selected_option

            action: Callable = MENU_ITEMS[selected_action]['action']
            if action:
                action(selected_yaml_data)
            else:
                break


if __name__ == "__main__":
    main()
