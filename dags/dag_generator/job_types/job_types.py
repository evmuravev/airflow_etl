import importlib


def delayed_import(func_path: str):
    module_name, func = func_path.rsplit('.', 1)

    def get_function():
        module = importlib.import_module(
            module_name,
            'dag_generator.job_types'
        )
        return getattr(module, func)

    return get_function


JOB_TYPES = {
    'dbt': {
        'dbt_model': delayed_import(
            '.dbt_loader.run_dbt_model'
        )
    },
    'dummy': delayed_import('.dummy.run_dummy_op'),
}
