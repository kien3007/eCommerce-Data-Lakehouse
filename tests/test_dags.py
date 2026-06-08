import os
import sys
import pytest

if sys.platform == 'win32':
    import types
    sys.modules['fcntl'] = types.ModuleType('fcntl')

from airflow.models import DagBag

# Setup DagBag from the dags directory
DAG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'airflow', 'dags')

@pytest.fixture(scope="session")
def dag_bag():
    """
    Load Airflow DAGs from the dags directory.
    """
    return DagBag(dag_folder=DAG_DIR, include_examples=False)

def test_no_import_errors(dag_bag):
    """
    Test that there are no import errors when loading the DAGs.
    If a DAG has a syntax error or a missing library, this will fail.
    """
    assert len(dag_bag.import_errors) == 0, \
        f"DAG import failures: {dag_bag.import_errors}"

def test_dag_ids_are_expected(dag_bag):
    """
    Test that the expected DAGs are loaded successfully.
    """
    expected_dags = ['dag_daily_medallion_etl', 'dag_iceberg_maintenance']
    loaded_dags = dag_bag.dags.keys()

    for dag_id in expected_dags:
        assert dag_id in loaded_dags, f"Expected DAG '{dag_id}' not found."

def test_dags_have_tags(dag_bag):
    """
    Test that every DAG has at least one tag defined for easier filtering in UI.
    """
    for dag_id, dag in dag_bag.dags.items():
        assert dag.tags, f"DAG '{dag_id}' does not have any tags."

def test_daily_dag_task_order(dag_bag):
    """
    Test that tasks in the daily medallion DAG run in the correct order:
    run_bronze_to_silver >> run_silver_to_gold
    """
    dag = dag_bag.dags['dag_daily_medallion_etl']
    tasks = {t.task_id: t for t in dag.tasks}

    assert 'run_bronze_to_silver' in tasks
    assert 'run_silver_to_gold' in tasks

    # Verify dependency chain
    bronze_task = tasks['run_bronze_to_silver']
    gold_task = tasks['run_silver_to_gold']

    assert 'run_bronze_to_silver' in [t.task_id for t in gold_task.upstream_list], \
        "Gold task should depend on Bronze task"
