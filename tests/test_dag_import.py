import pytest
from airflow.models import DagBag

def test_dag_import():
    # Load all DAG files from the airflow/dags directory
    dagbag = DagBag(dag_folder="airflow/dags", include_examples=False)
    
    # Assert that there are no import errors (like NameErrors, syntax errors, or broken imports)
    assert len(dagbag.import_errors) == 0, f"Airflow DAG Import Errors detected: {dagbag.import_errors}"
