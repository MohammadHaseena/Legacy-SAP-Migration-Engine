from datetime import datetime, timedelta
from airflow.sdk import dag
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

DATABRICKS_JOB_ID = 1234567889 # paste your Job ID here

@dag(
    dag_id="sap_medallion",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["sap", "databricks"],
)
def sap_medallion():
    DatabricksRunNowOperator(
        task_id="run_databricks_medallion",
        databricks_conn_id="databricks_default",
        job_id=DATABRICKS_JOB_ID,
        notebook_params={"run_date": "{{ ds }}"},
    )

sap_medallion()
