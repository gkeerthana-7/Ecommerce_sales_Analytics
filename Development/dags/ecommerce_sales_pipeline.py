from datetime import datetime

from airflow import DAG

from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

from airflow.providers.databricks.operators.databricks import (
    DatabricksNotebookOperator,
    DatabricksRunNowOperator,
)


# dbt Cloud Job IDs
SILVER_JOB_ID = 70506183139145
GOLD_JOB_ID = 70506183139146

# Slack Databricks Job ID
SLACK_JOB_ID = 770888403796059

# Existing Databricks Pytest notebook
PYTEST_NOTEBOOK = (
    "/Workspace/Users/anishkumarsingh703@gmail.com/"
    "PyTest.py"
)


with DAG(
    dag_id="ecommerce_sales_pipeline",
    start_date=datetime(2026, 9, 7),
    schedule=None,
    catchup=False,
    tags=["ecommerce", "dbt", "databricks", "pytest", "slack"],
) as dag:

    # Step 1: Run Silver transformation
    silver_dbt = DbtCloudRunJobOperator(
        task_id="silver_dbt",
        job_id=SILVER_JOB_ID,
        dbt_cloud_conn_id="dbt_cloud_new",
        wait_for_termination=True,
    )

    # Step 2: Run Gold transformation
    gold_dbt = DbtCloudRunJobOperator(
        task_id="gold_dbt",
        job_id=GOLD_JOB_ID,
        dbt_cloud_conn_id="dbt_cloud_new",
        wait_for_termination=True,
    )

    # Step 3: Run PyTest validation
    pytest_validation = DatabricksNotebookOperator(
        task_id="pytest_validation",
        databricks_conn_id="databricks_default",
        notebook_path=PYTEST_NOTEBOOK,
        source="WORKSPACE",
        existing_cluster_id="0907-172314-aapc4wbf",
        wait_for_termination=True,
    )

    # Step 4: Run Slack Databricks Job
    slack_job = DatabricksRunNowOperator(
        task_id="slack_job",
        databricks_conn_id="databricks_default",
        job_id=SLACK_JOB_ID,
        wait_for_termination=True,
    )

    # Pipeline order
    silver_dbt >> gold_dbt >> pytest_validation >> slack_job