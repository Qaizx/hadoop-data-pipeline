from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="finance_etl_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
) as dag:

    wait_for_file = BashOperator(
        task_id="wait_for_raw_file",
        bash_command="""
        docker exec namenode hdfs dfs -test -e /datalake/raw/finance
        """,
    )

    run_pipeline = BashOperator(
        task_id="run_spark_pipeline",
        bash_command="""
        docker exec spark-master spark-submit /jobs/finance_itsc_pipeline.py
        """,
    )

    run_pipeline
