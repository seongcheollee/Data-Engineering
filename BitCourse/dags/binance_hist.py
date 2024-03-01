import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


dag = DAG(
    dag_id = "binance_hist",
    default_args = {
        "owner": "seongcheol Lee",
        "start_date": airflow.utils.dates.days_ago(1)
    },
)


python_job = SparkSubmitOperator(
    task_id="kline_data_extract_hist",
    conn_id="spark-conn",
    application="jobs/python/binanceHistory.py",
    jars="/opt/airflow/dags/jars/postgresql-42.7.1.jar",
    dag=dag
)

python_job