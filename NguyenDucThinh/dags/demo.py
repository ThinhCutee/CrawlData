from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import subprocess
#import module
from data_clean import clean_data
from data_email import email
default_args = {
    'owner': 'thinhcute',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'crawl_dag',
    default_args=default_args,
    description='A DAG to run the crawling process',
    schedule_interval='@daily',
    start_date=datetime(2024, 5, 1),
    catchup=False,
)

crawl_command = """
cd /opt/airflow/crawldata && python test.py
"""

crawl_task = BashOperator(
    task_id='run_crawl',
    bash_command=crawl_command,
    dag=dag
)

clean_data_task = PythonOperator(
    task_id='run_clean',
    python_callable=clean_data,
    dag=dag
)
send_email_task = PythonOperator(
    task_id='send_email',
    python_callable=email,
    dag=dag
)
crawl_task >> clean_data_task >> send_email_task