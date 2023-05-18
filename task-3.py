from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
import random

# DEFINING PROPERTIES OF TASKS
default_args = {"retries":2,
                "retry_delay":timedelta(minutes=1)}

# FUNCTION OF DUMMY TASK
def executeTask():
    
    # GENERATING A RANDOM NUMBER
    num=random.randint(1,100)
    
    # IF NUMBER IS EVEN THEN TASK FAILS ELSE IT PASSES
    if num%2==0:
        raise Exception()
    else:
        return 

# DEFINING PROPERTIES OF DAG
with DAG(dag_id='slack_dag',
         start_date=datetime(2023,4,28),
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:
    
    # PYTHON OPERATOR TO EXECUTE DUMMY TASK
    dummy_task=PythonOperator(
        task_id='dummy_task',
        python_callable=executeTask
    )

    # SLACKWEBHOOKOPERATOR FOR SUCCESSFUL TASK
    success_task=SlackWebhookOperator(
        task_id='success_task',
        http_conn_id='slack_conn',
        message='Task Successfull',
        channel="#airflow",
        trigger_rule='all_success'
    )

    # SLACKWEBHOOKOPERATOR FOR FAILED TASK
    fail_task=SlackWebhookOperator(
        task_id='fail_task',
        http_conn_id='slack_conn',
        message='Task Failed',
        channel="#airflow",
        trigger_rule='all_failed'
    )

    # SETTING UP DEPENDENCY
    dummy_task >> [success_task, fail_task]
