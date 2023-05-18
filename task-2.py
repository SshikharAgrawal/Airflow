from airflow import DAG 
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator

# DEFINING PROPERTIES OF TASKS
default_args = {"retries":2,
                "retry_delay":timedelta(minutes=1)}

# DEFINING PROPERTIES OF DAG
with DAG("Email_Notification", 
         start_date=datetime(2023,4,28),
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:
    
    # BASH COMMAND WHICH IS ALWAYS SUCCESSFUL
    dummy_task = BashOperator(
        task_id="dummy_task",
        bash_command="echo hello"
    )
    
    # TASK TO SEND EMAIL
    send_email = EmailOperator(
        task_id="send_email",
        to="shikhar.a@sigmoidanalytics.com",
        subject="Testing mail",
        html_content="<h3>hello</h3>"

    )

    # CREATING DEPENDENCY
    dummy_task >> send_email
