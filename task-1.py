from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator

# DEFINING PROPERTIES OF TASKS
default_args = {"retries":2,
                "retry_delay":timedelta(minutes=1)}

# DEFINING PROPERTIES OF DAG
with DAG('table_dag',
         start_date=datetime(2023,4,28),
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:
    
    # CREATING A TABLE
    create_table=PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS customers (
                first_name TEXT NOT NULL,
                last_name INTEGER NOT NULL,
                email TEXT NOT NULL,
                age INTEGER NOT NULL
            );
        '''
    )

    # INSERTING VALUES IN TABLE
    insert_data=PostgresOperator(
        task_id='insert_data',
        postgres_conn_id='postgres',
        sql='''INSERT INTO customers VALUES( 
            ('Tom','Lee','tomlee@gmail.com',24), 
            ('John','Smith','john.smith@example.com',45), 
            ('Emma','Brown','emma.brown@example.com',23)
            );
        '''
    )

    # RUNNING QUERY ON TABLE
    run_query=PostgresOperator(
        task_id='run_query',
        postgres_conn_id='postgres',
        sql='''SELECT * 
            FROM customers;
        '''
    )

    # CREATING DEPENDENCY
    create_table >> insert_data >> run_query
