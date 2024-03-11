from airflow import DAG
import pendulum
import datetime
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_template",
    schedule="28 10 * * *",
    start_date=pendulum.datetime(2024, 3, 9, tz="Asia/Seoul"),
    catchup=True,
) as dag:
    
    bash_t1 = BashOperator(
        task_id='bash_t1',
        bash_command='echo "data_interval_end: {{ data_interval_end }} "'
    )

    bash_t2 = BashOperator(
        task_id='bash_t2',
        env={
                'START_DATE': '{{ data_interval_start | ds }}',
                'END_DATE': '{{ data_interval_end | ds }}'
            },
        bash_command='echo "Start date is $START_DATE"' +
                     ' echo "End date is $END_DATE"' 
    )