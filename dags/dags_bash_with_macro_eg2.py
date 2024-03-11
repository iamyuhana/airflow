from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_macro_eg2",
    schedule="28 10 * * 6#2",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    # START_DATE: 2주전 월요일, END_DATE: 2주전 토요일
    bash_task_t2 = BashOperator(
        task_id='bash_task_t2',
        env={
                'START_DATE': '{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=19) ) | ds }}',
                'END_DATE'  : '{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=14) ) | ds }}'
            },
        bash_command='echo "Start date is $START_DATE"' +
                     ' echo "End date is $END_DATE"' 
    )