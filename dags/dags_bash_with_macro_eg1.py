from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_macro_eg1",
    schedule="28 10 L * *",    # L: 매월 말일
    start_date=pendulum.datetime(2024, 3, 9, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    # START_DATE: 전월 말일, END_DATE: 1일 전
    bash_task_t1 = BashOperator(
        task_id='bash_task_t1',
        env={
                'START_DATE': '{{ data_interval_start.in_timezone("Asia/Seoul") | ds }}',
                'END_DATE': '{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.deteutil.relativedelta.relativedelta(days=1)) | ds }}'
            },
        bash_command='echo "Start date is $START_DATE"' +
                     ' echo "End date is $END_DATE"' 
    )