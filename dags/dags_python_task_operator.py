from airflow import DAG
import pendulum
from airflow.decorators import task

with DAG(
    dag_id="dags_python_task_decorator",
    schedule="18 20 * * *",
    start_date=pendulum.datetime(2024, 3, 9, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    @task(task_id="print_task_1")
    def print_context(some_input):
        print(some_input)
    
    print_task_1 = print_context("task_decorator 실행")