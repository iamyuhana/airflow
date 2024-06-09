from airflow import DAG
import pendulum
import datetime
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="dags_email_operator",
    schedule="45 19 * * *",
    start_date=pendulum.datetime(2024, 6, 8, tz="Asia/Seoul")
) as dag:
    
    send_email_task = EmailOperator(
        task_id='send_email_task',
        to='hnyu86@gmail.com',
        cc='hnyu86@naver.com',
        subject='Airflow 성공 메일',
        html_content='Airflow 작업이 완료되었습니다.'
    )