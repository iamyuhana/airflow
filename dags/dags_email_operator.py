from airflow import DAG
import pendulum
import datetime
from airflow.operators.email import EmailOperator
from common.common_func import EmailInfo

with DAG(
    dag_id="dags_email_operator",
    schedule="45 19 * * 6",
    start_date=pendulum.datetime(2024, 3, 8, tz="Asia/Seoul")
) as dag:
    
    send_email_task = EmailOperator(
        task_id='send_email_task',
        to='hnyu86@gmail.com',
        cc='hnyu86@naver.com',
        subject='Airflow 성공 메일',
        html_content='Airflow 작업이 완료되었습니다.'
    )