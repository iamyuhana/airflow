import airflow import DAG
import pendulum
import datetime
from airflow.operator.email import EmailOperator
import util

with DAG(
    dag_id="dags_email_operator",
    schedule="0 8 1 * *",
    start_date=pendulum.datetime(2024, 3, 4, tz="Asia")
) as dag:
    
    send_email_task = EmailOperator(
        task_id='send_email_task',
        to=util.TO_EMAIL,
        cc=util.CC_EMAIL,
        subject='Airflow 성공 메일',
        html_contents='Airflow 작업이 완료되었습니다.'
    )