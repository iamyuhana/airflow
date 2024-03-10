from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from common.common_func import regist2

with DAG(
    dag_id="dags_python_with_op_kwargs",
    schedule="05 19 * * *",
    start_date=pendulum.datetime(2024, 3, 9, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    regist2_t1 = PythonOperator(
        task_id='regist2_t1',
        python_callable=regist2,
        op_args=['hnyu', 'woman', 'kr', 'Seoul'],
        op_kwargs={'phone': '010-1234-1234', 'email': 'hnyu86@naver.com'}
    )
    
    regist2_t1