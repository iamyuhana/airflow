from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime, timedelta
import requests
import pendulum
import logging

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.today('UTC').add(days=-1),
    'email': ['xxx@xxx.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

@dag(default_args=default_args, schedule="@once")
def test_air_pollution_etl():
    """
    https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html
    """
    @task
    def get_data(**kwargs):
        params_json={'serviceKey': Variable.get("air_pollution_api_key"),
                     'returnType': 'json',
                     'numOfRows': '200',
                     'pageNo': '1',
                     'year': '2021',
                     'itemCode': 'PM10'
                    }
                
        resp_json = requests.get(Variable.get("air_pollution_api_url"), params = params_json)
        
        if resp_json.status_code == 200:
            result = resp_json.json()

        list_result = []
        for data in result['response']['body']['items']:
            data = [{"sn":data.get('sn'),
                     "dt":data.get('dataDate'),
                     "issue_dt":data.get('issueDate'),
                     "issue_gbn":data.get('issueGbn'),
                     "issue_time":data.get('issueTime'),
                     "issue_val":data.get('issueVal'),
                     "item_cd":data.get('itemCode'),
                     "clear_dt":data.get('clearDate'),
                     "clear_time":data.get('clearTime'),
                     "clear_val":data.get('clearVal'),
                     "district_nm":data.get('districtName'),
                     "move_nm":data.get('moveName')}]
            list_result.extend(data)

        return list_result

    @task
    def load_data(list_result: list):
        client = BigQueryHook(gcp_conn_id="bigquery_default").get_client()
        table_id = f"{Variable.get("gcp_project_nm")}.{Variable.get("gcp_dataset_nm")}.air_pollution"
        rows_to_insert = list_result

        errors = client.insert_rows_json(table_id, rows_to_insert)
        if errors == []:
            logging.info("New rows have been added.")
        else:
            logging.info("Encountered errors while inserting rows: {}".format(errors))
    
    list_result_data = get_data()
    load_data(list_result_data)

test_air_pollution_etl()
