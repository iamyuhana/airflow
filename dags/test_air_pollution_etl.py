from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import pendulum


default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2024, 5, 1, tz="Asia/Seoul"),    # We recommend against using dynamic values as start_date, especially datetime.now() as it can be quite confusing.
    'email': ['hnyu86@naver.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

@dag(default_args=default_args, schedule="@once")
def test_air_pollution_etl():
    """
    https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html
    """
    import pandas as pd
    import requests
    import logging

    @task
    def get_data(**kwargs):
        params_json={'serviceKey': Variable.get("air_pollution_api_key"),
                     'returnType': 'json',
                     'numOfRows': '1000',
                     'pageNo': '1',
                     'year': '2024',
                     'itemCode': 'PM10'
                    }
                
        resp_json = requests.get(Variable.get("air_pollution_api_url"), params = params_json)
        
        if resp_json.status_code == 200:
            result = resp_json.json()

        # list_result = []
        # for data in result['response']['body']['items']:
        #     data = [{"sn":data.get('sn'),
        #              "dt":data.get('dataDate'),
        #              "issue_dt":data.get('issueDate'),
        #              "issue_gbn":data.get('issueGbn'),
        #              "issue_time":data.get('issueTime'),
        #              "issue_val":data.get('issueVal'),
        #              "item_cd":data.get('itemCode'),
        #              "clear_dt":data.get('clearDate'),
        #              "clear_time":data.get('clearTime'),
        #              "clear_val":data.get('clearVal'),
        #              "district_nm":data.get('districtName'),
        #              "move_nm":data.get('moveName'),
        #              "etlworktime":pendulum.now().to_datetime_string()}]
        #     list_result.extend(data)

        # return list_result
        df = pd.DataFrame(result['response']['body']['items'])
        df.rename(columns={'sn':'sn',
                           'dataDate': 'dt', 
                           'issueDate': 'issue_dt', 
                           'issueGbn': 'issue_gbn', 
                           'issueTime': 'issue_time', 
                           'issueVal': 'issue_val', 
                           'itemCode': 'item_cd', 
                           'clearDate': 'clear_dt', 
                           'clearTime': 'clear_time', 
                           'clearVal': 'clear_val', 
                           'districtName': 'district_nm',
                           'moveName': 'move_nm'}, 
                           inplace=True)
        df['sn'] = df['sn'].astype(pd.Int64Dtype())
        df['dt'] = pd.to_datetime(df['dt'])
        df['issue_dt'] = pd.to_datetime(df['issue_dt'])
        df['clear_dt'] = pd.to_datetime(df['clear_dt'])
        df['issue_val'] = df['issue_val'].astype(pd.Int64Dtype())
        df['clear_val'] = df['clear_val'].astype(pd.Int64Dtype())        
        df['etlworktime'] = pd.Timestamp.now()

        return df

    @task
    def load_data(df_result):
        client = BigQueryHook(gcp_conn_id="bigquery_default").get_client()
        table_id = f"{Variable.get("gcp_project_nm")}.{Variable.get("gcp_dataset_nm")}.air_pollution"
        # print(df_result.dtypes)
        client.load_table_from_dataframe(df_result, table_id).result
        
        table = client.get_table(table_id)  # Make an API request.
        print("Loaded {} rows and {} columns to {}".format(table.num_rows, len(table.schema), table_id))

        # rows_to_insert = list_result
        # errors = client.insert_rows_json(table_id, rows_to_insert)    # Streaming Insert(최근에 스트리밍된 일부 행을 일반적으로 최대 90분 동안 테이블 복사에 사용하지 못할 수 있다)
        # if errors == []:
        #     logging.info("New rows have been added.")
        # else:
        #     logging.info("Encountered errors while inserting rows: {}".format(errors))

        msg = "success"
        return msg

    @task
    def make_mart(msg_result):
        hook = BigQueryHook(gcp_conn_id="bigquery_default")
        query = f"CALL `{Variable.get("gcp_project_nm")}.{Variable.get("gcp_dataset_nm")}.sp_dw_air_pollution_m01`(\'2024\');"
        hook.insert_job(
            configuration = {
                'query': {
                    'query': query,
                    'useLegacySql': False
                }
            },
            project_id = Variable.get("gcp_project_nm")
        )

    df_result_data = get_data()
    msg_result_data = load_data(df_result_data)
    make_mart(msg_result_data)

test_air_pollution_etl()
