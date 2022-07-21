from psycopg2 import DateFromTicks
import pendulum

from airflow.decorators import dag, task

from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
import urllib.parse 
from datetime import datetime
import os
import requests
import pandas as pd
import io

@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 7, 20, tz="UTC"),
    catchup=False,
    tags=['final_projectv0.1'],
)
def taskflow_api_etl():

    @task()
    def extract_onetime(date_of_data):
        if(date_of_data.month >= 7):
            file_name = date_of_data.strftime("%B/%A, %b %-d %Y.xls")
        else:
            file_name = date_of_data.strftime("%B/%b %-d %Y.xls")

        "https://cycling.data.tfl.gov.uk/CycleCounters/Blackfriars/June/Jun%201%202018.xls"

        base_url = "https://cycling.data.tfl.gov.uk/CycleCounters/Blackfriars/"
        url = base_url + urllib.parse.quote(file_name)
        
        local_filename = urllib.parse.unquote(url).split('/')[-1]
        
        if (os.path.exists(local_filename)):
            os.remove(local_filename)
        print(url) 
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        
        print(f"{local_filename} written")
        return local_filename

    @task()
    def extract_daily(date_of_data):
        if(date_of_data.month >= 7):
            file_name = date_of_data.strftime("%B/%A, %b %-d %Y.xls")
        else:
            file_name = date_of_data.strftime("%B/%b %-d %Y.xls")

        "https://cycling.data.tfl.gov.uk/CycleCounters/Blackfriars/June/Jun%201%202018.xls"

        base_url = "https://cycling.data.tfl.gov.uk/CycleCounters/Blackfriars/"
        url = urllib.parse.quote(base_url + file_name)
        
        local_filename = urllib.parse.unquote(url).split('/')[-1]
        
        if (os.path.exists(local_filename)):
            os.remove(local_filename)

        print(url)
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192): 
                    f.write(chunk)

        print(f"{local_filename} written")
        return local_filename

    # [END extract]

    # [START load]
    @task()
    def load(data:str):
        response = None
        hook = S3Hook(aws_conn_id='data-engineer-final-projects-data-lake-1')
        bucket = hook.get_bucket(
                    bucket_name='bicycle')
        response = bucket.upload_file(data,data)                  
            
        print(response)


    @task()
    def load_transform():
        bucket_name_local='bicycle'
        hook = S3Hook(aws_conn_id='data-engineer-final-projects-data-lake-1')
        bucket = hook.get_bucket(
                    bucket_name=bucket_name_local)

        hook_postgres = PostgresHook(postgres_conn_id='data-engineer-final-projects-postgres-1')

        engine = hook_postgres.get_sqlalchemy_engine()

        for key in bucket.objects.all():
            obj = hook.get_key(key.key, bucket_name=bucket_name_local)
            data = obj.get()['Body'].read()
            df = pd.read_csv(io.BytesIO(data))
            df.to_sql('bicycle_data',con=engine,if_exists='append',index=False)

    drop_table = PostgresOperator(
            task_id="drop_table",
            postgres_conn_id='data-engineer-final-projects-postgres-1',
            sql="""
                DROP TABLE IF EXISTS bicycle_data;
            """,
        )
    # [END load]

    # [START main_flow]
    drop_table
    load_array=[]
    year_month = [datetime(2018,7,i) for i in range(1,32)]
    for date in year_month:
        extraction = extract_onetime(date)
        load_array.append(load(extraction))
    load_array >> load_transform()


# [START dag_invocation]
tutorial_etl_dag = taskflow_api_etl()
# [END dag_invocation]

# [END tutorial]