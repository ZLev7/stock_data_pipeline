from __future__ import annotations
import pendulum
from airflow import DAG
from airflow.decorators import task
from datetime import timedelta
import requests
import json

bot_token = ''

chat_id = ''

url = f'https://api.telegram.org/bot{bot_token}/sendMessage'    

vps_host = ''

with DAG(
    dag_id="loading_monthly_data",
    catchup=False,
    start_date=pendulum.datetime(2024, 3, 1, tz="UTC"),
    schedule='@monthly',
    tags=["produces", "dataset-scheduled"],
) as dag1:
    
    message = str()

    @task(task_id='start_loading_stock_data')
    def start_loading():

        response = requests.put(
                f'{vps_host}/insert-monthly-data'
            )

        message = json.dumps(response.json())

        return 'Monthly Data was inserted'
    
    @task(task_id='send_status_to_tg')
    def send_status_to_tg():

        requests.post(url, params={
            'chat_id': chat_id,
            'text': message
        })

    start_loading() >> send_status_to_tg()

with DAG(
    dag_id="loading_quarterly_data",
    catchup=False,
    start_date=pendulum.datetime(2024, 3, 1, tz="UTC"),
    schedule=timedelta(days=95),
    tags=["produces", "dataset-scheduled"],
) as dag1:
    
    message = str()

    @task(task_id='start_loading_stock_data')
    def start_loading():

        response = requests.put(
            f'{vps_host}/insert-quarterly-data'
        )

        message = json.dumps(response.json())

        return 'Quarterly Data was inserted'
    
    @task(task_id='send_status_to_tg')
    def send_status_to_tg():

        requests.get(url, params={
            'chat_id': chat_id,
            'text': message
        })

    start_loading() >> send_status_to_tg()

with DAG(
    dag_id="loading_yearly_data",
    catchup=False,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule='@yearly',
    tags=["produces", "dataset-scheduled"],
) as dag1:
    
    message = str()

    @task(task_id='start_loading_stock_data')
    def start_loading():

        response = requests.put(
            f'{vps_host}/insert-yearly-data'
        )

        message = json.dumps(response.json())

        return 'Yearly Data was inserted'
    
    @task(task_id='send_status_to_tg')
    def send_status_to_tg():

        requests.get(url, params={
            'chat_id': chat_id,
            'text': message
        })

    start_loading() >> send_status_to_tg()