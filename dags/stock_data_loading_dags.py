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

fastapi_host = 'http://'

with DAG(
    dag_id="load_monthly_data",
    catchup=False,
    start_date=pendulum.datetime(2024, 3, 1, tz="UTC"),
    schedule='@monthly',
    tags=["stock_data_pipeline"],
) as load_monthly_data:
    
    @task(task_id='start_loading')
    def start_loading():

        message = str()
        
        try:
            response = requests.put(
                f'{fastapi_host}/insert-monthly-data'
            )
            
            message = json.dumps(response.json())

            if response.json()["status_code"] != "200":
                raise Exception('Data was not loaded!')

            requests.post(url, params={
                'chat_id': chat_id,
                'text': message
            })

            status_tg = requests.post(url, params={
                'chat_id': chat_id,
                'text': 'Monthly Data was inserted'
            })

            return f"""
                    Monthly Data was inserted.
                    TG Status:
                    {json.dumps(status_tg.json())}
                    """
        except Exception as e:
            status_tg = requests.post(url, params={
                'chat_id': chat_id,
                'text': f"""
                        Error occured during loading monthly data.
                        {message}
                        """
            })
            raise Exception(message) + "\n" + json.dumps(status_tg.json())
    
    start_loading()

with DAG(
    dag_id="load_quarterly_data",
    catchup=False,
    start_date=pendulum.datetime(2024, 3, 1, tz="UTC"),
    schedule=timedelta(days=93),
    tags=["stock_data_pipeline"],
) as load_quarterly_data:
    
    @task(task_id='start_loading')
    def start_loading():

        message = str()
        
        try:
            response = requests.get(
                f'{fastapi_host}/insert-quarterly-data'
            )
            
            message = json.dumps(response.json())

            if response.json()["status_code"] != "200":
                raise Exception('Data was not loaded!')

            requests.post(url, params={
                'chat_id': chat_id,
                'text': message
            })

            status_tg = requests.post(url, params={
                'chat_id': chat_id,
                'text': 'Quarterly Data was inserted'
            })

            return f"""
                    Quarterly Data was inserted.
                    TG Status:
                    {json.dumps(status_tg.json())}
                    """
        except Exception as e:
            status_tg = requests.post(url, params={
                'chat_id': chat_id,
                'text': f"""
                        Error occured during loading monthly data.
                        {message}
                        """
            })
            raise Exception(message) + "\n" + json.dumps(status_tg.json())
    
    start_loading()

with DAG(
    dag_id="load_yearly_data",
    catchup=False,
    start_date=pendulum.datetime(2024, 3, 1, tz="UTC"),
    schedule='@yearly',
    tags=["stock_data_pipeline"],
) as load_yearly_data:
    
    @task(task_id='start_loading')
    def start_loading():

        message = str()
        
        try:
            response = requests.get(
                f'{fastapi_host}/insert-yearly-data'
            )
            
            message = json.dumps(response.json())

            if response.json()["status_code"] != "200":
                raise Exception('Data was not loaded!')
            
            requests.post(url, params={
                'chat_id': chat_id,
                'text': message
            })

            status_tg = requests.post(url, params={
                'chat_id': chat_id,
                'text': 'Yearly Data was inserted'
            })

            return f"""
                    Yearly Data was inserted.
                    TG Status:
                    {json.dumps(status_tg.json())}
                    """
        except Exception as e:
            status_tg = requests.post(url, params={
                'chat_id': chat_id,
                'text': f"""
                        Error occured during loading monthly data.
                        {message}
                        """
            })
            raise Exception(message) + "\n" + json.dumps(status_tg.json())
    
    start_loading()
