from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import json
import logging

log = logging.getLogger(__name__)

NICKNAME = 'unbelievable'
COHORT = '45'
API_KEY = '25c27781-8fde-4b30-a22e-524044a7580f'
BASE_URL = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/'

HEADERS = {
    'X-Nickname': NICKNAME,
    'X-Cohort': COHORT,
    'X-API-KEY': API_KEY
}

default_args = {
    'owner': 'student'
}

def load_couriers():
    """Загрузка курьеров из API в stg.api_couriers"""
    log.info("Начало загрузки курьеров")
    
    pg_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("TRUNCATE TABLE stg.api_couriers;")
    
    offset = 0
    limit = 50
    all_couriers = []
    
    while True:
        params = {
            'limit': limit,
            'offset': offset,
            'sort_field': '_id',
            'sort_direction': 'asc'
        }
        
        response = requests.get(
            f'{BASE_URL}/couriers',
            headers=HEADERS,
            params=params
        )
        
        data = response.json()
        if not data:
            break
            
        all_couriers.extend(data)
        
        if len(data) < limit:
            break
            
        offset += limit
    
    log.info(f"Загружено {len(all_couriers)} курьеров из API")
    
    insert_sql = """
        INSERT INTO stg.api_couriers (object_id, object_value)
        VALUES (%s, %s::jsonb)
        ON CONFLICT (object_id) DO UPDATE SET
            object_value = EXCLUDED.object_value,
            load_ts = CURRENT_TIMESTAMP;
    """
    
    for courier in all_couriers:
        cursor.execute(insert_sql, (
            courier['_id'],
            json.dumps(courier, ensure_ascii=False)
        ))
    
    conn.commit()
    cursor.close()
    log.info(f"Сохранено {len(all_couriers)} курьеров в stg.api_couriers")


def load_deliveries():

    log.info("Начало загрузки доставок")
    
    pg_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("TRUNCATE TABLE stg.api_deliveries;")
    
    to_date = datetime.now()
    from_date = to_date - timedelta(days=7) # загружаем последние 7 дней
    
    to_str = to_date.strftime('%Y-%m-%d %H:%M:%S')
    from_str = from_date.strftime('%Y-%m-%d %H:%M:%S')
    
    log.info(f"Загрузка доставок с {from_str} по {to_str}")
    
    offset = 0
    limit = 50
    all_deliveries = []
    
    while True:
        params = {
            'from': from_str,
            'to': to_str,
            'limit': limit,
            'offset': offset,
            'sort_field': 'date',
            'sort_direction': 'asc'
        }
        
        response = requests.get(
            f'{BASE_URL}/deliveries',
            headers=HEADERS,
            params=params
        )
        
        data = response.json()
        if not data:
            break
            
        all_deliveries.extend(data)
        
        if len(data) < limit:
            break
            
        offset += limit
    
    log.info(f"Загружено {len(all_deliveries)} доставок из API")
    
    insert_sql = """
        INSERT INTO stg.api_deliveries (object_id, object_value)
        VALUES (%s, %s::jsonb)
        ON CONFLICT (object_id) DO UPDATE SET
            object_value = EXCLUDED.object_value,
            load_ts = CURRENT_TIMESTAMP;
    """
    
    for delivery in all_deliveries:
        cursor.execute(insert_sql, (
            delivery['delivery_id'],
            json.dumps(delivery, ensure_ascii=False)
        ))
    
    conn.commit()
    cursor.close()
    log.info(f"Сохранено {len(all_deliveries)} доставок в stg.api_deliveries")


with DAG(
    dag_id='05_load_courier_api_to_stg',
    description='Загрузка данных курьерской службы из в stg',
    default_args=default_args,
    schedule_interval='*/15 * * * *',  
    start_date=datetime(2026, 2, 26),
    catchup=False,
    tags=['api', 'couriers', 'delivery', 'stg', 'json'],
) as dag:
    
    load_couriers_task = PythonOperator(
        task_id='load_couriers',
        python_callable=load_couriers,
    )
    
    load_deliveries_task = PythonOperator(
        task_id='load_deliveries',
        python_callable=load_deliveries,
    )
    
    [load_couriers_task, load_deliveries_task]