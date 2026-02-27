from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
import logging

log = logging.getLogger(__name__)

default_args = {
    'owner': 'student'
}

def load_couriers_to_dds():
    log.info("Начало загрузки курьеров в DDS")
    
    pg_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT object_id, object_value 
        FROM stg.api_couriers
        ORDER BY id;
    """)
    
    rows = cursor.fetchall()
    log.info(f"Прочитано {len(rows)} записей из stg.api_couriers")
    
    if not rows:
        return
    
    couriers_for_dds = []
    for row in rows:
        object_id = row[0]
        courier_data = row[1]
        
        courier_name = courier_data.get('name')
        couriers_for_dds.append((object_id, courier_name))
    
    insert_sql = """
        INSERT INTO dds.dm_couriers (courier_id, courier_name)
        VALUES (%s, %s)
        ON CONFLICT (courier_id) DO UPDATE SET
            courier_name = EXCLUDED.courier_name;
    """
    
    cursor.executemany(insert_sql, couriers_for_dds)
    
    conn.commit()
    cursor.close()
    log.info(f"Загружено {len(couriers_for_dds)} курьеров в dds.dm_couriers")


with DAG(
    dag_id='06_stg_to_dds_couriers',
    description='Перенос курьеров из stg в dds',
    default_args=default_args,
    schedule_interval='*/15 * * * *',
    start_date=datetime(2026, 2, 26),
    catchup=False,
    tags=['stg', 'dds', 'couriers'],
) as dag:
    
    load_couriers = PythonOperator(
        task_id='load_couriers_to_dds',
        python_callable=load_couriers_to_dds,
    )