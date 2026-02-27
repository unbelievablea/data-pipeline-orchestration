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

def load_deliveries_to_dds():
    log.info("Начало загрузки доставок в DDS")
    
    pg_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, order_key FROM dds.dm_orders;")
    orders = {o[1]: o[0] for o in cursor.fetchall()}
    

    cursor.execute("SELECT id, courier_id FROM dds.dm_couriers;")
    couriers = {c[1]: c[0] for c in cursor.fetchall()}

    cursor.execute("""
        SELECT object_id, object_value 
        FROM stg.api_deliveries
        ORDER BY id;
    """)
    
    rows = cursor.fetchall()
    log.info(f"Прочитано {len(rows)} доставок из stg.api_deliveries")
    
    if not rows:
        return

    deliveries_for_dds = []
    orders_to_update = {}  
    
    for row in rows:
        delivery_id = row[0]
        delivery_data = row[1]
        
        order_key = delivery_data.get('order_id')
        courier_id = delivery_data.get('courier_id')
        
        if order_key not in orders:
            log.warning(f"Заказ {order_key} не найден в dm_orders")
            continue
            
        if courier_id not in couriers:
            log.warning(f"Курьер {courier_id} не найден в dm_couriers")
            continue
        
        order_dds_id = orders[order_key]
        courier_dds_id = couriers[courier_id]
        
        rate = delivery_data.get('rate')
        tip_sum = delivery_data.get('tip_sum', 0)
        delivery_ts = delivery_data.get('delivery_ts')
        
        deliveries_for_dds.append((
            delivery_id,
            order_dds_id,
            courier_dds_id,
            rate,
            tip_sum,
            delivery_ts
        ))
        
        orders_to_update[order_dds_id] = courier_dds_id
    
    log.info(f"Подготовлено {len(deliveries_for_dds)} доставок для вставки")
    
    if deliveries_for_dds:
        insert_sql = """
            INSERT INTO dds.fct_deliveries (
                delivery_id, order_id, courier_id, rate, tip_sum, delivery_ts
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (delivery_id) DO UPDATE SET
                rate = EXCLUDED.rate,
                tip_sum = EXCLUDED.tip_sum,
                delivery_ts = EXCLUDED.delivery_ts;
        """
        cursor.executemany(insert_sql, deliveries_for_dds)
        log.info(f"Вставлено {len(deliveries_for_dds)} записей в dds.fct_deliveries")
    
    if orders_to_update:
        update_sql = """
            UPDATE dds.dm_orders 
            SET courier_id = %s 
            WHERE id = %s;
        """
        update_data = [(cid, oid) for oid, cid in orders_to_update.items()]
        cursor.executemany(update_sql, update_data)
        log.info(f"Обновлено {len(update_data)} заказов (проставлен courier_id)")
    
    conn.commit()
    cursor.close()
    log.info("Загрузка доставок завершена")


with DAG(
    dag_id='07_stg_to_dds_deliveries',
    description='Перенос доставок из stg в dds и обновление заказов',
    default_args=default_args,
    schedule_interval='*/15 * * * *',  
    start_date=datetime(2026, 2, 26),
    catchup=False,
    tags=['stg', 'dds', 'deliveries', 'orders'],
) as dag:
    
    load_deliveries = PythonOperator(
        task_id='load_deliveries_to_dds',
        python_callable=load_deliveries_to_dds,
    )




    
