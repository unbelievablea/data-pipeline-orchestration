from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

log = logging.getLogger(__name__)

default_args = {
    'owner': 'student'
}

def calculate_courier_reward(rate_avg, order_sum):
    if rate_avg < 4:
        return max(order_sum * 0.05, 100.0)
    elif rate_avg < 4.5:
        return max(order_sum * 0.07, 150.0)
    elif rate_avg < 4.9:
        return max(order_sum * 0.08, 175.0)
    else:
        return max(order_sum * 0.10, 200.0)

def build_courier_ledger():
    log.info("Расчет витрины курьеров")
    
    pg_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("TRUNCATE TABLE cdm.dm_courier_ledger;")

    query = """
        WITH courier_stats AS (
            SELECT 
                c.id AS courier_id,
                c.courier_name,
                EXTRACT(YEAR FROM t.ts) AS settlement_year,
                EXTRACT(MONTH FROM t.ts) AS settlement_month,
                COUNT(DISTINCT o.id) AS orders_count,
                SUM(ps.total_sum) AS orders_total_sum,
                AVG(d.rate) AS rate_avg,
                SUM(d.tip_sum) AS courier_tips_sum
            FROM dds.dm_orders o
            JOIN dds.dm_timestamps t ON o.timestamp_id = t.id
            JOIN dds.fct_product_sales ps ON o.id = ps.order_id
            JOIN dds.dm_couriers c ON o.courier_id = c.id
            JOIN dds.fct_deliveries d ON o.id = d.order_id
            WHERE o.order_status = 'CLOSED'
            GROUP BY c.id, c.courier_name, settlement_year, settlement_month
        )
        SELECT 
            courier_id,
            courier_name,
            settlement_year,
            settlement_month,
            orders_count,
            orders_total_sum,
            rate_avg,
            courier_tips_sum
        FROM courier_stats
        ORDER BY courier_id, settlement_year, settlement_month;
        """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    log.info(f"Получено {len(rows)} строк для расчёта витрины")
    
    ledger_data = []
    
    for row in rows:
        courier_id = row[0]
        courier_name = row[1]
        year = int(row[2])
        month = int(row[3])
        orders_count = row[4]
        orders_total_sum = float(row[5])
        rate_avg = float(row[6])
        courier_tips_sum = float(row[7])
        order_processing_fee = orders_total_sum * 0.25
        courier_order_sum = 0
        if orders_count > 0:
            avg_order_sum = orders_total_sum / orders_count
            courier_order_sum = calculate_courier_reward(rate_avg, avg_order_sum) * orders_count
        
        courier_reward_sum = courier_order_sum + courier_tips_sum * 0.95
        
        ledger_data.append((
            courier_id,
            courier_name,
            year,
            month,
            orders_count,
            orders_total_sum,
            round(rate_avg, 2),
            round(order_processing_fee, 2),
            round(courier_order_sum, 2),
            round(courier_tips_sum, 2),
            round(courier_reward_sum, 2)
        ))
    
    log.info(f"Подготовлено {len(ledger_data)} записей для вставки в витрину")
    
    if ledger_data:
        insert_sql = """
            INSERT INTO cdm.dm_courier_ledger (
                courier_id, courier_name, settlement_year, settlement_month,
                orders_count, orders_total_sum, rate_avg,
                order_processing_fee, courier_order_sum, courier_tips_sum,
                courier_reward_sum
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (courier_id, settlement_year, settlement_month) 
            DO UPDATE SET
                orders_count = EXCLUDED.orders_count,
                orders_total_sum = EXCLUDED.orders_total_sum,
                rate_avg = EXCLUDED.rate_avg,
                order_processing_fee = EXCLUDED.order_processing_fee,
                courier_order_sum = EXCLUDED.courier_order_sum,
                courier_tips_sum = EXCLUDED.courier_tips_sum,
                courier_reward_sum = EXCLUDED.courier_reward_sum;
        """
        cursor.executemany(insert_sql, ledger_data)
        log.info(f"Вставлено {len(ledger_data)} записей в cdm.dm_courier_ledger")
    
    conn.commit()
    cursor.close()
    log.info("Расчёт витрины курьеров завершён")


with DAG(
    dag_id='08_cdm_courier_ledger',
    description='Витрина для расчётов с курьерами',
    default_args=default_args,
    schedule_interval='0/30 * * * *',  
    start_date=datetime(2026, 2, 26),
    catchup=False,
    tags=['cdm', 'couriers', 'ledger'],
) as dag:
    
    build_ledger = PythonOperator(
        task_id='build_courier_ledger',
        python_callable=build_courier_ledger,
    )