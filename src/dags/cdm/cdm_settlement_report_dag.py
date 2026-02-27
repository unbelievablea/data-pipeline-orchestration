import pendulum
from airflow.decorators import dag, task
from config_const import ConfigConst
from repositories.pg_connect import ConnectionBuilder

from cdm.settlement_report import SettlementReportLoader


@dag(
    dag_id='04_cdm_settlement_report',
    description='Создание и наполнение витрины для расчётов с ресторанами',
    schedule_interval='0/30 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'cdm', 'settlement'],
    is_paused_upon_creation=False
)
def cdm_settlement_report():

    @task
    def create_settlement_report_table():
        dwh_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_WAREHOUSE_CONNECTION)
        with dwh_pg_connect.client() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS cdm.dm_settlement_report (
                        id SERIAL PRIMARY KEY,
                        restaurant_id INTEGER NOT NULL REFERENCES dds.dm_restaurants(id),
                        restaurant_name VARCHAR NOT NULL,
                        settlement_date DATE NOT NULL,
                        orders_count INTEGER NOT NULL,
                        orders_total_sum NUMERIC(14,2) NOT NULL,
                        orders_bonus_payment_sum NUMERIC(14,2) NOT NULL,
                        orders_bonus_granted_sum NUMERIC(14,2) NOT NULL,
                        order_processing_fee NUMERIC(14,2) NOT NULL,
                        restaurant_reward_sum NUMERIC(14,2) NOT NULL,
                        UNIQUE(restaurant_id, settlement_date)
                    );
                """)
                conn.commit()

    @task
    def settlement_daily_report_load():
        """Загрузка данных в витрину"""
        dwh_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_WAREHOUSE_CONNECTION)
        rest_loader = SettlementReportLoader(dwh_pg_connect)
        rest_loader.load_report_by_days()
        print("Данные в витрину загружены")


    create_table = create_settlement_report_table()
    load_data = settlement_daily_report_load()

    create_table >> load_data



dag = cdm_settlement_report()