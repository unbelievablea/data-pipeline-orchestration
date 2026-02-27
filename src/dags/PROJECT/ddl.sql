-- Таблица оригинальных данных курьеров stg
CREATE TABLE IF NOT EXISTS stg.api_couriers (
    id SERIAL PRIMARY KEY,
    object_id VARCHAR NOT NULL UNIQUE,
    object_value JSONB NOT NULL,
    load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица оригинальных данных доставок stg
CREATE TABLE IF NOT EXISTS stg.api_deliveries (
    id SERIAL PRIMARY KEY,
    object_id VARCHAR NOT NULL UNIQUE,
    object_value JSONB NOT NULL,
    load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица курьеров dds 
CREATE TABLE IF NOT EXISTS dds.dm_couriers (
    id SERIAL PRIMARY KEY,
    courier_id VARCHAR NOT NULL UNIQUE,
    courier_name VARCHAR NOT NULL
);

-- Таблица фактов доставок в dds
CREATE TABLE IF NOT EXISTS dds.fct_deliveries (
    id SERIAL PRIMARY KEY,
    delivery_id VARCHAR NOT NULL UNIQUE,
    order_id INTEGER NOT NULL REFERENCES dds.dm_orders(id),
    courier_id INTEGER NOT NULL REFERENCES dds.dm_couriers(id),
    rate INTEGER NOT NULL CHECK (rate >= 1 AND rate <= 5),
    tip_sum NUMERIC(14,2) NOT NULL,
    delivery_ts TIMESTAMP NOT NULL,
    load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Витрина для расчётов с курьерами
CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
    id SERIAL PRIMARY KEY,
    courier_id INTEGER NOT NULL REFERENCES dds.dm_couriers(id),
    courier_name VARCHAR NOT NULL,
    settlement_year INTEGER NOT NULL,
    settlement_month INTEGER NOT NULL CHECK (settlement_month >= 1 AND settlement_month <= 12),
    orders_count INTEGER NOT NULL,
    orders_total_sum NUMERIC(14,2) NOT NULL,
    rate_avg NUMERIC(3,2) NOT NULL,
    order_processing_fee NUMERIC(14,2) NOT NULL,
    courier_order_sum NUMERIC(14,2) NOT NULL,
    courier_tips_sum NUMERIC(14,2) NOT NULL,
    courier_reward_sum NUMERIC(14,2) NOT NULL,
    UNIQUE(courier_id, settlement_year, settlement_month)
);


-- Добавляем ссылку на курьера в таблицу заказов
ALTER TABLE dds.dm_orders 
ADD COLUMN IF NOT EXISTS courier_id INTEGER REFERENCES dds.dm_couriers(id);


-- Очистка всех таблиц для перезапуска и тестов
DROP TABLE IF EXISTS cdm.dm_courier_ledger CASCADE;
DROP TABLE IF EXISTS dds.fct_deliveries CASCADE;
DROP TABLE IF EXISTS dds.dm_couriers CASCADE;
DROP TABLE IF EXISTS stg.deliveries CASCADE;
DROP TABLE IF EXISTS stg.couriers CASCADE;