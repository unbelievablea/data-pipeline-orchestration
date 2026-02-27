## Витрина курьеров — что и откуда берём

**Поля витрины**  
- courier_id, courier_name - кто  
- settlement_year, settlement_month - когда  
- orders_count, orders_total_sum - сколько заказов и на какую сумму  
- rate_avg - средний рейтинг за месяц  
- order_processing_fee - комиссия 25%  
- courier_order_sum - оплата курьеру (зависит от рейтинга)  
- courier_tips_sum - чаевые  
- courier_reward_sum - итог: оплата + чаевые за вычетом 5%

**Откуда берём**  
- курьеры: новая таблица dds.dm_couriers (из API /couriers)  
- заказы и суммы: уже есть в dds.fct_product_sales  
- доставки и рейтинги: новая таблица dds.fct_deliveries (из API /deliveries)  
- даты: уже есть в dds.dm_timestamps  
- связь курьера с заказом: добавили поле courier_id в dds.dm_orders

**Что тянем из API**  
- /couriers - _id, name  
- /deliveries - delivery_id, order_id, courier_id, rate, tip_sum, delivery_ts