# Оркестрация расширенного ETL в Airflow

Проект по построению сложного ETL-пайплайна с использованием Apache Airflow.

## Задача

Разработать DAG с ветвлением, динамическими задачами и обработкой ошибок для загрузки данных из нескольких источников в хранилище.

## Технологии

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)

## Что сделано

- Реализован DAG с динамическим созданием задач (через `expand()` или цикл)
- Настроены зависимости между задачами с использованием `BranchPythonOperator` для ветвления логики
- Добавлены уведомления об ошибках (слак/email) и повторные попытки
- Настроены сенсоры для ожидания появления файлов/данных
- Выполнена декомпозиция DAG на переиспользуемые задачи (TaskGroups)

## Результат

- Пайплайн обрабатывает данные из 3+ источников параллельно
- При ошибке в одном источнике — остальные продолжают работу
- DAG сам восстанавливается после сбоев (retries + backfill)
- Время выполнения сокращено за счёт параллелизации независимых задач
