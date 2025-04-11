# Домашнее задание Обработка и анализ данных в Yandex Cloud
# Задание 1: Работа с Yandex Data Proc и Hive/Spark
Фильтрация «хороших» валют (USD, EUR, RUB), подсчёт суммарной суммы транзакций по каждой валюте
```sh
spark.sql("""
    SELECT 
        currency,
        SUM(amount) as total_amount
    FROM transactions_v2
    WHERE currency IN ('USD', 'EUR', 'RUB')
    GROUP BY currency
    ORDER BY total_amount DESC
    """).show()
```
Подсчёт количества мошеннических (is_fraud=1) и нормальных (is_fraud=0) транзакций, суммарной суммы и среднего чека
```sh
spark.sql("""
    SELECT 
        is_fraud,
        COUNT(*) as transaction_count,
        SUM(amount) as total_amount,
        AVG(amount) as avg_amount
    FROM transactions_v2
    GROUP BY is_fraud
    ORDER BY is_fraud
    """).show()
```
Группировка по датам с вычислением ежедневного количества транзакций, суммарного объёма и среднего amount.
```sh
spark.sql("""
    SELECT 
        DATE(transaction_date) as transaction_date,
        COUNT(*) as transactions_count,
        SUM(amount) as total_amount,
        AVG(amount) as avg_amount
    FROM transactions_v2
    GROUP BY DATE(transaction_date)
    ORDER BY transaction_date
    """).show(truncate=False)
```
Использование временных функций (например, извлечение дня/месяца из transaction_date) и анализ транзакций по временным интервалам
```sh
daily_stats = spark.sql("""
    SELECT 
        DAY(transaction_date) as day_of_month,
        COUNT(*) as transactions_count,
        SUM(amount) as total_amount
    FROM transactions_v2
    GROUP BY DAY(transaction_date)
    ORDER BY day_of_month
    """)
```
JOIN с таблицей logs_v2 по transaction_id, чтобы посчитать количество логов на одну транзакцию, выделить самые частые категории category
```sh
print("Количество логов на одну транзакцию:")
transaction_logs = spark.sql("""
SELECT 
    t.transaction_id,
    COUNT(l.log_id) as logs_count,
    SUM(t.amount) as transaction_amount,
    t.currency
FROM transactions_v2 t
LEFT JOIN logs l ON t.transaction_id = l.transaction_id
GROUP BY t.transaction_id, t.currency
ORDER BY logs_count DESC
""")
transaction_logs.show()

print("\nСамые частые категории в логах:")
category_stats = spark.sql("""
SELECT 
    category,
    COUNT(*) as category_count,
    COUNT(DISTINCT transaction_id) as unique_transactions
FROM logs
GROUP BY category
ORDER BY category_count DESC
""")
category_stats.show()
```
