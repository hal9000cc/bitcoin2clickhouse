# Bitcoin Blockchain Parser для ClickHouse

Этот проект предоставляет структуру таблиц ClickHouse и примеры кода для парсинга Bitcoin блоков и сохранения данных о входах и выходах транзакций.

## Структура таблиц

### 1. Таблица входов транзакций (`transaction_inputs`)

Содержит информацию о всех входах транзакций Bitcoin:

- **Идентификаторы**: хеш блока, высота блока, хеш транзакции
- **Данные входа**: индекс входа, предыдущая транзакция, sequence number
- **Скрипт**: hex представление, тип скрипта, длина
- **SegWit**: поддержка witness данных
- **Метаданные**: размер, coinbase флаг

### 2. Таблица выходов транзакций (`transaction_outputs`)

Содержит информацию о всех выходах транзакций Bitcoin:

- **Идентификаторы**: хеш блока, высота блока, хеш транзакции
- **Данные выхода**: индекс выхода, значение в сатоши, скрипт
- **Анализ скрипта**: типы (P2PKH, P2SH, P2WPKH, P2WSH, P2TR, multisig)
- **Адреса**: массив адресов и их типов
- **UTXO**: отслеживание потраченных/непотраченных выходов

## Установка и настройка

### 1. Установка зависимостей

```bash
# Установка ClickHouse
# Следуйте инструкциям на https://clickhouse.com/docs/en/install

# Установка Python зависимостей
pip install clickhouse-driver
pip install -e python-bitcoin-blockchain-parser/
```

### 2. Создание таблиц

```sql
-- Выполните SQL скрипт для создания таблиц
clickhouse-client < clickhouse_schema.sql
```

### 3. Запуск парсинга

```bash
# Парсинг всех блоков
python bitcoin_parser_example.py /path/to/bitcoin/blocks

# Парсинг определенного диапазона блоков
python bitcoin_parser_example.py /path/to/bitcoin/blocks 0 1000
```

## Особенности реализации

### Партиционирование
- Таблицы партиционированы по месяцам (`toYYYYMM(block_timestamp)`)
- Это обеспечивает эффективное управление данными и быстрые запросы

### Индексы
- **Bloom filter** для поиска по адресам
- **Set** индексы для типов скриптов
- **MinMax** индексы для значений

### Материализованные представления
- Статистика по типам скриптов
- Статистика по адресам
- Статистика UTXO
- Статистика SegWit adoption

## Примеры запросов

### Топ-10 адресов по количеству транзакций
```sql
SELECT 
    address,
    count() as tx_count,
    sum(value) / 100000000.0 as total_btc
FROM transaction_outputs
ARRAY JOIN addresses as address
GROUP BY address
ORDER BY tx_count DESC
LIMIT 10;
```

### Статистика по типам скриптов
```sql
SELECT 
    script_type,
    count() as count,
    sum(value) / 100000000.0 as total_btc
FROM transaction_outputs
WHERE block_timestamp >= now() - INTERVAL 1 MONTH
GROUP BY script_type
ORDER BY count DESC;
```

### Анализ SegWit adoption
```sql
SELECT 
    toYYYYMM(block_timestamp) as month,
    countIf(is_segwit = 1) as segwit_inputs,
    count() as total_inputs,
    segwit_inputs / total_inputs * 100 as segwit_percentage
FROM transaction_inputs
GROUP BY month
ORDER BY month;
```

### Поиск транзакций по адресу
```sql
SELECT 
    transaction_hash,
    block_height,
    value / 100000000.0 as btc_value,
    script_type
FROM transaction_outputs
WHERE has(addresses, '1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa')
ORDER BY block_height DESC;
```

## Производительность

### Рекомендации по оптимизации

1. **Батчевая вставка**: Используйте батчи по 1000-10000 записей
2. **Партиционирование**: Данные автоматически партиционируются по месяцам
3. **Индексы**: Используйте соответствующие индексы для ваших запросов
4. **Материализованные представления**: Для часто используемой аналитики

### Ожидаемая производительность

- **Вставка**: ~10,000-50,000 записей/сек
- **Запросы по индексу**: <100ms
- **Агрегация**: зависит от объема данных
- **Хранилище**: ~1-2 GB на 100,000 блоков

## Мониторинг

### Полезные запросы для мониторинга

```sql
-- Размер таблиц
SELECT 
    table,
    formatReadableSize(sum(bytes)) as size
FROM system.parts
WHERE database = currentDatabase()
GROUP BY table;

-- Статистика по партициям
SELECT 
    partition,
    count() as rows,
    formatReadableSize(sum(bytes)) as size
FROM system.parts
WHERE table = 'transaction_outputs'
GROUP BY partition
ORDER BY partition;
```

## Расширения

### Дополнительные таблицы

Можно добавить таблицы для:
- **Блоков**: метаданные блоков
- **Транзакций**: общая информация о транзакциях
- **Адресов**: нормализованная таблица адресов
- **UTXO**: текущее состояние непотраченных выходов

### Интеграция с другими системами

- **Grafana**: для визуализации
- **Apache Kafka**: для стриминга данных
- **Apache Spark**: для больших данных
- **Elasticsearch**: для полнотекстового поиска

## Лицензия

Проект использует библиотеку `python-bitcoin-blockchain-parser` под лицензией MIT.
