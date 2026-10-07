# WEB-03 · Вебинар «Оптимизация PySpark»

Демо-стенд: PostgreSQL с подготовленными данными, Jupyter + PySpark 4.1, Spark UI, Spark History Server, данные в S3 (Timeweb).

## Доступы

| Сервис | Адрес | Доступ |
|---|---|---|
| Jupyter Server | http://45.144.220.99:8888/lab?token=fc9c375bc821d5f5ee69a56ccab73b00f969fb05100a46a0 | token: `fc9c375bc821d5f5ee69a56ccab73b00f969fb05100a46a0` |
| Spark UI | http://45.144.220.99:4040 | работает, пока в kernel жива SparkSession |
| Spark History Server | http://45.144.220.99:18080 | все прошлые запуски |
| PostgreSQL | `45.144.220.99:5432`, база `shop` | user `spark` / password `lw51yz3jgHSHPSaVYA16ichj` |
| S3 | `s3a://92083840-b111-497d-a1f5-f883b42b56b0/simulative/web-03/` | ключи в `infra/.env` |

JDBC URL для DBeaver / DataGrip: `jdbc:postgresql://45.144.220.99:5432/shop`

## Подключение к Jupyter из IDE

**VS Code**
1. Откройте `notebooks/spark_optimization_webinar.ipynb`.
2. *Select Kernel* → *Existing Jupyter Server…* → вставьте
   `http://45.144.220.99:8888/?token=fc9c375bc821d5f5ee69a56ccab73b00f969fb05100a46a0`
3. Выберите kernel *Python 3 (ipykernel)*.

**PyCharm Professional / IntelliJ IDEA Ultimate / DataSpell**
1. Откройте ноутбук → в тулбаре ноутбука выпадающий список *Jupyter Server* → *Configure Jupyter Server…*
   (или *Settings → Languages & Frameworks → Jupyter → Jupyter Servers*).
2. *Configured Server* → URL `http://45.144.220.99:8888/?token=fc9c375bc821d5f5ee69a56ccab73b00f969fb05100a46a0`.

Kernel выполняется на сервере: пароли и ключи приходят из переменных окружения контейнера, в ноутбуке их нет. Тот же ноутбук лежит на сервере в `/home/jovyan/work`, его можно открыть и в браузере через JupyterLab.

> Spark UI один на SparkSession. Если открыть второй ноутбук со своей сессией, его UI переедет на порт 4041 (проброшены 4040–4045). Перед вебинаром держите открытым один kernel.

## Данные

PostgreSQL, база `shop` (создаётся автоматически из `infra/initdb/01_shop_demo_data.sql`):

| Таблица | Строк | Роль в демо |
|---|---|---|
| `countries` | 200 | крошечный справочник → broadcast |
| `products` | 50 000 | маленький справочник → broadcast |
| `customers` | 2 000 000 | большая таблица → SortMergeJoin |
| `orders` | 10 000 000 | перекос: 60% у `customer_id = 1`, 15% `NULL`, 25% равномерно; `order_id` равномерный |
| `customer_revenue` | — | приёмник для демо записи из Spark |

S3, `simulative/web-03/` (создаётся `infra/scripts/prepare_s3.py`):

| Путь | Что это |
|---|---|
| `good/orders` | Parquet, `partitionBy(order_month)`, 24 файла по ~10 МБ |
| `good/customers`, `good/products`, `good/countries` | Parquet |
| `raw/orders_csv` | CSV с заголовком, 8 файлов, ~600 МБ |
| `bad/orders_small_files` | Parquet, `partitionBy(order_date)`, 2 920 файлов по ~100 КБ |
| `tmp/` | временные результаты демо записи, ноутбук сам их удаляет |

## Ноутбук

`notebooks/spark_optimization_webinar.ipynb`: каждый пример собран по схеме **❌ Как НЕ надо → ✅ Как надо → 📊 Метрики**.

1. Spark UI и метрики
2. PostgreSQL: партиционированное чтение, перекошенный `partitionColumn`, pushdown, пакетная запись
3. `explain()`: Exchange, SortMergeJoin, BroadcastHashJoin, бакетирование, AQE (план до и после выполнения)
4. Shuffle: агрегация до join, число партиций и AQE coalesce
5. Перекос: диагностика, AQE skew join, salting, NULL-ключи, окно по горячему ключу
6. S3: CSV и Parquet, partition pruning, мелкие файлы (чтение и запись), magic committer
7. Итоговая таблица «было → стало» и чеклист

`notebooks/spark_optimization_webinar_executed.ipynb` — та же тетрадь с результатами прогона на стенде. Это запасной вариант, если в прямом эфире что-то пойдёт не так.

Полный прогон занимает ~8–10 минут. Самые долгие ячейки: чтение 2 920 мелких файлов из S3 (~40–80 с) и запись через FileOutputCommitter. Время операций с S3 может заметно меняться от запуска к запуску.

## Обслуживание стенда

Файлы на сервере лежат в `/opt/spark-demo` (копия в `infra/`).

```bash
ssh root@45.144.220.99
```

```bash
cd /opt/spark-demo && docker compose ps
```

```bash
cd /opt/spark-demo && docker compose restart jupyter
```

Пересоздать данные в S3 (все наборы или только `good` / `raw` / `bad`):

```bash
docker exec demo-jupyter python /home/jovyan/work/scripts/prepare_s3.py bad
```

Пересоздать базу с нуля (данные генерируются ~1 минуту):

```bash
cd /opt/spark-demo && docker compose down && docker volume rm spark-demo_pgdata && docker compose up -d
```

## Безопасность

- Порты 8888, 4040–4045, 18080 и 5432 открыты в интернет. Jupyter закрыт токеном, PostgreSQL паролем, а **Spark UI и History Server без авторизации**. После вебинара закройте порты или ограничьте доступ своим IP (`ufw allow from <IP>`).
- Root-пароль сервера и ключи S3 передавались в чате. Их стоит сменить после вебинара.
