"""
Однократная подготовка данных в S3 для вебинара.

Запуск на сервере:
    docker exec demo-jupyter python /home/jovyan/work/scripts/prepare_s3.py

Создаёт в s3a://<bucket>/simulative/web-03/:
    good/countries, good/products, good/customers   — Parquet, несколько крупных файлов
    good/orders                                     — Parquet, partitionBy(order_month), 1 файл на месяц
    raw/orders_csv                                  — CSV с заголовком (для демо «CSV vs Parquet»)
    bad/orders_small_files                          — Parquet, partitionBy(order_date) × 4 файла на день
                                                      (≈2 900 мелких файлов — демо «проблема мелких файлов»)
"""
import os
import sys
import time

from pyspark.sql import SparkSession, functions as F

S3_ROOT = f"s3a://{os.environ['S3_BUCKET']}/{os.environ['S3_PREFIX']}"
PG_URL = f"jdbc:postgresql://{os.environ['PG_HOST']}:{os.environ['PG_PORT']}/{os.environ['POSTGRES_DB']}"
PG_PROPS = {
    "user": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
    "driver": "org.postgresql.Driver",
    "fetchsize": "10000",
}

spark = (
    SparkSession.builder.appName("prepare-s3-demo-data")
    .master("local[8]")
    .config("spark.driver.memory", "6g")
    .config("spark.ui.enabled", "false")
    .config("spark.hadoop.fs.s3a.endpoint", os.environ["S3_ENDPOINT"])
    .config("spark.hadoop.fs.s3a.endpoint.region", "ru-1")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["S3_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["S3_SECRET_KEY"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.maximum", "200")
    .config("spark.hadoop.fs.s3a.threads.max", "64")
    .config("spark.hadoop.fs.s3a.committer.name", "magic")
    .config("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
    .config("spark.sql.sources.commitProtocolClass",
            "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")
    .config("spark.sql.parquet.output.committer.class",
            "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter")
    .getOrCreate()
)


def jdbc(table, column=None, lo=None, hi=None, n=None):
    if column is None:
        return spark.read.jdbc(PG_URL, table, properties=PG_PROPS)
    return spark.read.jdbc(PG_URL, table, column=column, lowerBound=lo, upperBound=hi,
                           numPartitions=n, properties=PG_PROPS)


countries = jdbc("countries")
products = jdbc("products")
customers = jdbc("customers", "customer_id", 1, 2_000_000, 8)
orders = (
    jdbc("orders", "order_id", 1, 10_000_000, 16)
    .withColumn("order_date", F.to_date("created_at"))
    .withColumn("order_month", F.date_format("created_at", "yyyy-MM"))
)

ONLY = set(sys.argv[1:])  # пусто = всё; "good" / "raw" / "bad" = только этот набор


def step(name, fn):
    if ONLY and name.split("/")[0] not in ONLY and not name.startswith("local"):
        print(f"[skip] {name}", flush=True)
        return
    t0 = time.perf_counter()
    fn()
    print(f"[ok] {name}: {time.perf_counter() - t0:.1f} s", flush=True)


step("good/countries", lambda: countries.coalesce(1).write.mode("overwrite").parquet(f"{S3_ROOT}/good/countries"))
step("good/products", lambda: products.coalesce(1).write.mode("overwrite").parquet(f"{S3_ROOT}/good/products"))
step("good/customers", lambda: customers.repartition(4).write.mode("overwrite").parquet(f"{S3_ROOT}/good/customers"))

# Читаем orders из PostgreSQL один раз и дальше раскладываем из локального кеша
orders_local = "file:/tmp/orders_snapshot"
step("local snapshot orders", lambda: orders.write.mode("overwrite").parquet(orders_local))
orders = spark.read.parquet(orders_local)

step("good/orders (partitionBy order_month, 1 файл на месяц)", lambda: (
    orders.drop("order_date")
    .repartition("order_month")
    .sortWithinPartitions("created_at")
    .write.mode("overwrite").partitionBy("order_month").parquet(f"{S3_ROOT}/good/orders")
))

step("raw/orders_csv", lambda: (
    orders.drop("order_date", "order_month")
    .repartition(8)
    .write.mode("overwrite").option("header", True).csv(f"{S3_ROOT}/raw/orders_csv")
))

step("bad/orders_small_files (partitionBy order_date × 4 файла)", lambda: (
    orders.drop("order_month")
    .repartition(4)
    .write.mode("overwrite").partitionBy("order_date").parquet(f"{S3_ROOT}/bad/orders_small_files")
))

spark.stop()
print("DONE")
