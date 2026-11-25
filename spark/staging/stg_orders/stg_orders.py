import argparse
import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BUCKET = 'de-course'
STG_ORDERS_PATH = f"s3a://{BUCKET}/staging/stg_orders"


spark = (
    SparkSession.builder
    .config("spark.hadoop.fs.s3a.endpoint", os.environ.get('MINIO_ENDPOINT'))
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get('MINIO_ROOT_USER'))
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get('MINIO_ROOT_PASSWORD'))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")

    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

parser = argparse.ArgumentParser()
parser.add_argument("--load-date", required=True, help="Дата запуска загрузки, ГГГГ-ММ-ДД")
args = parser.parse_args()

load_date = datetime.strptime(args.load_date, "%Y-%m-%d").date()
start_date = load_date - timedelta(days=3)


df_orders = (
    spark.read.parquet(f"s3a://{BUCKET}/raw/orders")
    .filter((F.col("order_created_dt") >= start_date) & (F.col("order_created_dt") < load_date))
)

df_stg_orders = (
    df_orders
    .select(
        F.col("order_id"),
        F.col("customer_id"),
        F.col("order_created_at").cast("timestamp").alias("ordered_at"),
        F.to_date("order_created_at").alias("order_date"),
        F.col("status").alias("order_status"),
        F.col("total_amount").alias("order_amount"),
        (F.col("status") == "test").alias("is_test_order"),
    )
)


(
    df_stg_orders.write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("order_date")
    .save(STG_ORDERS_PATH)
)

print(f"Сохранено: {STG_ORDERS_PATH}")

spark.stop()
