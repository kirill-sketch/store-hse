import argparse
import os
from datetime import date, timedelta

from pyspark.sql import SparkSession, functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--load-date", required=True, type=date.fromisoformat)
load_date = parser.parse_args().load_date
start = load_date - timedelta(days=3)
spark = (SparkSession.builder
    .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT"))
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER"))
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic").getOrCreate())
source = spark.read.parquet("s3a://de-course/intermediate/int_orders_with_payments")
result = (
    source.filter((F.col("order_date") >= start) & (F.col("order_date") < load_date))
    .filter(~F.col("is_test_order"))
    .select("order_id", "customer_id", "order_date", "ordered_at", "order_status", "order_amount",
            "paid_amount", "refunded_amount", "net_paid_amount", "successful_payments_count",
            "first_paid_at", "is_confirmed"))
(
    result.write.mode("overwrite")
    .partitionBy("order_date")
    .parquet("s3a://de-course/marts/fct_orders")
)
spark.stop()
