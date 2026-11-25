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
orders = (
    spark.read.parquet("s3a://de-course/staging/stg_orders")
    .filter((F.col("order_date") >= start) & (F.col("order_date") < load_date))
)
payments = (
    spark.read.parquet("s3a://de-course/staging/stg_payments")
    .filter(
        ((F.col("paid_dt") >= start) & (F.col("paid_dt") < load_date))
        | F.col("paid_dt").isNull()
    )
)
success = (F.col("payment_status") == "success") & (~F.col("is_refund"))
refund = F.col("is_refund")
order_payments = payments.groupBy("order_id").agg(
    F.count(F.when(success, 1)).alias("successful_payments_count"),
    F.coalesce(F.sum(F.when(success, F.col("amount"))), F.lit(0)).alias("paid_amount"),
    F.coalesce(-F.sum(F.when(refund, F.col("amount"))), F.lit(0)).alias("refunded_amount"),
    F.min(F.when(success, F.col("paid_at"))).alias("first_paid_at"),
    F.count(F.when(F.col("payment_status") == "success", 1)).alias("success_operations_count"),
    F.count(F.when(F.col("payment_status") == "failed", 1)).alias("failed_operations_count"),
    F.count(F.when(F.col("payment_status") == "pending", 1)).alias("pending_operations_count"))
joined = orders.join(order_payments, on="order_id", how="left")
def zero(column_name):
    return F.coalesce(F.col(column_name), F.lit(0))
result = joined.select(
    "order_id", "customer_id", "ordered_at", "order_date",
    "order_status", "order_amount", "is_test_order",
    zero("successful_payments_count").alias("successful_payments_count"),
    zero("paid_amount").alias("paid_amount"),
    zero("refunded_amount").alias("refunded_amount"),
    (zero("paid_amount") - zero("refunded_amount")).alias("net_paid_amount"),
    "first_paid_at",
    ((F.col("order_status") == "paid") & (zero("successful_payments_count") > 0)).alias("is_confirmed"),
    zero("success_operations_count").alias("success_operations_count"),
    zero("failed_operations_count").alias("failed_operations_count"),
    zero("pending_operations_count").alias("pending_operations_count"))
(
    result.write.mode("overwrite")
    .partitionBy("order_date")
    .parquet("s3a://de-course/intermediate/int_orders_with_payments")
)
spark.stop()
