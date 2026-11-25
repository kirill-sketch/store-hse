import argparse
import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BUCKET = 'de-course'
STG_PAYMENTS_PATH = f"s3a://{BUCKET}/staging/stg_payments"


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




df_payments = (
    spark.read.parquet(f"s3a://{BUCKET}/raw/payments")
    .filter(
        ((F.col("paid_dt") >= start_date) & (F.col("paid_dt") < load_date))
        | F.col("paid_dt").isNull()
    )
)

df_stg_payments = (
    df_payments.select(
        F.col("payment_id"),
        F.col("order_id"),
        F.col("paid_at"),
        F.col("payment_status"),
        F.col("amount"),
        ((F.col("payment_status") == "success") & (F.col("amount") < 0)).alias("is_refund"),
    )
)


df_stg_payments = df_stg_payments.withColumn("paid_dt", F.to_date("paid_at"))


(
    df_stg_payments.write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("paid_dt")
    .save(STG_PAYMENTS_PATH)
)

print(f"Сохранено: {STG_PAYMENTS_PATH}")

spark.stop()
