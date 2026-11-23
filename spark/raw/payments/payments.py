import argparse
import os
from datetime import datetime, timedelta

from pyspark.sql import functions as F
from pyspark.sql import SparkSession


PG = dict(
    host='postgres',
    port=5432,
    dbname=os.environ.get('POSTGRES_DB'),
    user=os.environ.get('POSTGRES_USER'),
    password=os.environ.get('POSTGRES_PASSWORD'),
)

BUCKET = 'de-course'
PAYMENTS_PATH = f"s3a://{BUCKET}/raw/payments"


def sink_has_data(spark: SparkSession, path: str) -> bool:
    try:
        return spark.read.parquet(path).limit(1).count() > 0
    except Exception:
        return False


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

parser = argparse.ArgumentParser()
parser.add_argument("--load-date", required=True, help="Дата запуска загрузки, ГГГГ-ММ-ДД")
args = parser.parse_args()

load_date = datetime.strptime(args.load_date, "%Y-%m-%d").date()
start_date = load_date - timedelta(days=3)

jdbc = dict(
    url=f"jdbc:postgresql://{PG['host']}:{PG['port']}/{PG['dbname']}",
    user=PG['user'],
    password=PG['password'],
    driver='org.postgresql.Driver',
)

if sink_has_data(spark, PAYMENTS_PATH):
    print(f"Приёмник не пуст — инкремент [{start_date} .. {load_date}]")
    where = f"""
        WHERE (paid_at >= '{start_date}'::timestamp
          AND paid_at <  '{load_date}'::timestamp)
           OR paid_at IS NULL
    """
else:
    print("Данных в приёмнике нет — полная загрузка таблицы")
    where = ""



query = f"""
    SELECT payment_id
         , order_id
         , paid_at
         , payment_status
         , amount
    FROM public.payments
    {where}
"""

df_payments = (
    spark.read.format("jdbc")
    .option("query", query)
    .options(**jdbc)
    .load()
)


df_payments = df_payments.withColumn("paid_dt", F.to_date("paid_at"))


(
    df_payments.write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("paid_dt")
    .save(PAYMENTS_PATH)
)

print(f"Сохранено: {PAYMENTS_PATH}")
