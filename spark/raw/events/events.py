import argparse
import os
from datetime import datetime, timedelta

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException


PG = dict(
    host='postgres',
    port=5432,
    dbname=os.environ.get('POSTGRES_DB'),
    user=os.environ.get('POSTGRES_USER'),
    password=os.environ.get('POSTGRES_PASSWORD'),
)

BUCKET = 'de-course'
EVENTS_PATH = f"s3a://{BUCKET}/raw/events"


def sink_has_data(spark: SparkSession, path: str) -> bool:
    try:
        return spark.read.parquet(path).limit(1).count() > 0
    except AnalysisException as exc:
        if exc.getErrorClass() == "PATH_NOT_FOUND":
            return False
        raise


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

if sink_has_data(spark, EVENTS_PATH):
    print(f"Приёмник не пуст — инкремент [{start_date} .. {load_date}]")
    where = f"""
        WHERE event_time >= '{start_date}'::timestamp
          AND event_time <  '{load_date}'::timestamp
    """
else:
    print("данных в приёмнике нет — полная загрузка таблицы")
    where = ""


query = f"""
    SELECT event_id
         , customer_id
         , event_time
         , event_type
         , product_id
         , session_id
    FROM public.events
    {where}
"""

df_events = (
    spark.read.format("jdbc")
    .option("query", query)
    .options(**jdbc)
    .load()
)


# event_time не бывает NULL; партиция по дате позволяет обновлять только трёхдневное окно.
df_events = df_events.withColumn("event_dt", F.to_date("event_time"))


(
    df_events.write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("event_dt")
    .save(EVENTS_PATH)
)

print(f"Сохранено: {EVENTS_PATH}")

spark.stop()
