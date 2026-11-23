import os

from pyspark.sql import SparkSession


PG = dict(
    host='postgres',
    port=5432,
    dbname=os.environ.get('POSTGRES_DB'),
    user=os.environ.get('POSTGRES_USER'),
    password=os.environ.get('POSTGRES_PASSWORD'),
)

BUCKET = 'de-course'
CUSTOMERS_PATH = f"s3a://{BUCKET}/raw/customers"

spark = (
    SparkSession.builder
    .config("spark.hadoop.fs.s3a.endpoint", os.environ.get('MINIO_ENDPOINT'))
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get('MINIO_ROOT_USER'))
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get('MINIO_ROOT_PASSWORD'))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

jdbc = dict(
    url=f"jdbc:postgresql://{PG['host']}:{PG['port']}/{PG['dbname']}",
    user=PG['user'],
    password=PG['password'],
    driver='org.postgresql.Driver',
)



query = """
    SELECT customer_id
         , registered_at
         , city
         , acquisition_channel
    FROM public.customers
"""

df_customers = (
    spark.read.format("jdbc")
    .option("query", query)
    .options(**jdbc)
    .load()
)


(
    df_customers.write
    .format("parquet")
    .mode("overwrite")
    .save(CUSTOMERS_PATH)
)

print(f"Сохранено: {CUSTOMERS_PATH}, строк: {df_customers.count()}")
