import os
from pyspark.sql import SparkSession


spark = (SparkSession.builder
    .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT"))
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER"))
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate())
query = "SELECT order_id, product_id, quantity, unit_price, discount_amount FROM public.order_items"
df = (spark.read.format("jdbc")
    .option("query", query)
    .option("url", "jdbc:postgresql://postgres:5432/" + os.environ["POSTGRES_DB"])
    .option("user", os.environ["POSTGRES_USER"])
    .option("password", os.environ["POSTGRES_PASSWORD"])
    .option("driver", "org.postgresql.Driver")
    .load())
df.write.mode("overwrite").parquet("s3a://de-course/raw/order_items")
spark.stop()
