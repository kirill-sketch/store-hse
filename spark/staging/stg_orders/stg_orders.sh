
spark-submit \
  --master spark://spark-master:7077 \
  --name load-stg_orders \
  --deploy-mode client \
  --conf spark.executor.instances=1 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=1g \
  --conf spark.driver.memory=1g \
  --conf spark.dynamicAllocation.enabled=false \
  stg_orders.py "$@"
