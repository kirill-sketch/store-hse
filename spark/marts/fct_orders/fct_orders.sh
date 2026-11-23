#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
spark-submit --master spark://spark-master:7077 --name load-fct_orders --deploy-mode client --conf spark.executor.instances=1 --conf spark.executor.cores=1 --conf spark.executor.memory=1g --conf spark.driver.memory=1g --conf spark.dynamicAllocation.enabled=false fct_orders.py "$@"
