from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os, sys

load_dotenv(dotenv_path='/opt/app/.env')
spark_master_hostname = os.getenv('SPARK_MASTER_HOST_NAME')
spark_port = os.getenv('SPARK_MASTER_PORT')

spark = (
    SparkSession
    .builder
    .appName('Welcome')
    .config('spark.master', f'spark://{spark_master_hostname}:{spark_port}')
    .config('spark.eventLog.enabled', 'true')
    .config('spark.eventLog.dir', '/tmp/spark-events')
    .config('spark.history.fs.logDirectory', '/tmp/spark-events')
    .getOrCreate()
)
spark.sparkContext.setLogLevel('WARN')

print(f'Hi, {sys.argv[1]}')
spark.stop()