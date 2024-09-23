import os, sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv(dotenv_path='/opt/app/.env')
spark_master_hostname = os.getenv('SPARK_MASTER_HOST_NAME')
spark_port = os.getenv('SPARK_MASTER_PORT')

spark = (
    SparkSession
    .builder
    .appName('Welcome')
    .config('spark.master', f'spark://{spark_master_hostname}:{spark_port}')
    .getOrCreate()
)
spark.sparkContext.setLogLevel('WARN')

print(f'Hi, {sys.argv[1]}')
spark.stop()