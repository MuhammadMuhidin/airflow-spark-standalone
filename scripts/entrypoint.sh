#!/bin/bash
airflow db init
echo "AUTH_ROLE_PUBLIC = 'Admin'" >> webserver_config.py

export SPARK_FULL_HOST_NAME="spark://$SPARK_MASTER_HOST_NAME"
airflow connections add 'spark_mumix' \
    --conn-type 'spark' \
    --conn-host $SPARK_FULL_HOST_NAME \
    --conn-port $SPARK_MASTER_PORT

airflow variables import /scripts/airflow_variables.json

airflow webserver