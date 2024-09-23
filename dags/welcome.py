from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow import DAG

spark_scripts_path = '{{var.json.sparkscripts.path}}'
sensor_file_path = '{{var.json.sensorfile.path}}'

default_args = {
    'owner': 'airflow',
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'welcome',
    default_args=default_args,
    description='this is my first dag',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['example'],
)

def file_check(task_id, filepath):
    return FileSensor(
        task_id=task_id,
        filepath=filepath,
        poke_interval=10,
        timeout=300,
        mode='poke',
        dag=dag
    )

def spark_job(task_id, application, **kwargs):
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id='spark_mumix',
        application=application,
        application_args=[kwargs['extra_args']],
        dag=dag
    )
    
items_list = [
    {
        'uid': '1', 'task_id': 'first', 'msg': 'this is message from first.'
    },
    {
        'uid': '2', 'task_id': 'second', 'msg': 'this is message from second.'
    }
]

start = DummyOperator(task_id='start')
wait = DummyOperator(task_id='wait')
end = DummyOperator(task_id='end')

precheck_file = file_check(
    task_id='precheck_file',
    filepath=sensor_file_path
)

start >> precheck_file >> wait

for item in items_list:
    submit_spark =spark_job(
        task_id=item['task_id'],
        application=f'{spark_scripts_path}/welcome.py',
        extra_args=item['msg']
    )
    wait >> submit_spark >> end
