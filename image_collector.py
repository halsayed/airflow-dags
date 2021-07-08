import airflow.utils.dates
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator


source_name = 'camera1'
source_path = 'camera'
file_pattern = '*.jpeg'
destentation_path = 'output'

dag = DAG(
    dag_id=f'{source_name}_collector_v3',
    description="Monitor NFS share for newly saved images ...",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@hourly",
    default_args={"depends_on_past": True},
)

start_task = DummyOperator(task_id='start')
stop_task = DummyOperator(task_id='stop')

wait_for_new_files = FileSensor(
    task_id=f'wait_for_new_file', 
    filepath='/opt/share/{source_path}/{file_pattern}',
    poke_interval=30,
    dag=dag
)

copy_new_files = BashOperator(
    task_id='copy_new_files',
    bash_command=(
        f'mkdir -p /opt/share/{destentation_path}/{source_name}/{{ds}} &&'
        f'cp -rf /opt/share/{source_path}/{file_pattern} /opt/share/{destentation_path}/{source_name}/{{ds}} &&'
        f'rm /opt/share/{source_path}/{file_pattern}'
    )
)

start_task >> wait_for_new_files >> copy_new_files >> stop_task
