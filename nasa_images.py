import json
import nasapy

import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

dag = DAG(
    dag_id="download_nasa_image_v8",
    description="Download Nasa's daily image ...",
    start_date=airflow.utils.dates.days_ago(7),
    schedule_interval="@daily",
)

def _get_image_metadata(image_date, **context):
    nasa_key = Variable.get('nasa_key')
    nasa = nasapy.Nasa(key=nasa_key)
    image_json = nasa.picture_of_the_day(date=image_date)
    with open(f'/opt/share/nasa/{image_date}.metadata', 'w') as file:
        file.write(json.dumps(image_json))
    context['task_instance'].xcom_push(key='url', value=image_json.get('url'))


get_image_metadata = PythonOperator(
    task_id = 'get_image_metadata',
    python_callable=_get_image_metadata,
    op_kwargs={'image_date': '{{ds}}'},
    dag=dag
)

download_image = BashOperator(
    task_id="download_image",
    bash_command="curl {{task_instance.xcom_pull(task_ids='get_image_metadata', key='url')}} --output /opt/share/nasa/{{ds}}.jpeg",
    dag=dag,
)

notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /opt/share/nasa/*.jpeg | wc -l) images."',
    dag=dag,
)

get_image_metadata >> download_image >> notify