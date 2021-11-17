from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'schedule_interval': '@daily',
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

dag = DAG(
  dag_id='flow_mf_fee', 
  description='Load file mf_fee',
  default_args=default_args,
  catchup=False)

task1 = BashOperator(
  task_id='load_mf_fee', 
  bash_command="/opt/airflow/scripts/2_de_assignment_spark_job/src/run.sh mf_fee {{ ds }}",
  dag=dag)

watchfile_mf_fee = FileSensor(
                  task_id='wait_file_mf_fee',
                  poke_interval=60,
                  filepath="/opt/airflow/sources/mf_fee_{{ execution_date.strftime('%Y%m') }}_th.csv",
                  dag=dag
                  )

# setting dependencies
# wait for file trigger
watchfile_mf_fee >> task1