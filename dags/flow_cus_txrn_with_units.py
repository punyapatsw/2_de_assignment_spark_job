from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
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
  dag_id='flow_cus_txrn_with_units', 
  description='Create report to table cus_txrn_with_units',
  default_args=default_args,
  catchup=False)

task1 = BashOperator(
  task_id='cus_txrn_with_units', 
  bash_command="/opt/airflow/scripts/2_de_assignment_spark_job/src/run.sh cus_txrn_with_units {{ ds }}",
  dag=dag)

wait_mf_fee = ExternalTaskSensor(
    task_id="wait_mf_fee",
    external_dag_id='flow_mf_fee',
    external_task_id='load_mf_fee',
    allowed_states=['success'],
    failed_states=['failed', 'skipped'],
    dag=dag
)

wait_mf_nav = ExternalTaskSensor(
    task_id="wait_mf_nav",
    external_dag_id='flow_mf_nav',
    external_task_id='load_mf_nav"',
    allowed_states=['success'],
    failed_states=['failed', 'skipped'],
    dag=dag
)

wait_mf_txrn = ExternalTaskSensor(
    task_id="wait_mf_txrn",
    external_dag_id='flow_mf_txrn',
    external_task_id='load_mf_txrn',
    allowed_states=['success'],
    failed_states=['failed', 'skipped'],
    dag=dag
)

# setting dependencies
# wait for all souce table load success
wait_mf_fee >> task1
wait_mf_nav >> task1
wait_mf_txrn >> task1