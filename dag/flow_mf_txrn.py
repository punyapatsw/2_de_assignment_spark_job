# airflow related
from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
# from etl_flow import etl_flow

# other packages
from datetime import datetime
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'schedule_interval': '@daily',
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

# def mf_txrn(**context):
#     etl_flow.etl_flow('mf_txrn', context['execution_date'])

dag = DAG(
  dag_id='flow_mf_txrn', 
  description='Load file mf_txrn',
  default_args=default_args)

# task1 = PythonOperator(
#   task_id='load_mf_txrn', 
#   python_callable=mf_txrn, 
#   provide_context=True,
#   dag=dag)

task1 = BashOperator(
  task_id='load_mf_txrn', 
  bash_command="/home/punyapat/Documents/Code/2_de_assignment_spark_job/src/run.sh mf_txrn ''",
  dag=dag)

watchfile_mf_txrn = FileSensor(
                  task_id='wait_file_mf_txrn',
                  poke_interval=60,
                  filepath='/home/punyapat/Documents/Code/2_de_assignment_spark_job/input/mock_fund_buying_transaction_data.csv'
                  )

# setting dependencies
watchfile_mf_txrn >> task1