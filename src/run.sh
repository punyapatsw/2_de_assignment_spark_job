#!/bin/bash

export JAVA_HOME=/opt/airflow/scripts/jdk8u312-b07

JOB_NAME=$1
DATA_DATE=$2

# python job accept 2 parameter $JOB_NAME and $DATA_DATE
# ex. python __main__.py mf_nav 2021-01-01
python /opt/airflow/scripts/2_de_assignment_spark_job/src/__main__.py $JOB_NAME $DATA_DATE
