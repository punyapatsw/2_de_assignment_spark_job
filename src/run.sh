#!/bin/bash

JOB_NAME=$2
DATA_DATE=$3

source ~/Documents/Code/2_de_assignment_spark_job/src/bin/activate
python ~/Documents/Code/2_de_assignment_spark_job/src/src/__main__.py $JOB_NAME $DATA_DATE