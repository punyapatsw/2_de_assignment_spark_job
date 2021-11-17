from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import configparser
import quality_check.validate_data as validate_data

# Setup Environment
CONFIG_FILE = '/opt/airflow/scripts/2_de_assignment_spark_job/src/config/file.ini'
HMS_URI = "thrift://hive-metastore:9083"

def spark_conn(appName:str)->SparkSession:
    """setup spark connection

    Args:
        appName (str): spark app name

    Returns:
        SparkSession: spark session
    """
    spark = SparkSession.builder.master('local')\
    .appName(appName)\
    .config("hive.metastore.uris",HMS_URI)\
    .config("spark.hadoop.hive.exec.dynamic.partition", "true")\
    .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")\
    .enableHiveSupport().getOrCreate()
    return spark

def etl_flow(job_name: str,data_date:str):
    """main control flow for etl job

    Args:
        job_name (str): job mf for inject module and read config source file
        data_date (str): logical execution date format YYYY-MM-DD

    Raises:
        ImportError: invalid job name specified
    """

    # inject module
    if job_name=='mf_txrn':
        import transform.mf_txrn as etl
    elif job_name=='mf_fee':
        import transform.mf_fee as etl
    elif job_name=='mf_nav':
        import transform.mf_nav as etl
    elif job_name=='cus_txrn_with_units':
        import transform.cus_txrn_with_units as etl
    else :
        raise ImportError(
            "Sorry: no implementation for your file"
        )
    
    # read source file configuration
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    file_date=data_date.replace("-","")[0:6]
    source_file=config[job_name]['file_path']+config[job_name]['file_name']
    
    # start etl flow
    spark=spark_conn(job_name)
    df = etl.extract(spark,source_file.format(file_date),data_date)
    df = etl.transform(df)
    etl.load(df,"data_date",config[job_name]['tbl_name'])

    # check is partitioned
    if config[job_name]['load_mth'] =='replace':
        data_date=None

    # post etl process
    validate_data.validate_data(spark,df,config[job_name]['tbl_name'],data_date,config[job_name]['sum_col'])
    validate_data.compute_stats(spark,config[job_name]['tbl_name'],data_date)