from pyspark.sql import SparkSession
import configparser
import quality_check.validate_data as validate_data

def spark_conn(appName):
    spark = SparkSession.builder.master('local')\
    .appName(appName)\
    .config("hive.metastore.uris", "thrift://localhost:9083")\
    .config("spark.hadoop.hive.exec.dynamic.partition", "true")\
    .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")\
    .enableHiveSupport().getOrCreate()
    return spark

def etl_flow(job_name: str,data_date:str):
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
    config = configparser.ConfigParser()
    config.read('src/config/file.ini')
    file_date=data_date.replace("-","")[0:6]
    # tbl_name='mf.'+job_name
    source_file=config[job_name]['file_path']+config[job_name]['file_name']
    # print(source_file.format(argv[1]))
    spark=spark_conn(job_name)
    df = etl.extract(spark,source_file.format(file_date),data_date)
    df = etl.transform(df)
    etl.load(df,"data_date",config[job_name]['tbl_name'])
    if config[job_name]['load_mth'] =='replace':
        data_date=None
    validate_data.validate_data(spark,df,config[job_name]['tbl_name'],data_date,config[job_name]['sum_col'])
    validate_data.compute_stats(spark,config[job_name]['tbl_name'],data_date)