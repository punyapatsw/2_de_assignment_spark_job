import sys
from pyspark.sql import SparkSession
import configparser

def spark_conn(appName):
    spark = SparkSession.builder.master('local')\
    .appName(appName)\
    .config("hive.metastore.uris", "thrift://localhost:9083")\
    .config("spark.hadoop.hive.exec.dynamic.partition", "true")\
    .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")\
    .enableHiveSupport().getOrCreate()
    return spark

def main(argv):
    if argv[0]=='mf_txrn':
        import transform.mf_txrn as etl
    elif argv[0]=='mf_fee':
        import transform.mf_fee as etl
    elif argv[0]=='mf_nav':
        import transform.mf_nav as etl
    elif argv[0]=='cus_txrn_with_units':
        import transform.cus_txrn_with_units as etl
    else :
        raise ImportError(
            "Sorry: no implementation for your file"
        )
    config = configparser.ConfigParser()
    config.read('src/config/file.conf')
    file_date=argv[1].replace("-","")[0:6]
    # tbl_name='mf.'+argv[0]
    source_file=config[argv[0]]['file_path']+config[argv[0]]['file_name']
    # print(source_file.format(argv[1]))
    df = etl.extract(spark_conn(argv[0]),source_file.format(file_date),argv[1])
    df = etl.transform(df)
    etl.load(df,"data_date",config[argv[0]]['tbl_name'])

if __name__=='__main__':
    main(sys.argv[1:])
