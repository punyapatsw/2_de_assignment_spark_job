from pyspark.sql.functions import *

# spark = SparkSession.builder.master('local')\
#     .appName('Nav_txrn_report')\
#     .config("hive.metastore.uris", "thrift://localhost:9083")\
#     .config("spark.hadoop.hive.exec.dynamic.partition", "true")\
#     .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")\
#     .enableHiveSupport().getOrCreate()

# source_file = '../input/mock_fund_buying_transaction_data.csv'
# df = spark.read.option("header",True).option("multiline",true).csv(source_file)
# for col_name in df.columns:
#     df = df.withColumn(col_name, regexp_replace(col(col_name), "[\\r\\n]", " "))
# df = df.withColumn("txrn_date",to_date(col("date"), "yyyyMMdd")).drop('date')
# # df = df[['cus_id','fund_name','order_amount','txrn_date']]
# df.write.mode('Overwrite').partitionBy("txrn_date").format("hive").saveAsTable("mf.mf_txrn")

def extract(spark:'spark.Session',input_file:str,data_date:str):
    source_file = input_file
    df = spark.read.option("header",True).option("multiline",True).csv(source_file)
    return df

def transform(df:'spark.DataFrame'):
    for col_name in df.columns:
        df = df.withColumn(col_name, regexp_replace(col(col_name), "[\\r\\n]", " "))
    df = df.withColumn("data_date",to_date(col("date"), "yyyyMMdd")).drop('date')

    # df.show()
    return df
    # insert_overwrite(df,"txrn_date","mf.mf_txrn")
    

def load(df:'spark.DataFrame',ptn_col:str,tbl_name:str):
    df.write.mode('Overwrite').partitionBy(ptn_col).format("hive").saveAsTable(tbl_name)
