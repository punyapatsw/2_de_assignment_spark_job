from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local')\
    .appName('Nav_txrn_report')\
    .config("hive.metastore.uris", "thrift://localhost:9083")\
    .config("spark.hadoop.hive.exec.dynamic.partition", "true")\
    .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")\
    .enableHiveSupport().getOrCreate()

source_file = '../input/mock_fund_buying_transaction_data.csv'
df = spark.read.option("header",True).option("multiline",true).csv(source_file)
for col_name in df.columns:
    df = df.withColumn(col_name, regexp_replace(col(col_name), "[\\r\\n]", " "))
df = df.withColumn("txrn_date",to_date(col("date"), "yyyyMMdd")).drop('date')
# df = df[['cus_id','fund_name','order_amount','txrn_date']]
df.write.mode('Overwrite').partitionBy("txrn_date").format("hive").saveAsTable("mf.mf_txrn")