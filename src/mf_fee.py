from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local')\
    .appName('Nav_txrn_report')\
    .config("hive.metastore.uris", "thrift://localhost:9083")\
    .config("spark.hadoop.hive.exec.dynamic.partition", "true")\
    .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")\
    .enableHiveSupport().getOrCreate()

source_file = '../input/mf_fee_202101_th.csv'
df = spark.read.option("header",True).option("multiline",true).csv(source_file)
for col_name in df.columns:
    df = df.withColumn(col_name, regexp_replace(col(col_name), "[\\r\\n]", " "))
df = df.withColumn("fee_valid_from",to_date(col("fee_valid_from"), "d/M/yyyy"))
df = df.withColumn('fee_tt',regexp_replace('fee_tt', ',', '').cast('decimal(8,5)'))\
    .withColumn('fee_of',regexp_replace('fee_of', ',', '').cast('decimal(8,5)'))\
    .withColumn('fee_sp',regexp_replace('fee_sp', ',', '').cast('decimal(8,5)'))\
    .withColumn('fee_fr',regexp_replace('fee_fr', ',', '').cast('decimal(8,5)'))\
    .withColumn('fee_ia',regexp_replace('fee_ia', ',', '').cast('decimal(8,5)'))\
    .withColumn('fee_uw',regexp_replace('fee_uw', ',', '').cast('decimal(8,5)'))\
    .withColumn('fee_eo',regexp_replace('fee_eo', ',', '').cast('decimal(8,5)'))\
    .withColumn('fee_ff',regexp_replace('fee_ff', ',', '').cast('decimal(8,5)'))\
    .withColumn('fee_bf',regexp_replace('fee_bf', ',', '').cast('decimal(8,5)'))\
    .withColumn('fee_si',regexp_replace('fee_si', ',', '').cast('decimal(8,5)'))\
    .withColumn('fee_so',regexp_replace('fee_so', ',', '').cast('decimal(8,5)'))\
    .withColumn('fee_tf',regexp_replace('fee_tf', ',', '').cast('decimal(8,5)'))\
    .withColumn('unit_tf',regexp_replace('unit_tf', ',', '').cast('decimal(8,5)'))\
    .withColumn('fee_cf',regexp_replace('fee_cf', ',', '').cast('decimal(8,5)'))\
    .withColumn('fee_ot',regexp_replace('fee_ot', ',', '').cast('decimal(8,5)'))\
    .withColumn('fee_tt_actual',regexp_replace('fee_tt_actual', ',', '').cast('decimal(8,5)'))\
    .withColumn('fee_of_actual',regexp_replace('fee_of_actual', ',', '').cast('decimal(8,5)'))\
    .withColumn('fee_sp_actual',regexp_replace('fee_sp_actual', ',', '').cast('decimal(8,5)'))\
    .withColumn('fee_fr_actual',regexp_replace('fee_fr_actual', ',', '').cast('decimal(8,5)'))\
    .withColumn('fee_ia_actual',regexp_replace('fee_ia_actual', ',', '').cast('decimal(8,5)'))\
    .withColumn('fee_uw_actual',regexp_replace('fee_uw_actual', ',', '').cast('decimal(8,5)'))\
    .withColumn('fee_eo_actual',regexp_replace('fee_eo_actual', ',', '').cast('decimal(8,5)'))\
    .withColumn('fee_ff_actual',regexp_replace('fee_ff_actual', ',', '').cast('decimal(8,5)'))\
    .withColumn('fee_bf_actual',regexp_replace('fee_bf_actual', ',', '').cast('decimal(8,5)'))\
    .withColumn('fee_si_actual',regexp_replace('fee_si_actual', ',', '').cast('decimal(8,5)'))\
    .withColumn('fee_so_actual',regexp_replace('fee_so_actual', ',', '').cast('decimal(8,5)'))\
    .withColumn('fee_tf_actual',regexp_replace('fee_tf_actual', ',', '').cast('decimal(8,5)'))\
    .withColumn('unit_actual_tf',regexp_replace('unit_actual_tf', ',', '').cast('decimal(8,5)'))\
    .withColumn('fee_cf_actual',regexp_replace('fee_cf_actual', ',', '').cast('decimal(8,5)'))\
    .withColumn('fee_ot_actual',regexp_replace('fee_ot_actual', ',', '').cast('decimal(8,5)'))
df.write.mode('Overwrite').format("hive").saveAsTable("mf.mf_fee")
# df = df.withColumn("txrn_date",to_date(col("date"), "yyyyMMdd"))
# df = df[['cus_id','fund_name','order_amount','txrn_date']]
# df.write.mode('Overwrite').partitionBy("txrn_date").format("hive").saveAsTable("mf.mf_txrn")