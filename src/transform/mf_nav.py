# from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# spark = SparkSession.builder.master('local')\
#     .appName('Nav_txrn_report')\
#     .config("hive.metastore.uris", "thrift://localhost:9083")\
#     .config("spark.hadoop.hive.exec.dynamic.partition", "true")\
#     .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")\
#     .enableHiveSupport().getOrCreate()

# source_file = '../input/mf_nav_202101_th.csv'
# df = spark.read.option("header",True).option("multiline",True).csv(source_file)
# for col_name in df.columns:
#     df = df.withColumn(col_name, regexp_replace(col(col_name), "[\\r\\n]", " "))


# df.show()
# df = df.withColumn("APPR_DATE_DD",split(col("APPR_DATE"),"/").getItem(0))\
#     .withColumn("APPR_DATE_MM",split(col("APPR_DATE"),"/").getItem(1))\
#     .withColumn("APPR_DATE_YYYY",split(col("APPR_DATE"),"/").getItem(2))\
#     .withColumn("REGIS_DATE_DD",split(col("REGIS_DATE"),"/").getItem(0))\
#     .withColumn("REGIS_DATE_MM",split(col("REGIS_DATE"),"/").getItem(1))\
#     .withColumn("REGIS_DATE_YYYY",split(col("REGIS_DATE"),"/").getItem(2))\
#     .drop("name")
# df = df.withColumn("APPR_DATE_YYYY",col("APPR_DATE_YYYY").cast("int")-543)\
#     .withColumn("REGIS_DATE_YYYY",col("REGIS_DATE_YYYY").cast("int")-543)
# df = df.withColumn("APPR_DATE",concat(col("APPR_DATE_YYYY"),lit('-'),col("APPR_DATE_MM"),lit('-'),col("APPR_DATE_DD")))\
#     .withColumn("REGIS_DATE",concat(col("REGIS_DATE_YYYY"),lit('-'),col("REGIS_DATE_MM"),lit('-'),col("REGIS_DATE_DD")))
# df = df.withColumn("APPR_DATE",to_date(col("APPR_DATE")))\
#     .withColumn("REGIS_DATE",to_date(col("REGIS_DATE")))
# df = df.drop("APPR_DATE_YYYY")\
#     .drop("APPR_DATE_MM")\
#     .drop("APPR_DATE_DD")\
#     .drop("REGIS_DATE_YYYY")\
#     .drop("REGIS_DATE_MM")\
#     .drop("REGIS_DATE_DD")
# df = df.withColumn('nav_rmf_pvd',regexp_replace('nav_rmf_pvd', ',', '').cast('decimal(18,2)'))\
#     .withColumn('nav',regexp_replace('nav', ',', '').cast('decimal(18,2)'))\
#     .withColumn('invsest_own_fund_value',regexp_replace('invsest_own_fund_value', ',', '').cast('decimal(18,2)'))
# df.write.mode('Overwrite').format("hive").saveAsTable("mf.mf_nav")


def extract(spark:'spark.Session',input_file:str,data_date:str):
    source_file = input_file
    df = spark.read.option("header",True).option("multiline",True).csv(source_file)
    return df

def transform(df:'spark.DataFrame'):
    for col_name in df.columns:
        df = df.withColumn(col_name, regexp_replace(col(col_name), "[\\r\\n]", " "))

    df = df.withColumn("APPR_DATE_DD",split(col("APPR_DATE"),"/").getItem(0))\
        .withColumn("APPR_DATE_MM",split(col("APPR_DATE"),"/").getItem(1))\
        .withColumn("APPR_DATE_YYYY",split(col("APPR_DATE"),"/").getItem(2))\
        .withColumn("REGIS_DATE_DD",split(col("REGIS_DATE"),"/").getItem(0))\
        .withColumn("REGIS_DATE_MM",split(col("REGIS_DATE"),"/").getItem(1))\
        .withColumn("REGIS_DATE_YYYY",split(col("REGIS_DATE"),"/").getItem(2))\
        .drop("name")
    df = df.withColumn("APPR_DATE_YYYY",col("APPR_DATE_YYYY").cast("int")-543)\
        .withColumn("REGIS_DATE_YYYY",col("REGIS_DATE_YYYY").cast("int")-543)
    df = df.withColumn("APPR_DATE",concat(col("APPR_DATE_YYYY"),lit('-'),col("APPR_DATE_MM"),lit('-'),col("APPR_DATE_DD")))\
        .withColumn("REGIS_DATE",concat(col("REGIS_DATE_YYYY"),lit('-'),col("REGIS_DATE_MM"),lit('-'),col("REGIS_DATE_DD")))
    df = df.withColumn("APPR_DATE",to_date(col("APPR_DATE")))\
        .withColumn("REGIS_DATE",to_date(col("REGIS_DATE")))
    df = df.drop("APPR_DATE_YYYY")\
        .drop("APPR_DATE_MM")\
        .drop("APPR_DATE_DD")\
        .drop("REGIS_DATE_YYYY")\
        .drop("REGIS_DATE_MM")\
        .drop("REGIS_DATE_DD")
    df = df.withColumn('nav_rmf_pvd',regexp_replace('nav_rmf_pvd', ',', '').cast('decimal(18,2)'))\
        .withColumn('nav',regexp_replace('nav', ',', '').cast('decimal(18,2)'))\
        .withColumn('invsest_own_fund_value',regexp_replace('invsest_own_fund_value', ',', '').cast('decimal(18,2)'))
    # df.show()
    return df
    # insert_overwrite(df,"txrn_date","mf.mf_txrn")

def load(df:'spark.DataFrame',ptn_col:str,tbl_name:str):
    df.write.mode('Overwrite').format("hive").saveAsTable(tbl_name)
