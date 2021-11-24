from pyspark.sql.functions import *

def extract(spark,input_file:str,data_date:str):
    """read csv file to spark dataframe

    Args:
        spark (SparkSession): spark session
        input_file (str): path and souce file name
        data_date (str): logical execution date format YYYY-MM-DD

    Returns:
        sparkDataFrame: source data in spark dataframe
    """
    source_file = input_file
    df = spark.read.option("header",True).option("multiline",True).csv(source_file)
    return df

def transform(df):
    """transform data from csv into appropriate format

    Args:
        df (sparkDataFrame): source data in spark dataframe

    Returns:
        sparkDataFrame: transformed dataframe for hive table
    """

    # remove line breaks
    for col_name in df.columns:
        df = df.withColumn(col_name, regexp_replace(col(col_name), "[\\r\\n]", " "))

    # change data from BE to CE
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

    # cast to hive table datatype
    df = df.withColumn('nav_rmf_pvd',regexp_replace('nav_rmf_pvd', ',', '').cast('decimal(18,2)'))\
        .withColumn('nav',regexp_replace('nav', ',', '').cast('decimal(18,2)'))\
        .withColumn('invsest_own_fund_value',regexp_replace('invsest_own_fund_value', ',', '').cast('decimal(18,2)'))
    return df

def load(df,ptn_col:str,tbl_name:str):
    """load dataframe to hive table

    Args:
        df (sparkDataFrame): tranformed dataframe
        ptn_col (str): partition field name
        tbl_name (str): hive table names
    """
    df.write.mode('Overwrite').format("hive").saveAsTable(tbl_name)
