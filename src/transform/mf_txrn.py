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

    # cast to hive table datatype
    df = df.withColumn("data_date",to_date(col("date"), "yyyyMMdd")).drop('date')
    return df

def load(df,ptn_col:str,tbl_name:str):
    """load dataframe to hive table

    Args:
        df (sparkDataFrame): tranformed dataframe
        ptn_col (str): partition field name
        tbl_name (str): hive table names
    """
    df.write.mode('Overwrite').partitionBy(ptn_col).format("hive").saveAsTable(tbl_name)
