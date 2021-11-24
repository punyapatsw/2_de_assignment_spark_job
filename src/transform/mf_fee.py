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

    # cast datatype to hive table datatype
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
    return df
    

def load(df:'sparkDataFrame',ptn_col:str,tbl_name:str):
    """load dataframe to hive table

    Args:
        df (spark.DataFrame): tranformed dataframe
        ptn_col (str): partition field name
        tbl_name (str): hive table names
    """
    df.write.mode('Overwrite').format("hive").saveAsTable(tbl_name)