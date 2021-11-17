from pyspark.sql.functions import *

def extract(spark:SparkSession,input_file:str,data_date:str)->sparkDataFrame:
    """read csv file to spark dataframe

    Args:
        spark (SparkSession): spark session
        input_file (str): path and souce file name
        data_date (str): logical execution date format YYYY-MM-DD

    Returns:
        sparkDataFrame: source data in spark dataframe
    """
    nav_sql="""
        select regis_id_year1,
        PROJ_ABBR_NAME,
        nav
        from mf.mf_nav;
        """
    nav_df = spark.sql(nav_sql)


    fee_sql="""
        select regis_id,
        fee_tt
        from mf.mf_fee;
        """
    fee_df = spark.sql(fee_sql)\
        .distinct()
    
    txrn_sql="""
        select cus_id,
        fund_name,
        order_amount,
        data_date
        from mf.mf_txrn  
        where data_date='{}'
        """.format(data_date)
    txrn_df = spark.sql(txrn_sql)

    df={'nav_df':nav_df,'fee_df':fee_df,'txrn_df':txrn_df}
    return df

def transform(df:list[sparkDataFrame])->sparkDataFrame:
    """transform data from csv into appropriate format

    Args:
        df (list[sparkDataFrame]): list of dataframe from hive table

    Returns:
        sparkDataFrame: customer order report with unit
    """

    df['txrn_df'] = df['txrn_df'].join(df['nav_df'], df['txrn_df'].fund_name == df['nav_df'].PROJ_ABBR_NAME,'left')\
        .drop("PROJ_ABBR_NAME")
    # some mutual fund has multiple fee. select max fee to avoid duplication 
    df['fee_df']=df['fee_df'].groupBy("regis_id").agg(max("fee_tt").alias("fee_tt"))
    df['txrn_df'] = df['txrn_df'].join(df['fee_df'], df['txrn_df'].regis_id_year1 == df['fee_df'].regis_id,'left')\
        .drop("regis_id")\
        .drop("regis_id_year1")

    # calculate brought unit
    df = df['txrn_df'].withColumn("bought_unit",when(col("nav").isNull(),-99.0) \
        .otherwise(col("order_amount")/(col("nav")+col("nav")*col("fee_tt"))))
    return df

def load(df:sparkDataFrame,ptn_col:str,tbl_name:str):
    """load dataframe to hive table

    Args:
        df (sparkDataFrame): tranformed dataframe
        ptn_col (str): partition field name
        tbl_name (str): hive table names
    """
    df.write.mode('Overwrite').partitionBy(ptn_col).format("hive").saveAsTable(tbl_name)