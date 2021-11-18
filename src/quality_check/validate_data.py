from pyspark.sql.functions import *

def compute_stats(spark:sparkSession,tbl_name:str,data_date:str):
    """compute hive table statistics

    Args:
        spark (SparkSession): spark session
        tbl_name (str): hive table name
        data_date (str): logical execution date format YYYY-MM-DD
    """
    if data_date is not None:
        sql="ANALYZE TABLE {} PARTITION(data_date='{}') COMPUTE STATISTICS".format(tbl_name,data_date)
    else :
        sql="ANALYZE TABLE {} COMPUTE STATISTICS".format(tbl_name)
    spark.sql(sql)
    
def validate_data(spark:sparkSession,df:sparkDataFrame,tbl_name:str,data_date:str,col:str):
    """validate data insert completeness by compare result dataframe with data in hive table

    Args:
        spark (SparkSession): spark session
        tbl_name (str): hive table name
        data_date (str): logical execution date format YYYY-MM-DD
        col (str): partitioned column
    """
    
    
    count_df = df.count()
    sum_df=df.select(sum(col)).collect()[0][0]
    
    if data_date is not None:
        sql="""
            select count(*),sum({}) from {} where data_date='{}'
            """.format(col,tbl_name,data_date)
    else :
        sql="""
            select count(*),sum({}) from {}
            """.format(col,tbl_name)
    hive_df= spark.sql(sql)
    count_hive=hive_df.collect()[0][0]
    sum_hive=hive_df.collect()[0][1]
    msg="""
        Validate data
        Count dataframe  : {}
        Count hive table : {}
        Sum dataframe    : {}
        Sum hive table   : {}
        """.format(count_df,sum_df,count_hive,sum_hive)
    print(msg)
    if count_df!=count_hive or  sum_df!=sum_hive:
        msg="""
            Validate data failed!
            """
        raise Exception(msg)