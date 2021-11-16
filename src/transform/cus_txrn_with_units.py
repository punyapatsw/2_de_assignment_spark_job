# from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# spark = SparkSession.builder.master('local')\
#     .appName('Nav_txrn_report')\
#     .config("hive.metastore.uris", "thrift://localhost:9083")\
#     .config("spark.hadoop.hive.exec.dynamic.partition", "true")\
#     .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")\
#     .enableHiveSupport().getOrCreate()

# source_file = '../input/mock_fund_buying_transaction_data.csv'
# txrn_date='2021-01-01'
# sql="""
#     select cus_id,
#     fund_name,
#     order_amount,
#     nav,
#     fee_tt,
#     case when nav is NULL
#     then -99.0
#     else order_amount/(nav+(nav*fee_tt))
#     end as bought_unit,
#     txrn_date
#     from mf.mf_txrn txrn 
#     inner join mf.mf_nav nav on txrn.fund_name = nav.PROJ_ABBR_NAME
#     inner join mf.mf_fee fee on nav.regis_id_year1 = fee.regis_id
#     where txrn='{}'
#     """.format(txrn_date)
# df = spark.sql(sql)

# nav_sql="""
#     select regis_id_year1,
#     PROJ_ABBR_NAME,
#     nav
#     from mf.mf_nav;
#     """
# nav_df = spark.sql(nav_sql)
# nav_df.show()

# fee_sql="""
#     select regis_id,
#     fee_tt
#     from mf.mf_fee;
#     """
# fee_df = spark.sql(fee_sql)\
#     .distinct()

# fee_df.show()

# nav_df = nav_df.join(fee_df, nav_df.regis_id_year1 == fee_df.regis_id,'inner')\
#         .drop("regis_id")\
#         .drop("regis_id_year1")
# # fee_df.Boardcast.destroy()
# nav_df.show()



# txrn_sql="""
#     select cus_id,
#     fund_name,
#     order_amount,
#     txrn_date
#     from mf.mf_txrn  
#     where txrn_date='{}'
#     """.format(txrn_date)
# txrn_df = spark.sql(txrn_sql)

# txrn_df = txrn_df.join(nav_df, txrn_df.fund_name == nav_df.PROJ_ABBR_NAME,'inner')\
#         .drop("PROJ_ABBR_NAME")
# txrn_df = txrn_df.withColumn("bought_unit",when(col("nav").isNull(),-99.0) \
#     .otherwise(col("order_amount")/(col("nav")+col("nav")*col("fee_tt"))))

# df.write.mode('Overwrite').partitionBy("txrn_date").format("hive").saveAsTable("mf.cus_txrn_with_units")


def extract(spark:'spark.Session',input_file:str,data_date:str):
    nav_sql="""
        select regis_id_year1,
        PROJ_ABBR_NAME,
        nav
        from mf.mf_nav;
        """
    nav_df = spark.sql(nav_sql)
    # nav_df.show()

    fee_sql="""
        select regis_id,
        fee_tt
        from mf.mf_fee;
        """
    fee_df = spark.sql(fee_sql)\
        .distinct()

    # fee_df.show()

    
    txrn_sql="""
        select cus_id,
        fund_name,
        order_amount,
        data_date
        from mf.mf_txrn  
        where data_date='{}'
        """.format(data_date)
    txrn_df = spark.sql(txrn_sql)

    # fee_df.Boardcast.destroy()
    # nav_df.show()
    df={'nav_df':nav_df,'fee_df':fee_df,'txrn_df':txrn_df}
    return df

def transform(df:'spark.DataFrame'):
    df['nav_df'] = df['nav_df'].join(df['fee_df'], df['nav_df'].regis_id_year1 == df['fee_df'].regis_id,'inner')\
        .drop("regis_id")\
        .drop("regis_id_year1")
    df['txrn_df'] = df['txrn_df'].join(df['nav_df'], df['txrn_df'].fund_name == df['nav_df'].PROJ_ABBR_NAME,'inner')\
            .drop("PROJ_ABBR_NAME")
    df = df['txrn_df'].withColumn("bought_unit",when(col("nav").isNull(),-99.0) \
        .otherwise(col("order_amount")/(col("nav")+col("nav")*col("fee_tt"))))
    return df
    # insert_overwrite(df,"txrn_date","mf.mf_txrn")

def load(df:'spark.DataFrame',ptn_col:str,tbl_name:str):
    df.write.mode('Overwrite').partitionBy(ptn_col).format("hive").saveAsTable(tbl_name)