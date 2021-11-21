Create external table mf.mf_txrn(
cus_id string
,fund_name string
,order_amount decimal(18,0))
partitioned by(
data_date date
)
row format delimited fields terminated by '\u0001'
stored as parquet
location '/shared/datalake/mf/mf_txrn';