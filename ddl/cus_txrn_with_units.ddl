Create external table mf.cus_txrn_with_units(
cus_id string
,fund_name string
,order_amount decimal(18,0)
,nav decimal(18,2)
,fee_tt decimal(8,5)
,brought_unit double)
partitioned by(
data_date date
)
row format delimited fields terminated by '\u0001'
stored as parquet
location '/shared/datalake/mf/cus_txrn_with_units';
