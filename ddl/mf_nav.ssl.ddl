Create external table mf.mf_nav(
regis_id_year1 string
,PROJ_THAI_NAME string
,PROJ_ABBR_NAME string
,COMP_THAI_NAME string
,PROJ_RETAIL_TYPE string
,POLICY_THAI_DESC string
,SPEC_GR_DESC string
,policyspec_desc string
,current_rmf_pvd_type string
,management_style_th string
,fund_compare string
,MUTUAL_INV_TYPE string
,INVEST_COUNTRY_FLAG string
,REDEMP_PERIOD string
,proj_term_th string
,SUPERVISOR_NAME string
,APPR_DATE date
,REGIS_DATE date
,nav_rmf_pvd decimal(18,2)
,nav decimal(18,2)
,invsest_own_fund_value decimal(18,2))
row format delimited fields terminated by '\u0001'
stored as parquet
location '/shared/datalake/mf/mf_nav';