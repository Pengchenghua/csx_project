
----昨天
set last_1day=regexp_replace(date_sub(current_date,1),'-','');
set created_time = from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss');

insert overwrite table csx_tmp.report_sss_r_d_crm_service_commission_trend_new partition(sdt) 
select
	concat_ws('&',cast(service_id as string),${hiveconf:last_1day}) as biz_id,
	service_id,
	service_work_no,
	service_name,
	'20220228' as sales_date,
	--regexp_replace(date_sub(current_date,1),'-','') as sales_date,
	sum(sales_value_commion) as sales_value_commion,
	sum(ripei_sales_value_commion) as ripei_sales_value_commion,
	sum(bbc_sales_value_commion) as bbc_sales_value_commion,
	sum(ripei_bbc_sales_value_commion) as ripei_bbc_sales_value_commion,
	sum(fuli_sales_value_commion) as fuli_sales_value_commion,
	sum(profit_commion) as profit_commion,
	sum(ripei_profit_commion) as ripei_profit_commion,
	sum(bbc_profit_commion) as bbc_profit_commion,
	sum(ripei_bbc_profit_commion) as ripei_bbc_profit_commion,
	sum(fuli_profit_commion) as fuli_profit_commion,
	sum(commion_total) as commion_total,
	sum(ripei_commion_total) as ripei_commion_total,
	sum(bbc_commion_total) as bbc_commion_total,
	sum(commion_ripei_bbc_total) as commion_ripei_bbc_total,
	sum(commion_fuli_total) as commion_fuli_total,
	smonth,
	from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as updated_time,
	${hiveconf:last_1day} as sdt
from 
	csx_tmp.report_sss_r_m_crm_service_customer_commission_new
where 
	--smonth=substr(regexp_replace(date_sub(current_date,1),'-',''),1,6)
	smonth='202202'
	and service_id is not null
group by 
	service_id,service_work_no,service_name,smonth;