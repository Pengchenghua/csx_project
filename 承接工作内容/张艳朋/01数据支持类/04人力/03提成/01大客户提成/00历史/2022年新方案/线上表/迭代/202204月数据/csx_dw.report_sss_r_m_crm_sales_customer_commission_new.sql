
--set last_1day=regexp_replace(date_sub(current_date,1),'-','');
set last_1day='20220430';

--客户昨天的当月提成
insert overwrite table csx_tmp.report_sss_r_m_crm_sales_customer_commission_new partition(smonth)  
select 
	concat_ws('&',customer_no,cast(sales_id as string),smonth) as biz_id,
	customer_no,
	customer_name,
	coalesce(sales_id,'') as sales_id,
	coalesce(work_no,'') as work_no,
	coalesce(sales_name,'') as sales_name,
	sales_value,
	rp_sales_value as ripei_sales_value,
	bbc_sales_value as bbc_sales_value,
	rp_bbc_sales_value as ripei_bbc_sales_value,
	fl_sales_value as fuli_sales_value,
	--销售额提成
	tc_rp_sales_value_sales+tc_fl_sales_value_sales
	+tc_bbc_sales_value_sales as sales_value_commion,
	--日配销售额提成
	tc_rp_sales_value_sales as ripei_sales_value_commion,
	--bbc销售额提成
	tc_bbc_sales_value_sales as bbc_sales_value_commion,
	--日配&bbc销售额提成
	tc_rp_sales_value_sales+tc_bbc_sales_value_sales as ripei_bbc_sales_value_commion,	
	--福利销售额提成
	tc_fl_sales_value_sales as fuli_sales_value_commion,	
	profit as profit,
	rp_profit as ripei_profit,
	bbc_profit as bbc_profit,
	rp_bbc_profit as ripei_bbc_profit,
	fl_profit as fuli_profit,
	--定价毛利额提成
	tc_rp_profit_sales+tc_fl_profit_sales+
	tc_bbc_profit_sales as profit_commion,
	--日配定价毛利额提成
	tc_rp_profit_sales as ripei_profit_commion,
	--bbc定价毛利额提成
	tc_bbc_profit_sales as bbc_profit_commion,
	--日配bbc定价毛利额提成
	tc_rp_profit_sales+tc_bbc_profit_sales as ripei_bbc_profit_commion,
	--福利定价毛利额提成
	tc_fl_profit_sales as fuli_profit_commion,
	sales_prorate as prorate,
	sales_rp_prorate as ripei_prorate,
	sales_bbc_prorate as bbc_prorate,
	sales_rp_bbc_prorate as ripei_bbc_prorate,
	sales_fl_prorate as fuli_prorate,	
	tc_sales as commion_total,
	tc_rp_sales_value_sales+tc_rp_profit_sales as ripei_commion_total,
	tc_bbc_sales_value_sales+tc_bbc_profit_sales as bbc_commion_total,
	tc_rp_sales_value_sales+tc_rp_profit_sales+
	tc_bbc_sales_value_sales+tc_bbc_profit_sales as commion_ripei_bbc_total,	
	tc_fl_sales_value_sales+tc_fl_profit_sales as commion_fuli_total,
	sales_over_rate as over_rate,
	-1*refund_sales_value as refund_sales_value, --本月退货金额
	-1*refund_rp_sales_value as ripei_refund_sales_value,
	-1*refund_bbc_sales_value as bbc_refund_sales_value,
	-1*refund_rp_bbc_sales_value as ripei_bbc_refund_sales_value,--本月日配&BBC退货金额
	-1*refund_fl_sales_value as fuli_refund_sales_value,--本月福利退货金额
	${hiveconf:last_1day} as sdt,
	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as updated_time,
	smonth
from  
	csx_tmp.tc_new_cust_salary_info_202204
;



