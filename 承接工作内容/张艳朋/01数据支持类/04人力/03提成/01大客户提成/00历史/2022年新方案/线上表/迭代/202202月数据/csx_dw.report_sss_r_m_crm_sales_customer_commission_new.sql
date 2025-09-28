
set last_1day=regexp_replace(date_sub(current_date,1),'-','');

--客户昨天的当月提成
insert overwrite table csx_tmp.report_sss_r_m_crm_sales_customer_commission_new partition(smonth)  
select 
	concat_ws('&',customer_no,cast(sales_id as string),smonth) as biz_id,
	customer_no,
	customer_name,
	coalesce(sales_id,'') as sales_id,
	coalesce(work_no,'') as work_no,
	coalesce(sales_name,'') as sales_name,
	customer_sales_value as sales_value,
	customer_ripei_sales_value as ripei_sales_value,
	customer_bbc_sales_value as bbc_sales_value,
	customer_ripei_bbc_sales_value as ripei_bbc_sales_value,
	customer_fuli_sales_value as fuli_sales_value,
	--销售额提成
	tc_ripei_sales_value_salesperson+tc_fuli_sales_value_salesperson
	+tc_bbc_sales_value_salesperson as sales_value_commion,
	--日配销售额提成
	tc_ripei_sales_value_salesperson as ripei_sales_value_commion,
	--bbc销售额提成
	tc_bbc_sales_value_salesperson as bbc_sales_value_commion,
	--日配&bbc销售额提成
	tc_ripei_sales_value_salesperson+tc_bbc_sales_value_salesperson as ripei_bbc_sales_value_commion,	
	--福利销售额提成
	tc_fuli_sales_value_salesperson as fuli_sales_value_commion,	
	customer_profit as profit,
	customer_ripei_profit as ripei_profit,
	customer_bbc_profit as bbc_profit,
	customer_ripei_bbc_profit as ripei_bbc_profit,
	customer_fuli_profit as fuli_profit,
	--定价毛利额提成
	tc_ripei_profit_salesperson+tc_fuli_profit_salesperson+
	tc_bbc_profit_salesperson as profit_commion,
	--日配定价毛利额提成
	tc_ripei_profit_salesperson as ripei_profit_commion,
	--bbc定价毛利额提成
	tc_bbc_profit_salesperson as bbc_profit_commion,
	--日配bbc定价毛利额提成
	tc_ripei_profit_salesperson+tc_bbc_profit_salesperson as ripei_bbc_profit_commion,
	--福利定价毛利额提成
	tc_fuli_profit_salesperson as fuli_profit_commion,
	salesperson_prorate as prorate,
	salesperson_ripei_prorate as ripei_prorate,
	salesperson_bbc_prorate as bbc_prorate,
	salesperson_ripei_bbc_prorate as ripei_bbc_prorate,
	salesperson_fuli_prorate as fuli_prorate,	
	tc_salesperson as commion_total,
	tc_ripei_sales_value_salesperson+tc_ripei_profit_salesperson as ripei_commion_total,
	tc_bbc_sales_value_salesperson+tc_bbc_profit_salesperson as bbc_commion_total,
	tc_ripei_sales_value_salesperson+tc_ripei_profit_salesperson+
	tc_bbc_sales_value_salesperson+tc_bbc_profit_salesperson as commion_ripei_bbc_total,	
	tc_fuli_sales_value_salesperson+tc_fuli_profit_salesperson as commion_fuli_total,
	salesperson_over_rate as over_rate,
	-1*customer_refund_sales_value as refund_sales_value, --本月退货金额
	-1*customer_refund_ripei_sales_value as ripei_refund_sales_value,
	-1*customer_refund_bbc_sales_value as bbc_refund_sales_value,
	-1*customer_refund_ripei_bbc_sales_value as ripei_bbc_refund_sales_value,--本月日配&BBC退货金额
	-1*customer_refund_fuli_sales_value as fuli_refund_sales_value,--本月福利退货金额
	${hiveconf:last_1day} as sdt,
	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as updated_time,
	smonth
from  
	csx_tmp.tc_new_cust_salary
;



