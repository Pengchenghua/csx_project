--set last_1day=regexp_replace(date_sub(current_date,1),'-','');
set last_1day='20220320';
set created_time = from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss');
set created_by='zhangyanpeng';
set month_start_day ='20220201';	
set month_end_day ='20220228';	
set year_start_day ='20220101';	

--服务管家提成
insert overwrite table csx_dw.report_sss_r_m_crm_service_commission partition(sdt) 
select
	concat_ws('&', service_user_work_no,smonth) as biz_id,
	region_code_service as region_code,
	region_name_service as region_name,
	province_code_service as province_code,
	province_name_service as province_name,
	city_group_code_service as city_group_code,
	city_group_name_service as city_group_name,
	smonth as yearmonth,
	service_id,
	service_user_work_no as service_work_no,
	service_user_name as service_name,
	sum(customer_sales_value) as sales_value,
	sum(customer_ripei_sales_value) as ripei_sales_value,
	sum(customer_bbc_sales_value) as bbc_sales_value,
	sum(customer_fuli_sales_value) as fuli_sales_value,	
	sum(customer_ripei_bbc_sales_value) as ripei_bbc_sales_value,
	sum(customer_profit) as profit,
	sum(customer_ripei_profit) as ripei_profit,
	sum(customer_bbc_profit) as bbc_profit,
	sum(customer_fuli_profit) as fuli_profit,	
	sum(customer_ripei_bbc_profit) as ripei_bbc_profit,
	coalesce(sum(customer_profit)/abs(sum(customer_sales_value)),0) as prorate,
	coalesce(sum(customer_ripei_profit)/abs(sum(customer_ripei_sales_value)),0) as ripei_prorate,
	coalesce(sum(customer_bbc_profit)/abs(sum(customer_bbc_sales_value)),0) as bbc_prorate,
	coalesce(sum(customer_fuli_profit)/abs(sum(customer_fuli_sales_value)),0) as fuli_prorate,
	coalesce(sum(customer_ripei_bbc_profit)/abs(sum(customer_ripei_bbc_sales_value)),0) as ripei_bbc_prorate,
	sum(salary_sales_value) as salary_sales_value,
	sum(salary_profit) as salary_profit,
	sum(receivable_amount) as receivable_amount,
	sum(overdue_amount) as overdue_amount,
	service_user_over_rate as service_over_rate,
	sum(tc_sales_value_service) as tc_sales_value,
	sum(tc_profit_service) as tc_profit,
	sum(tc_service) as tc_total,
	${hiveconf:created_by} as create_by,
	${hiveconf:created_time} as created_time,
	${hiveconf:created_time} as update_time,
	${hiveconf:last_1day} as sdt
from
	csx_tmp.tc_new_cust_salary
where
	service_user_work_no is not null
group by 
	region_code_service,region_name_service,province_code_service,province_name_service,
	city_group_code_service,city_group_name_service,smonth,service_id,
	service_user_work_no,service_user_name,service_user_over_rate
;

--销售员提成
insert overwrite table csx_dw.report_sss_r_m_crm_salesperson_commission partition(sdt) 
select
	concat_ws('&', cast(sales_id as string),smonth) as biz_id,
	region_code_salesperson as region_code,
	region_name_salesperson as region_name,
	province_code_salesperson as province_code,
	province_name_salesperson as province_name,
	city_group_code_salesperson as city_group_code,
	city_group_name_salesperson as city_group_name,
	smonth as yearmonth,
	sales_id,
	work_no,
	sales_name,
	salesperson_sales_value_ytd as sales_value_ytd,-- 销售员年度累计销售额
	salesperson_ripei_bbc_sales_value_ytd as ripei_bbc_sales_value_ytd,-- 销售员年度累计日配&BBC销售额
	salesperson_fuli_sales_value_ytd as fuli_sales_value_ytd,-- 销售员年度累计福利销售额
	sum(customer_sales_value) as sales_value,
	sum(customer_ripei_sales_value) as ripei_sales_value,
	sum(customer_bbc_sales_value) as bbc_sales_value,
	sum(customer_fuli_sales_value) as fuli_sales_value,	
	sum(customer_ripei_bbc_sales_value) as ripei_bbc_sales_value,
	sum(customer_profit) as profit,
	sum(customer_ripei_profit) as ripei_profit,
	sum(customer_bbc_profit) as bbc_profit,
	sum(customer_fuli_profit) as fuli_profit,	
	sum(customer_ripei_bbc_profit) as ripei_bbc_profit,
	coalesce(sum(customer_profit)/abs(sum(customer_sales_value)),0) as prorate,
	coalesce(sum(customer_ripei_profit)/abs(sum(customer_ripei_sales_value)),0) as ripei_prorate,
	coalesce(sum(customer_bbc_profit)/abs(sum(customer_bbc_sales_value)),0) as bbc_prorate,
	coalesce(sum(customer_fuli_profit)/abs(sum(customer_fuli_sales_value)),0) as fuli_prorate,
	coalesce(sum(customer_ripei_bbc_profit)/abs(sum(customer_ripei_bbc_sales_value)),0) as ripei_bbc_prorate,
	sum(salary_fuli_sales_value) as salary_fuli_sales_value,
	sum(salary_fuli_profit) as salary_fuli_profit,
	sum(salary_ripei_sales_value) as salary_ripei_sales_value,
	sum(salary_ripei_profit) as salary_ripei_profit,
	sum(salary_bbc_sales_value) as salary_bbc_sales_value,
	sum(salary_bbc_profit) as salary_bbc_profit, 
	sum(salary_sales_value) as salary_ripei_bbc_sales_value,
	sum(salary_profit) as salary_ripei_bbc_profit,
	sum(receivable_amount) as receivable_amount,
	sum(overdue_amount) as overdue_amount,
	salesperson_over_rate,
	sum(tc_ripei_sales_value_salesperson+tc_fuli_sales_value_salesperson+tc_bbc_sales_value_salesperson) as tc_sales_value,
	sum(tc_ripei_profit_salesperson+tc_fuli_profit_salesperson+tc_bbc_profit_salesperson) as tc_profit,
	sum(tc_salesperson) as tc_total,
	${hiveconf:created_by} as create_by,
	${hiveconf:created_time} as created_time,
	${hiveconf:created_time} as update_time,
	${hiveconf:last_1day} as sdt
from 
	csx_tmp.tc_new_cust_salary
where
	sales_id is not null
group by 
	region_code_salesperson,region_name_salesperson,province_code_salesperson,province_name_salesperson,
	city_group_code_salesperson,city_group_name_salesperson,smonth,sales_id,work_no,
	sales_name,salesperson_sales_value_ytd,salesperson_ripei_bbc_sales_value_ytd,
	salesperson_fuli_sales_value_ytd,salesperson_over_rate
;

--客户提成
insert overwrite table csx_dw.report_sss_r_m_crm_customer_commission partition(sdt) 
select
	concat_ws('&', cast(customer_id as string),smonth) as biz_id,
	region_code_customer as region_code,
	region_name_customer as region_name,
	province_code_customer as province_code,
	province_name_customer as province_name,
	city_group_code_customer as city_group_code,
	city_group_name_customer as city_group_name,
	smonth as yearmonth,
	customer_id,
	customer_no,
	customer_name,
	coalesce(sales_id,'') as sales_id,
	coalesce(work_no,'') as work_no,
	coalesce(sales_name,'') as sales_name,
	is_part_time_service_manager,
	coalesce(service_id,'') as service_id,
	coalesce(service_user_work_no,'') as service_work_no,
	coalesce(service_user_name,'') as service_name,
	customer_sales_value as sales_value,
	customer_ripei_sales_value as ripei_sales_value,
	customer_bbc_sales_value as bbc_sales_value,
	customer_fuli_sales_value as fuli_sales_value,
	customer_ripei_bbc_sales_value as ripei_bbc_sales_value,
	customer_profit as profit,
	customer_ripei_profit as ripei_profit,
	customer_bbc_profit as bbc_profit,
	customer_fuli_profit as fuli_profit,
	customer_ripei_bbc_profit as ripei_bbc_profit,
	customer_prorate as prorate,
	customer_ripei_prorate as ripei_prorate,
	customer_bbc_prorate as bbc_prorate,
	customer_fuli_prorate as fuli_prorate,
	customer_ripei_bbc_prorate as ripei_bbc_prorate,
	salesperson_ripei_bbc_prorate,
	salesperson_fuli_prorate,
	salary_fuli_sales_value,salary_fuli_profit,
	salary_ripei_sales_value,salary_ripei_profit,
	salary_bbc_sales_value,salary_bbc_profit, 
	salary_sales_value as salary_ripei_bbc_sales_value,
	salary_profit as salary_ripei_bbc_profit,
	receivable_amount,overdue_amount,assigned_type,salesperson_sales_value_fp_rate,salesperson_profit_fp_rate,service_user_sales_value_fp_rate as service_sales_value_fp_rate,
	service_user_profit_fp_rate as service_profit_fp_rate,salesperson_over_rate,service_user_over_rate as service_over_rate,
	tc_ripei_sales_value_salesperson+tc_fuli_sales_value_salesperson+tc_bbc_sales_value_salesperson as tc_ripei_sales_value_salesperson, --提成-销售额-销售员
	tc_sales_value_service,
	tc_ripei_profit_salesperson+tc_fuli_profit_salesperson+tc_bbc_profit_salesperson as tc_ripei_profit_salesperson, --提成-定价毛利额-销售员
	tc_profit_service,
	tc_salesperson,
	tc_service,
	${hiveconf:created_by} as create_by,
	${hiveconf:created_time} as created_time,
	${hiveconf:created_time} as update_time,
	${hiveconf:last_1day} as sdt
from 
	csx_tmp.tc_new_cust_salary
where
	customer_id is not null
;