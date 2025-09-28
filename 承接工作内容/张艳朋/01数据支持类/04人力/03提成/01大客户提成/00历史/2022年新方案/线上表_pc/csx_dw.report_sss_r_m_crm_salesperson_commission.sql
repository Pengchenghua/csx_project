---财务销售结算 crm 销售员销售结算 提成趋势

--SET hive.execution.engine=mr;
--动态分区
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

--中间结果压缩
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.type=BLOCK;
SET parquet.compression=SNAPPY;
--启用引号识别
set hive.support.quoted.identifiers=none;
set hive.map.aggr=true;

----昨天
set last_1day=regexp_replace(date_sub(current_date,1),'-','');
set created_time = from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss');
set created_by='zhangyanpeng';

insert overwrite table csx_dw.report_sss_r_m_crm_salesperson_commission partition(sdt) 
select
	concat_ws('&', cast(b.sales_id as string),a.smonth) as biz_id,
	b.sales_region_code as region_code,
	b.sales_region_name as region_name,
	b.province_code,
	b.province_name,
	b.city_group_code,
	b.city_group_name,
	smonth as yearmonth,
	b.sales_id,
	a.work_no,
	a.sales_name,
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
	sum(tc_ripei_sales_value_salesperson) as tc_sales_value,
	sum(tc_ripei_profit_salesperson) as tc_profit,
	coalesce(sum(tc_ripei_sales_value_salesperson),0)+coalesce(sum(tc_ripei_profit_salesperson),0)+
	coalesce(sum(tc_fuli_sales_value_salesperson),0)+coalesce(sum(tc_fuli_profit_salesperson),0)+
	coalesce(sum(tc_bbc_sales_value_salesperson),0)+coalesce(sum(tc_bbc_profit_salesperson),0) tc_total,
	${hiveconf:created_by} as create_by,
	${hiveconf:created_time} as created_time,
	${hiveconf:created_time} as update_time,
	${hiveconf:last_1day} as sdt
from 
	(
	select 
		*
	from
		csx_tmp.tc_new_cust_salary
	--where
		--work_no in ('80767662','80768307','80803749')
	) a 
	left join
		(
		select
			customer_id,customer_no,sales_id,work_no,sales_name,sales_region_code,sales_region_name,province_code,province_name,city_group_code,city_group_name
		from
			csx_dw.dws_crm_w_a_customer
		where
			sdt='20220228'
		) b on b.customer_no=a.customer_no
group by 
	b.sales_region_code,b.sales_region_name,b.province_code,b.province_name,b.city_group_code,b.city_group_name,a.smonth,b.sales_id,
	a.work_no,a.sales_name,salesperson_sales_value_ytd,salesperson_ripei_bbc_sales_value_ytd,salesperson_fuli_sales_value_ytd,salesperson_over_rate
;