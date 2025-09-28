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

insert overwrite table csx_dw.report_sss_r_m_crm_service_commission partition(sdt) 
select
	concat_ws('&', cast(b.service_user_id as string),a.smonth) as biz_id,
	b.sales_region_code as region_code,
	b.sales_region_name as region_name,
	b.sales_province_code as province_code,
	b.sales_province_name as province_name,
	b.city_group_code,
	b.city_group_name,
	a.smonth as yearmonth,
	b.service_user_id as service_id,
	b.service_user_work_no as service_work_no,
	b.service_user_name as service_name,
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
	coalesce(sum(tc_sales_value_service),0)+coalesce(sum(tc_profit_service),0) tc_total,
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
	where
		service_user_work_no !=''
		--and service_user_work_no in ('81122116','81129243','81129344')
	) a 
	left join
		(
		select
			customer_no,sales_region_code,sales_region_name,sales_province_code,sales_province_name,city_group_code,city_group_name,
			concat_ws(';', collect_list(cast(service_user_id as string))) as service_user_id,
			concat_ws(';', collect_list(service_user_work_no)) as service_user_work_no,
			concat_ws(';', collect_list(service_user_name)) as service_user_name
		from
			(
			select
				distinct customer_no,service_user_id,service_user_work_no,service_user_name,sales_region_code,sales_region_name,sales_province_code,sales_province_name,
				city_group_code,city_group_name
			from
				csx_dw.dws_crm_w_a_customer_sales_link
			where
				sdt='20220228'
				and is_additional_info = 1 
				and service_user_id <> 0
			) t1
		group by 
			customer_no,sales_region_code,sales_region_name,sales_province_code,sales_province_name,city_group_code,city_group_name
		) b on b.customer_no=a.customer_no
group by 
	b.sales_region_code,b.sales_region_name,b.sales_province_code,b.sales_province_name,b.city_group_code,b.city_group_name,a.smonth,b.service_user_id,
	b.service_user_work_no,b.service_user_name,service_user_over_rate
;