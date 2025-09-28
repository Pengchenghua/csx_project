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

insert overwrite table csx_dw.report_sss_r_m_crm_customer_commission partition(sdt) 
select
	concat_ws('&', cast(b.customer_id as string),a.smonth) as biz_id,
	b.sales_region_code as region_code,
	b.sales_region_name as region_name,
	b.province_code,
	b.province_name,
	b.city_group_code,
	b.city_group_name,
	smonth as yearmonth,
	b.customer_id,
	a.customer_no,
	a.customer_name,
	b.sales_id,
	a.work_no,
	a.sales_name,
	is_part_time_service_manager,
	c.service_user_id as service_id,
	a.service_user_work_no as service_work_no,
	a.service_user_name as service_name,
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
	service_user_profit_fp_rate as service_profit_fp_rate,salesperson_over_rate,service_user_over_rate as service_over_rate,tc_ripei_sales_value_salesperson,tc_sales_value_service,
	tc_ripei_profit_salesperson,tc_profit_service,
	coalesce(tc_ripei_sales_value_salesperson,0)+coalesce(tc_ripei_profit_salesperson,0)+
	coalesce(tc_fuli_sales_value_salesperson,0)+coalesce(tc_fuli_profit_salesperson,0)+
	coalesce(tc_bbc_sales_value_salesperson,0)+coalesce(tc_bbc_profit_salesperson,0) as tc_salesperson,
	coalesce(tc_sales_value_service,0)+coalesce(tc_profit_service,0) as tc_service,
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
	--	customer_no in ('105235','105381','105557')
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
		) c on c.customer_no=a.customer_no
;