

-- 大客户提成：月度新客户
select 
	b.performance_province_name,b.performance_city_name,b.customer_code,b.customer_name,b.business_attribute_desc,b.dev_source_name,b.sales_user_number,b.sales_user_name,b.sign_date,
	a.first_sale_date
from
	(
	select 
		business_attribute_desc,dev_source_name,customer_code,customer_name,channel_name,sales_user_name,sales_user_number,performance_province_name,performance_city_name,
		regexp_replace(split(first_sign_time, ' ')[0], '-', '') as sign_date,estimate_contract_amount*10000 estimate_contract_amount
	from 
		-- csx_dw.dws_crm_w_a_customer
		csx_dim.csx_dim_crm_customer_info
	where 
		sdt='current'
		and customer_code<>''
		and channel_code in('1','7','8')
		and performance_province_name in ('江苏南京','江苏苏州')
	)b
	join --客户最早销售月 新客月、新客季度
		(
		select 
			customer_code,
			min(first_sale_date) first_sale_date
		from 
			-- csx_dw.dws_crm_w_a_customer_active
			csx_dws.csx_dws_crm_customer_active_di
		where 
			sdt = 'current'
		group by 
			customer_code
		having 
			min(first_sale_date)>='20220101' and min(first_sale_date)<='20220331'
		)a on b.customer_code=a.customer_code;

-- 人数、销售额、毛利额
select
	substr(a.s_sdt,1,7) as s_month,
	case when a.diff_month between 0 and 3 then '0-3个月'
		when a.diff_month between 4 and 6 then '4-6个月'
		when a.diff_month between 7 and 12 then '7-12个月'
		when a.diff_month >=13 then '1年以上'
		else '其他' end as diff_month_tag,
	case when a.diff_month between 0 and 3 then '0'
		when a.diff_month between 4 and 6 then '1'
		when a.diff_month between 7 and 12 then '2'
		when a.diff_month >=13 then '3'
		else '其他' end as diff_month_flag,		
	count(distinct a.sales_user_number)	as sales_cnt,
	sum(a.sale_amt) as sale_amt,sum(profit) as profit	
from
	(
	select
		ceil(months_between(a.s_sdt,b.s_begin_date)) diff_month,a.s_sdt,b.s_begin_date,
		a.sales_user_number,a.sales_user_name,a.sale_amt,a.profit
	from
		(	
		select
			from_unixtime(unix_timestamp(sdt,'yyyyMMdd'),'yyyy-MM-dd') as s_sdt,sales_user_number,sales_user_name,
			sum(sale_amt) as sale_amt,sum(profit) as profit	
		from
			csx_dws.csx_dws_sale_detail_di
		where
			sdt between '20220101' and '20221130'
			and channel_code in ('1','7','9')
			and sales_user_number !=''
			and sales_user_name not like '%B'
			and sales_user_name not like '%C'
			and sales_user_name not like '%虚拟%'
			and goods_code not in ('8718','8708','8649') --202112月签呈，剔除飞天茅台酒销售额及定价毛利额，每月,'8718','8708','8649'
			and (business_type_code in(1,2,6)
				or (business_type_code in(2,5) and performance_province_name = '平台-B')) --平台酒水
			and (performance_province_name !='福建省' or (performance_province_name='福建省' and inventory_dc_name not like '%V2DC%')) --2.0 按仓库名称判断
		group by 
			from_unixtime(unix_timestamp(sdt,'yyyyMMdd'),'yyyy-MM-dd'),sales_user_number,sales_user_name
		) a 
		left join
			(
			select
				employee_code,employee_name,begin_date,from_unixtime(unix_timestamp(begin_date,'yyyyMMdd'),'yyyy-MM-dd') as s_begin_date
			from
				csx_dim.csx_dim_basic_employee
			where
				sdt='current'
				-- and employee_code='81133021'
			group by 
				employee_code,employee_name,begin_date,from_unixtime(unix_timestamp(begin_date,'yyyyMMdd'),'yyyy-MM-dd')
			) b on b.employee_code=a.sales_user_number
	where
		b.begin_date is not null
	) a 
group by 
	substr(a.s_sdt,1,7),
	case when a.diff_month between 0 and 3 then '0-3个月'
		when a.diff_month between 4 and 6 then '4-6个月'
		when a.diff_month between 7 and 12 then '7-12个月'
		when a.diff_month >=13 then '1年以上'
		else '其他' end ,
	case when a.diff_month between 0 and 3 then '0'
		when a.diff_month between 4 and 6 then '1'
		when a.diff_month between 7 and 12 then '2'
		when a.diff_month >=13 then '3'
		else '其他' end
;

-- 新客数


select
	substr(a.first_sale_date,1,7) as s_month,
	case when a.diff_month between 0 and 3 then '0-3个月'
		when a.diff_month between 4 and 6 then '4-6个月'
		when a.diff_month between 7 and 12 then '7-12个月'
		when a.diff_month >=13 then '1年以上'
		else '其他' end as diff_month_tag,
	case when a.diff_month between 0 and 3 then '0'
		when a.diff_month between 4 and 6 then '1'
		when a.diff_month between 7 and 12 then '2'
		when a.diff_month >=13 then '3'
		else '其他' end as diff_month_flag,		
	count(distinct a.sales_user_number)	as sales_cnt,
	count(distinct a.customer_code)	as customer_cnt
from
	(
	select
		ceil(months_between(a.first_sale_date,b.s_begin_date)) diff_month,a.first_sale_date,b.s_begin_date,
		a.sales_user_number,a.sales_user_name,customer_code
	from
		(
		select 
			sales_user_number,sales_user_name,customer_code,
			from_unixtime(unix_timestamp(min(first_sale_date),'yyyyMMdd'),'yyyy-MM-dd') first_sale_date
		from 
			-- csx_dw.dws_crm_w_a_customer_active
			csx_dws.csx_dws_crm_customer_active_di
		where 
			sdt = 'current'
			and sales_user_number !=''
			and sales_user_name not like '%B'
			and sales_user_name not like '%C'
			and sales_user_name not like '%虚拟%'
		group by 
			sales_user_number,sales_user_name,customer_code
		having 
			min(first_sale_date)>='20220101' and min(first_sale_date)<='20221130'
		) a 
		left join
			(
			select
				employee_code,employee_name,begin_date,from_unixtime(unix_timestamp(begin_date,'yyyyMMdd'),'yyyy-MM-dd') as s_begin_date
			from
				csx_dim.csx_dim_basic_employee
			where
				sdt='current'
				-- and employee_code='81133021'
			group by 
				employee_code,employee_name,begin_date,from_unixtime(unix_timestamp(begin_date,'yyyyMMdd'),'yyyy-MM-dd')
			) b on b.employee_code=a.sales_user_number
	where
		b.begin_date is not null
	) a 
group by 
	substr(a.first_sale_date,1,7),
	case when a.diff_month between 0 and 3 then '0-3个月'
		when a.diff_month between 4 and 6 then '4-6个月'
		when a.diff_month between 7 and 12 then '7-12个月'
		when a.diff_month >=13 then '1年以上'
		else '其他' end ,
	case when a.diff_month between 0 and 3 then '0'
		when a.diff_month between 4 and 6 then '1'
		when a.diff_month between 7 and 12 then '2'
		when a.diff_month >=13 then '3'
		else '其他' end
		
