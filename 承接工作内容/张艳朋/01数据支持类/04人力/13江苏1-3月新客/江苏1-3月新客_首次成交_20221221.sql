

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
