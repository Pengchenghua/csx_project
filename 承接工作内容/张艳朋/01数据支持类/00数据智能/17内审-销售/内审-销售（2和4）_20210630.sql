-- 客户签约后超3个月仍未履约客户（1-4月至今未履约）

select 
	a.sales_province_name,
	a.city_group_name,
	a.customer_no,
	a.customer_name,
	a.sign_date,
	a.work_no,
	a.sales_name,
	a.first_supervisor_name,
	a.first_category_name,
	a.second_category_name,
	a.third_category_name,
	a.attribute_desc,
	a.cooperation_mode_name,
	a.dev_source_name
from 
	(
	select
		customer_no,customer_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,sales_province_name,city_group_name,
		work_no,sales_name,first_supervisor_name,
		first_category_name,second_category_name,third_category_name,attribute_desc,cooperation_mode_name,dev_source_name
	from  
		csx_dw.dws_crm_w_a_customer 
	where 
		sdt='current'
		and regexp_replace(substr(sign_time,1,10),'-','')>='20210101'
		and regexp_replace(substr(sign_time,1,10),'-','')<='20210430'
		and sales_province_name='四川省'
	) a 
	left join 
		(
		select
			customer_no
		from  
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20210101'
			and sdt<='20210628'
		group by 
			customer_no
		) b on a.customer_no=b.customer_no
where 
	b.customer_no is null




-- 连续3个月无新成交客户的销售人员；

select 
	a.province_name,
	a.work_no,
	a.sales_name,
	a.position,
	b.begin_date,
	a.sales_supervisor_name
from 
	(
	select
		sales_id,work_no,sales_name,position,sales_supervisor_name,province_name
	from  
		csx_dw.dws_uc_w_a_sale_org_m 
	where 
		sdt='20210629'
		and position = 'SALES'
	) a 
	left join
		(
		select
			employee_code,employee_name,begin_date,end_date,emp_status
		from
			csx_dw.dws_basic_w_a_employee_org_m
		where
			sdt='20210629'
		) b on b.employee_code=a.work_no	
	left join 
		(
		select
			sales_id,sales_name,first_order_date,last_order_date
		from  
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt='20210629'
			and first_order_date>='20210401'
			and first_order_date<='20210629'
		group by 
			sales_id,sales_name,first_order_date,last_order_date
		) c on a.sales_id=c.sales_id
where 
	c.sales_id is null
	
	
	
-- 客户签约后超3个月仍未履约客户（1-4月至今未履约）

select 
	a.sales_province_name,
	a.city_group_name,
	a.customer_no,
	a.customer_name,
	a.sign_date,
	a.work_no,
	a.sales_name,
	a.first_supervisor_name,
	a.first_category_name,
	a.second_category_name,
	a.third_category_name,
	a.attribute_desc,
	a.cooperation_mode_name,
	a.dev_source_name,
	a.estimate_contract_amount
from 
	(
	select
		customer_no,customer_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,sales_province_name,city_group_name,
		work_no,sales_name,first_supervisor_name,
		first_category_name,second_category_name,third_category_name,attribute_desc,cooperation_mode_name,dev_source_name,estimate_contract_amount
	from  
		csx_dw.dws_crm_w_a_customer 
	where 
		sdt='current'
		and regexp_replace(substr(sign_time,1,10),'-','')>='20210101'
		and regexp_replace(substr(sign_time,1,10),'-','')<='20210430'
		and sales_province_name='四川省'
	) a 
	left join 
		(
		select
			customer_no
		from  
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20210101'
			and sdt<='20210630'
		group by 
			customer_no
		) b on a.customer_no=b.customer_no
where 
	b.customer_no is null
	
	
	
	
-- 连续3个月无新成交客户的销售人员；

select 
	a.province_name,
	a.work_no,
	a.sales_name,
	a.position,
	b.begin_date,
	a.sales_supervisor_name,
	a.org_name
from 
	(
	select
		sales_id,work_no,sales_name,position,sales_supervisor_name,province_name,org_name
	from  
		csx_dw.dws_uc_w_a_sale_org_m 
	where 
		sdt='20210630'
		and position = 'SALES'
		and province_name='四川省'
		and org_name !='商超'
	) a 
	left join
		(
		select
			employee_code,employee_name,begin_date,end_date,emp_status
		from
			csx_dw.dws_basic_w_a_employee_org_m
		where
			sdt='20210630'
		) b on b.employee_code=a.work_no	
	left join 
		(
		select
			sales_id,sales_name,first_order_date,last_order_date
		from  
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt='20210630'
			and first_order_date>='20210401'
			and first_order_date<='20210630'
		group by 
			sales_id,sales_name,first_order_date,last_order_date
		) c on a.sales_id=c.sales_id
where 
	c.sales_id is null