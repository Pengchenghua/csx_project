--==============================================================================================================================================================================

-- 业务类型销售额

select
	b.sales_region_name,
	b.province_name,
	a.business_type_name,
	a.smonth,
	sum(a.sales_value) sales_value,
	sum(a.profit) as profit,
	sum(a.profit)/abs(sum(a.sales_value)) as profit_rate,
	count(distinct a.customer_no) as customer_cnt
from
	(
	select 
		customer_no,business_type_name,substr(sdt,1,6) as smonth,sales_value,profit
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20200101' and sdt<='20211109'
		and channel_code in('1','7','9')
		--and business_type_code ='1'
	) a 
	join 
		(
		select
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
			sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and province_name='北京市'
		)b on a.customer_no=b.customer_no
group by
	b.sales_region_name,
	b.province_name,
	a.business_type_name,
	a.smonth		
;	

--==============================================================================================================================================================================

-- 新履约客户及履约金额

select
	b.sales_region_name,
	b.province_name,
	a.smonth,
	count(a.customer_no) as customer_cnt
from
	(
	select
		sales_id,substr(first_order_date,1,6) as smonth,customer_no
	from
		csx_dw.dws_crm_w_a_customer_active
	where 
		sdt = 'current' 
		and substr(first_order_date,1,6) between '202001' and '202111'
	) as a 
	join 
		(
		select
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
			sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and province_name='北京市'
		) b on a.customer_no=b.customer_no
group by 
	b.sales_region_name,
	b.province_name,
	a.smonth
;	
