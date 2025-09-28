-- ====================================================================================================================================================

insert overwrite directory '/tmp/zhangyanpeng/20210526_linshi_2' row format delimited fields terminated by '\t' 

-- DESCRIBE csx_dw.dws_crm_w_a_customer;
-- DESCRIBE csx_ods.source_csms_w_a_yszx_customer_relation_new;
select
	a.sales_province_name,
	a.city_group_name,
	a.customer_no,
	a.customer_name,
	b.sap_sub_cus_code,
	b.sap_sub_cus_name,
	a.work_no,
	a.sales_name,
	a.first_supervisor_name,
	a.price_period,
	a.price_type,
	a.price_date,
	a.attribute_name
from	
	(
	select 
		customer_no,customer_name,sales_province_name,city_group_name,work_no,sales_name,first_supervisor_name,price_period,price_type,price_date,attribute_name
	from 
		csx_dw.dws_crm_w_a_customer 
	where 
		sdt='current'
		and sales_province_name='安徽省'
	)a 
	left join
		(
		select 
			sap_sub_cus_code,sap_sub_cus_name,sap_cus_code
		from 
			csx_ods.source_csms_w_a_yszx_customer_relation_new
		where 
			sdt = 'current'
		)b on a.customer_no=b.sap_cus_code




	