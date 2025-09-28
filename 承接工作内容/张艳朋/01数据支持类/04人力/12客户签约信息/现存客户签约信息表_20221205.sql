select
	performance_region_name,performance_province_name,performance_city_name,
	customer_code,
	customer_name,
	channel_name,
	sales_user_number,
	sales_user_name,
	business_attribute_desc,
	sign_date,
	first_sign_date,
	first_sale_date,
	last_sale_date
from
	csx_dws.csx_dws_crm_customer_active_di
where
	sdt='current'
	and last_sale_date>='20220601'
	
select
	performance_region_name,performance_province_name,performance_city_name,
	business_number,customer_code,customer_name,
	business_attribute_name,
	business_sign_time,
	first_business_sign_time,
	channel_name,
	first_category_name,
	second_category_name,
	third_category_name,
	contract_begin_date,
	contract_end_date,
	estimate_contract_amount,
	contract_cycle,
	first_sign_time,
	business_type_name
from
	csx_dim.csx_dim_crm_business_info
where
	sdt='current'
	and status=1 -- 是否有效 0无效 1有效
	and approval_status_code=2 -- 审批状态编码 0:待发起 1：审批中 2：审批完成 3：审批拒绝
	and business_stage=5 -- 阶段状态 1.阶段1 2.阶段2 3.阶段3 4.阶段4 5.阶段5
	and customer_id in 
		(select 
			distinct customer_id 
		from 
			csx_dws.csx_dws_crm_customer_active_di
		where
			sdt='current' and last_sale_date>='20220601')
	
	
	
	