-- ============新系统数据==========
select 
	substr(business_sign_time,1,7) month 
    ,customer_code
	,customer_name
	,business_number
	,performance_region_code
	,performance_region_name
	,performance_province_code
	,performance_province_name
	,performance_city_code
	,performance_city_name
	,first_category_code
	,first_category_name
	,second_category_code
	,second_category_name
	,third_category_code
	,third_category_name
	,business_attribute_code
	,business_attribute_name
	,estimate_contract_amount
	,to_date(first_sign_time) first_sign_date
	,case when substr(first_sign_time,1,7) = substr(business_sign_time,1,7) then '新签约客户' else '老签约客户' end as new_or_old_customer_mark
	,to_date(business_sign_time) business_sign_date
	,to_date(first_business_sign_time) first_business_sign_date
from 
	csx_dim.csx_dim_crm_business_info
where 
	sdt='current' 
	and to_date(business_sign_time) >= '2023-07-01'
    and to_date(business_sign_time) <= '2023-07-31' -- date_sub(current_date(),1)
    and business_stage = 5 
	and status='1'
    and business_attribute_code in ('1', '2', '5')