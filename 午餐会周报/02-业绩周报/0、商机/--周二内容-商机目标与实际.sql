-- MYSQL 
-- 商机目标从MYSQL取
SELECT
    a.performance_region_name,
    a.performance_province_name,
    a.performance_city_name,
    a.business_attribute_code,
    a.business_attribute_name,
    sum(cast(a.estimate_contract_amount as decimal(26,6))) estimate_contract_amount,
    count(a.business_number) businss_cnt,
    sum(if(a.is_new_flag='新客',cast(a.estimate_contract_amount as decimal(26,6)),0)) as new_cust_amount,
    sum(if(a.is_new_flag='新客',1,0)) as new_cust_cnt,
    sum(if(a.is_new_flag='老客',cast(a.estimate_contract_amount as decimal(26,6)),0)) as old_cust_amount,
    sum(if(a.is_new_flag='老客',1,0)) as old_cust_cnt                        -- 目标确认标识
FROM
    data_analysis_prd.report_csx_analyse_tmp_crm_business_info_hi AS a
left join 
    data_analysis_prd.source_crm_business_signing_target b on a.business_number=b.business_number
where sdt=DATE_FORMAT(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY), '%Y%m%d')        
    and b.confir_status=1
    and b.months='202512' 
group by 
	a.performance_region_name,
    a.performance_province_name,
    a.performance_city_name,
    a.business_attribute_code,
    a.business_attribute_name;
	
	
-- ============商机新签==========
select 
	substr(business_sign_time,1,7) month, 
    customer_code,
	customer_name,
	business_number,
	performance_region_code,
	performance_region_name,
	performance_province_code,
	performance_province_name,
	performance_city_code,
	performance_city_name,
	first_category_code,
	first_category_name,
	second_category_code,
	second_category_name,
	third_category_code,
	third_category_name,
	business_attribute_code,
	business_attribute_name,
	estimate_contract_amount,
	to_date(first_sign_time) first_sign_date,
	case when substr(first_sign_time,1,7) = substr(business_sign_time,1,7) then '新签约客户' else '老签约客户' end as new_or_old_customer_mark,
	to_date(business_sign_time) business_sign_date,
	to_date(first_business_sign_time) first_business_sign_date,
	contract_cycle_desc,
	case 
	when contract_cycle_desc in('小于1个月') then estimate_contract_amount
	when regexp_replace(contract_cycle_desc,'个月','') <=12 then estimate_contract_amount
	when regexp_replace(contract_cycle_desc,'个月','') >12 then estimate_contract_amount/regexp_replace(contract_cycle_desc,'个月','')*12
	else estimate_contract_amount end estimate_contract_amount_nh
from 
	csx_dim.csx_dim_crm_business_info
where 
	sdt='current' 
	and to_date(business_sign_time) >=trunc('${sdt_yes_date}','MM')
    and to_date(business_sign_time) <= '${sdt_yes_date}'
    and contract_type = 2 
    and business_stage = 5
	and status='1'
    and business_attribute_code in ('1', '2', '5');