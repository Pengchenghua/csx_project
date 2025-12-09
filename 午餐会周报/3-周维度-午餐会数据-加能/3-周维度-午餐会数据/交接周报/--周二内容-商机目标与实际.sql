-- MYSQL 
-- 商机目标从MYSQL取
SELECT
	performance_region_name,
	performance_province_name,
	case when performance_province_name='上海' then performance_city_name else '' end performance_city_name,
	sum(cast(a.estimate_contract_amount as decimal(26,6))) estimate_contract_amount,
	count(a.business_number) businss_cnt
FROM
	data_analysis_prd.report_csx_analyse_tmp_crm_business_info_hi AS a
	left join 
	data_analysis_prd.source_crm_business_signing_target b on a.business_number=b.business_number
	where sdt=DATE_FORMAT(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY), '%Y%m%d')	
	and confir_status=1
	and b.months='202509' 
-- 	and a.business_attribute_name in ('福利','BBC')
	group by performance_region_name,
	a.performance_province_name ,
	case when performance_province_name='上海' then performance_city_name else '' end 
	order by 
	 case  performance_province_name when '北京市' then 1 
	 	when '河北省' then 2 
	 	when '陕西省' then 3 
	 	when '河南省' then 4
	 	when '东北' then 5
	 	when '福建省' then 6
	 	when '广东深圳' then 7
	 	when '广东广州' then 8
	 	when '江西省' then 9
	 	when '上海' then 12.1
	 	when '上海松江' then 13
	 	when '江苏苏州' then 14
	 	when '江苏南京' then 15
	 	when '重庆市' then 10
	 	when '四川省' then 11
	 	when '贵州省' then 12 	 	
	 	when '浙江省' then 16
	 	when '安徽省' then 17
	 	when '湖北省' then 18
	 	else 19 end 
	 	
	;
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