--战略客户部季度绩效数据
-- 商机年化金额 
 
--战略客户部季度绩效数据
-- 商机年化金额 
 with tmp_business_detail as (select * ,
if((business_attribute_name in ('福利','BBC') and estimate_contract_amount_nh>=200) or (business_attribute_name in ('日配') and estimate_contract_amount_nh>=1000),1,0) is_new_flag
from 
(select a.customer_code,
	b.customer_name,
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
	b.second_category_name,
	third_category_code,
	third_category_name,
	business_attribute_code,
	business_attribute_name,
	business_type_name,
	estimate_contract_amount,
	to_date(first_sign_time) first_sign_date,
	to_date(business_sign_time) business_sign_date,
	to_date(first_business_sign_time) first_business_sign_date,
	contract_cycle_desc,
	case 
	when contract_cycle_desc in('小于1个月') then estimate_contract_amount
	when regexp_replace(contract_cycle_desc,'个月','') <=12 then estimate_contract_amount
	when regexp_replace(contract_cycle_desc,'个月','') >12 then estimate_contract_amount/regexp_replace(contract_cycle_desc,'个月','')*12
	else estimate_contract_amount end estimate_contract_amount_nh,
	b.strategy_user_name,
	owner_user_number,
	owner_user_name,
	row_number()over(partition by a.customer_id,business_attribute_name order by business_sign_time desc ) rn  
from    csx_dim.csx_dim_crm_business_info a 
left join 
(
  select 
    customer_id,
    customer_code,
	customer_name ,
	strategy_status,
	strategy_user_name,
	second_category_name
  from    csx_dim.csx_dim_crm_customer_info 
  where sdt='current'
  and customer_code<>''
  -- and channel_code in('1','7','9')
  
) b on a.customer_id=b.customer_id
where sdt='current'
-- and strategy_status=1
and business_stage=5
and status=1
)a 
where rn=1 
and a.owner_user_number in ('81154555','80765593','80080009','81233104','81310462')
),
--战略客户部销售
 tmp_sale_detail as (
SELECT substr(sdt,1,6) mon,
    a.performance_region_name,
    a.performance_province_name,
  a.customer_code,
  b.customer_name,
  a.business_type_name,
  b.second_category_name,
  strategy_user_name,
  -- sign_company_code,
  agreement_company_code,
--   agreement_dc_code,
  sum(sale_amt) sale_amt,
  sum(a.profit) profit,
  sum(a.profit)/abs(sum(a.sale_amt)) maolilv,
  sum(if(partner_type_code in (1,3),sale_amt,0)) par_sale_amt,
  sum(if(partner_type_code in (1,3),profit,0)) par_profit
from     csx_dws.csx_dws_sale_detail_di a
left  join
--    客户信息
(
  select 
    a.customer_code,
	a.customer_name ,
	if(b.customer_code is not null ,1,a.strategy_status) strategy_status,
	if(a.strategy_user_name='',b.owner_user_name,a.strategy_user_name) strategy_user_name,
	a.second_category_name
  from    csx_dim.csx_dim_crm_customer_info a
  left join tmp_business_detail b on a.customer_code=b.customer_code
  where sdt='current'
  and a.customer_code<>''

)b on b.customer_code=a.customer_code
left join 
(select shop_code,company_code as agreement_company_code from csx_dim.csx_dim_shop where sdt='current') c on a.agreement_dc_code=c.shop_code
where a.sdt>='20251001' 
and a.sdt<='20251231'
    and (b.strategy_status =1 or a.customer_code in('243884','232923'))
    -- and partner_type_code not  in (1, 3)
group by a.customer_code,b.customer_name,a.business_type_name,strategy_user_name,
substr(sdt,1,6)
-- sign_company_code,
-- agreement_dc_code,
,agreement_company_code,
b.second_category_name,
a.performance_province_name,
a.performance_region_name
) 
select a.*,if(b.customer_code is not null ,1,0 ) is_new_cust_flag,
	if(c.is_new_flag=1, c.business_sign_date,'')business_sign_date,
	if(c.is_new_flag=1, c.estimate_contract_amount_nh,'')estimate_contract_amount_nh
from tmp_sale_detail a 
left join 
(select customer_code,business_type_name
 from csx_analyse.csx_analyse_sales_new_customer_info_mf
 where smt>='202510'
 group by customer_code,business_type_name
 ) b on a.customer_code=b.customer_code and  a.business_type_name=b.business_type_name
left join 
tmp_business_detail c on a.customer_code=c.customer_code and a.business_type_name=c.business_type_name

;

--战略客户部销售
SELECT substr(sdt,1,6) mon,
  a.customer_code,
  b.customer_name,
  a.business_type_name,
  b.second_category_name,
  strategy_user_name,
  -- sign_company_code,
  agreement_company_code,
--   agreement_dc_code,
  sum(sale_amt) sale_amt,
  sum(a.profit) profit,
  sum(a.profit)/abs(sum(a.sale_amt)) maolilv
from     csx_dws.csx_dws_sale_detail_di a
left  join
--    客户信息
(
  select 
    customer_code,
	customer_name ,
	strategy_status,
	strategy_user_name,
	second_category_name
  from    csx_dim.csx_dim_crm_customer_info 
  where sdt='current'
  and customer_code<>''
  -- and channel_code in('1','7','9')
  
)b on b.customer_code=a.customer_code
left join 
(select shop_code,company_code as agreement_company_code from csx_dim.csx_dim_shop where sdt='current') c on a.agreement_dc_code=c.shop_code
where a.sdt>='20250401' 
and a.sdt<='20250630'
    and (b.strategy_status =1 or a.customer_code in('243884','232923'))
    -- and partner_type_code not  in (1, 3)
group by a.customer_code,b.customer_name,a.business_type_name,strategy_user_name,
substr(sdt,1,6)
-- sign_company_code,
-- agreement_dc_code,
,agreement_company_code,
b.second_category_name
;

-- 商机年化金额 
select * 
from 
(select a.customer_code,
	b.customer_name,
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
	b.second_category_name,
	third_category_code,
	third_category_name,
	business_attribute_code,
	business_attribute_name,
	estimate_contract_amount,
	to_date(first_sign_time) first_sign_date,
	to_date(business_sign_time) business_sign_date,
	to_date(first_business_sign_time) first_business_sign_date,
	contract_cycle_desc,
	case 
	when contract_cycle_desc in('小于1个月') then estimate_contract_amount
	when regexp_replace(contract_cycle_desc,'个月','') <=12 then estimate_contract_amount
	when regexp_replace(contract_cycle_desc,'个月','') >12 then estimate_contract_amount/regexp_replace(contract_cycle_desc,'个月','')*12
	else estimate_contract_amount end estimate_contract_amount_nh,
	b.strategy_user_name,
	row_number()over(partition by a.customer_id,business_attribute_name order by business_sign_time desc ) rn  
from csx_dim.csx_dim_crm_business_info a 
join 
(
  select 
    customer_id,
    customer_code,
	customer_name ,
	strategy_status,
	strategy_user_name,
	second_category_name
  from    csx_dim.csx_dim_crm_customer_info 
  where sdt='current'
  and customer_code<>''
  -- and channel_code in('1','7','9')
  
) b on a.customer_id=b.customer_id
where sdt='current'
and strategy_status=1
and business_stage=5
and status=1
)a 
where rn=1
and ((business_attribute_name in ('福利','BBC') and estimate_contract_amount_nh>=200)
        or (business_attribute_name in ('日配') and estimate_contract_amount_nh>=1000))