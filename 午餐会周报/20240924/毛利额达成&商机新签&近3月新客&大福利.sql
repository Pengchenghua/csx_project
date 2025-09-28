---------------------------- 业务类型毛利额达成
select 
smonth,
business_type_name,
province_name,
city_group_name,
sum(sale_amt) as sale_amt,
sum(profit) as profit,
sum(sale_amt_target) as sale_amt_target,
sum(profit_target) as profit_target
from 
(
select substr(sdt,1,6) smonth,
business_type_name,
performance_province_name province_name,
performance_city_name city_group_name,
sum(sale_amt) as sale_amt,
sum(profit) as profit,
null as sale_amt_target,
null as profit_target
from csx_dws.csx_dws_sale_detail_di
where sdt >=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-0),'-','')
and sdt <=regexp_replace('${sdt_yes_date}','-','')
and performance_province_name not like '平台%'
group by substr(sdt,1,6),
business_type_name,
performance_province_name,
performance_city_name

union all
select
month as smonth,
business_type_name,
province_name,
city_group_name,
null as sale_amt,
null as profit,
sum(sales_value) as sale_amt_target,
sum(profit) as profit_target
from csx_ods.csx_ods_csx_data_market_dws_basic_w_a_business_target_manage_df ---kpi目标
where month=substr(regexp_replace('${sdt_yes_date}','-',''),1,6)
and province_name not like '平台%'
group by month,
business_type_name,
province_name,
city_group_name
)a
group by smonth,
business_type_name,
province_name,
city_group_name
order by business_type_name,province_name;



---------------------------- 商机新签
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
    and business_stage = 5 
	and status='1'
    and business_attribute_code in ('1', '2', '5');


---------------------------- 新客业绩占比  日配剔除直送仓
-- 本月与近3个月
-- 本月
select
	'本月' month_flag,
	a.performance_region_name,
	a.performance_province_name,	
	a.performance_city_name,
	a.business_type_name,
	if(c.first_sales_date >= regexp_replace(trunc('${sdt_yes_date}','MM'),'-',''),'新客','老客') as xinlaok,
	count(distinct a.customer_code) by_cust,
	sum(a.sale_amt)/10000 by_sale_amt,
	sum(a.profit)/10000 by_profit		
	from (
	       select * 
	       from csx_dws.csx_dws_sale_detail_di 
	       where sdt >=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),0),'-','') 
			  and sdt <= regexp_replace(add_months('${sdt_yes_date}',0),'-','') 
	          -- and channel_code in('1','7','9')
	          and business_type_code in ('1') 
		  ) a
    join ( 
	            select distinct shop_code 
				from csx_dim.csx_dim_shop 
				where sdt='current' and shop_low_profit_flag=0  
			  )b on a.inventory_dc_code = b.shop_code
left join  -- 首单日期
(
  select 
    customer_code,
	business_type_code,
	min(first_business_sale_date) first_sales_date
  from csx_dws.csx_dws_crm_customer_business_active_di
  where sdt ='current' and 	business_type_code in (1)
  group by customer_code,
           business_type_code
)c on c.customer_code=a.customer_code and c.business_type_code=a.business_type_code
group by 
	a.performance_region_name,
	a.performance_province_name,	
	a.performance_city_name,
	a.business_type_name,
	if(c.first_sales_date >= regexp_replace(trunc('${sdt_yes_date}','MM'),'-',''),'新客','老客')

-- 近3月
union all
select
	'近3月' month_flag,
	a.performance_region_name,
	a.performance_province_name,	
	a.performance_city_name,
	a.business_type_name,
	if(c.first_sales_date >= regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-2),'-',''),'新客','老客') as xinlaok,
	count(distinct a.customer_code) by_cust,
	sum(a.sale_amt)/10000 by_sale_amt,
	sum(a.profit)/10000 by_profit		
	from (
	       select * 
	       from csx_dws.csx_dws_sale_detail_di 
	       where sdt >=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-2),'-','') 
			  and sdt <= regexp_replace(add_months('${sdt_yes_date}',0),'-','')
	          -- and channel_code in('1','7','9')
	          and business_type_code in ('1') 
		  ) a
    join ( 
	            select distinct shop_code 
				from csx_dim.csx_dim_shop 
				where sdt='current' and shop_low_profit_flag=0  
			  )b on a.inventory_dc_code = b.shop_code
left join  -- 首单日期
(
  select 
    customer_code,
	business_type_code,
	min(first_business_sale_date) first_sales_date
  from csx_dws.csx_dws_crm_customer_business_active_di
  where sdt ='current' and 	business_type_code in (1)
  group by customer_code,
           business_type_code
)c on c.customer_code=a.customer_code and c.business_type_code=a.business_type_code
group by 
	a.performance_region_name,
	a.performance_province_name,	
	a.performance_city_name,
	a.business_type_name,
	if(c.first_sales_date >= regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-2),'-',''),'新客','老客')
;



---------------------------- 福利BBC达成进度

select substr(sdt,1,6) smonth,
performance_region_name,
performance_province_name province_name,
performance_city_name city_group_name,
business_type_name,
sum(sale_amt) as sale_amt,
sum(profit) as profit,
if(business_type_name='福利业务','福利',business_type_name) as business_type_name_new
from csx_dws.csx_dws_sale_detail_di
where sdt >=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-0),'-','')
and sdt <=regexp_replace('${sdt_yes_date}','-','')
and business_type_code in ('2','6')
group by substr(sdt,1,6),
business_type_name,
performance_province_name,
performance_city_name,
performance_region_name,
if(business_type_name='福利业务','福利',business_type_name)




/*
---------------------------- 日配剔除直送仓新客业绩比例   旧版 仅本月
select
	a.performance_region_name,
	a.performance_province_name,	
	a.performance_city_name,
	a.business_type_name,
	if(c.first_sales_date >= regexp_replace(trunc('${sdt_yes_date}','MM'),'-','') and c.first_sales_date<= regexp_replace(add_months('${sdt_yes_date}',0),'-',''),'新客','老客') as xinlaok,
	count(distinct case when a.sdt >= regexp_replace(trunc('${sdt_yes_date}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${sdt_yes_date}',0),'-','') then a.customer_code end) by_cust,
	sum(case when a.sdt >= regexp_replace(trunc('${sdt_yes_date}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${sdt_yes_date}',0),'-','') then a.sale_amt end)/10000 by_sale_amt,
	sum(case when a.sdt >= regexp_replace(trunc('${sdt_yes_date}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${sdt_yes_date}',0),'-','') then a.profit end)/10000 by_profit		
	from (
	       select * 
	       from csx_dws.csx_dws_sale_detail_di 
	       where sdt >=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),0),'-','') 
			  and sdt <= regexp_replace(add_months(date_sub(current_date,1),0),'-','') 
	          -- and channel_code in('1','7','9')
	          and business_type_code in ('1') 
		  ) a
    join ( 
	            select distinct shop_code 
				from csx_dim.csx_dim_shop 
				where sdt='current' and shop_low_profit_flag=0  
			  )b on a.inventory_dc_code = b.shop_code
left join  -- 首单日期
(
  select 
    customer_code,
	business_type_code,
	min(first_business_sale_date) first_sales_date
  from csx_dws.csx_dws_crm_customer_business_active_di
  where sdt ='current' and 	business_type_code in (1)
  group by customer_code,
           business_type_code
)c on c.customer_code=a.customer_code and c.business_type_code=a.business_type_code
group by 
	a.performance_region_name,
	a.performance_province_name,	
	a.performance_city_name,
	a.business_type_name,
	if(c.first_sales_date >= regexp_replace(trunc('${sdt_yes_date}','MM'),'-','') and c.first_sales_date<= regexp_replace(add_months('${sdt_yes_date}',0),'-',''),'新客','老客')
;
*/



