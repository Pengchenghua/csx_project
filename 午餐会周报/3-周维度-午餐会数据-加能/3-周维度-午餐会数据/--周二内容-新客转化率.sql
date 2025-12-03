
---------------------------- 日配剔除直送仓新客业绩比例
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
    left join ( 
	            select distinct shop_code 
				from csx_dim.csx_dim_shop 
				where sdt='current'
				and shop_low_profit_flag=0  
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
	       where sdt >= regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-2),'-','') 
			  and sdt <= regexp_replace(add_months('${sdt_yes_date}',0),'-','')
	          -- and channel_code in('1','7','9')
	          and business_type_code in ('1') 
		  ) a
    left join ( 
	            select distinct shop_code 
				from csx_dim.csx_dim_shop 
				where sdt='current' 
				and shop_low_profit_flag=0  
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
