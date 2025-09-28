-- 新开客户数、履约客户数和连续三月未开新客的BD

-- 5月新开客户数、履约客户数 20240604
select 
	performance_region_name,
	performance_province_name,	
	performance_city_name,
	sum(by_cust_new) as by_cust_new,
	sum(by_cust) as by_cust	
from 
(
select
	performance_region_name,
	performance_province_name,	
	performance_city_name,
	-- business_type_name,
	0 as by_cust_new,
	count(distinct customer_code) by_cust
	-- sum(sale_amt)/10000 by_sale_amt,
	-- sum(profit)/10000 by_profit		
from csx_dws.csx_dws_sale_detail_di 
where sdt >=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),0),'-','') 
and sdt <= regexp_replace(add_months('${sdt_yes_date}',0),'-','') 
-- and channel_code in('1','7','9')
and business_type_code in ('1','2','6')  -- 日配福利BBC
group by 
	performance_region_name,
	performance_province_name,	
	performance_city_name

union all
select 
	performance_region_name,
	performance_province_name,	
	performance_city_name,
	count(distinct customer_code) by_cust_new,
	0 as by_cust
from csx_analyse.csx_analyse_fr_tc_sales_new_customer_business_mi
where is_new_customer='是'
and sales_user_number<>''
and sales_user_number is not null
and smt=substr(regexp_replace('${sdt_yes_date}','-',''),1,6)
group by 
	performance_region_name,
	performance_province_name,	
	performance_city_name
) a
group by 
	performance_region_name,
	performance_province_name,	
	performance_city_name



; 


-- 5月新开客户数、履约客户数 20240604
select 
	performance_region_name,
	performance_province_name,	
	performance_city_name,
	a.business_type_name,
	a.customer_code,
	c.customer_name,
	first_category_name,
	second_category_name,
	sum(by_cust_new) as by_cust_new,
	sum(by_cust) as by_cust	
from 
(
select
	performance_region_name,
	performance_province_name,	
	performance_city_name,
	business_type_code,
	business_type_name,
	customer_code,
	sum(sale_amt)/10000 by_sale_amt,
	sum(profit)/10000 by_profit		
from csx_dws.csx_dws_sale_detail_di 
where sdt >=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),0),'-','') 
and sdt <= regexp_replace(add_months('${sdt_yes_date}',0),'-','') 
-- and channel_code in('1','7','9')
and business_type_code in ('1','2','6')  -- 日配福利BBC
group by 
	performance_region_name,
	performance_province_name,	
	performance_city_name,
	business_type_name,
	business_type_code,
	customer_code
)a
left join 
(
select 
	business_type_code,
	customer_code
from csx_analyse.csx_analyse_fr_tc_sales_new_customer_business_mi
where is_new_customer='是'
and sales_user_number<>''
and sales_user_number is not null
and smt=substr(regexp_replace('${sdt_yes_date}','-',''),1,6)
) b on a.business_type_code=b.business_type_code and a.customer_code=b.customer_code
left join
(select customer_code,
	customer_name,
	first_category_name,
	second_category_name
from csx_dim.csx_dim_crm_customer_info 
	where sdt='current'
)c on a.customer_code=c.customer_code
group by 	performance_region_name,
	performance_province_name,	
	performance_city_name,
	a.business_type_name,
	a.customer_code,
	c.customer_name,
	first_category_name,
	second_category_name



