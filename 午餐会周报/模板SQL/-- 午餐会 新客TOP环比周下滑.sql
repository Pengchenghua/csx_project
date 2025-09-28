-- 午餐会 新客TOP环比周下滑



------------------------- 全国新老客毛利（月至今）----------------------

select
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  a.city_group_code,
  a.city_group_name,
    sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sales_value end) sales_value,
	sum(case when  a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.profit end) profit,	
    sum(case when substr(c.first_sales_date,1,6)=substr(a.sdt,1,6)  and a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sales_value end) xby_sales_value,
	sum(case when substr(c.first_sales_date,1,6)=substr(a.sdt,1,6)  and a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.profit end) xby_profit,	
	sum(case when substr(c.first_sales_date,1,6)=substr(a.sdt,1,6)  and a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.sales_value end) xsy_sales_value,
	sum(case when substr(c.first_sales_date,1,6)=substr(a.sdt,1,6)  and a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.profit end) xsy_profit,		
	sum(case when substr(c.first_sales_date,1,6)<substr(a.sdt,1,6)  and a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sales_value end) by_sales_value,
	sum(case when substr(c.first_sales_date,1,6)<substr(a.sdt,1,6)  and a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.profit end) by_profit,	
	sum(case when substr(c.first_sales_date,1,6)<substr(a.sdt,1,6)  and a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.sales_value end) sy_sales_value,
	sum(case when substr(c.first_sales_date,1,6)<substr(a.sdt,1,6)  and a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.profit end) sy_profit	
from
(
  select
   sdt,
    performance_region_code        region_code
        ,performance_region_name        region_name
		,performance_province_code      province_code
		,performance_province_name      province_name
	    ,performance_city_code     city_group_code
		,performance_city_name     city_group_name,
	customer_code,
    sum(sale_amt)as sales_value,
    sum(profit)as profit
  from
  (
    select * from csx_dws.csx_dws_sale_detail_di
  where   sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') 
  and sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') 
  and business_type_code='1'
  ) a 
 left join (select distinct shop_code 
				from csx_dim.csx_dim_shop 
				where sdt='current'  and  shop_low_profit_flag=1   --  低毛利DC标识(1-是,0-否)
				)c
        on a.inventory_dc_code = c.shop_code
 where c.shop_code is null
 and inventory_dc_code not in ('W0AJ','W0G6','WB71','W0J2') 
                    -- 3海军仓 和 监狱仓W0J2       
  -- and performance_city_name not in  	  ('南平市','三明市','宁德市','龙岩市','东北','黔江区','宁波市','台州市')   
  group by  sdt,
         performance_region_code 
        ,performance_region_name   
		,performance_province_code 
		,performance_province_name 
	    ,performance_city_code     
		,performance_city_name     
		,customer_code
)a
left join  -- 首单日期
(
  select customer_code,min(first_business_sale_date) first_sales_date
  from csx_dws.csx_dws_crm_customer_business_active_di
  where sdt ='current' and 	business_type_code=1
  group by customer_code
)c on c.customer_code=a.customer_code 
group by  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  a.city_group_code,
  a.city_group_name

 ;
  
  
   
  
   
  
select
a.city_group_name  as `城市`,
a.customer_code  as `客户编码`,
a.customer_name as `客户名称`,
sales_value/10000 as `月至今销售额(万元)`,	 
 sz_profit/abs(sz_sales_value) as `第一周`,
 bz_profit/abs(bz_sales_value)  as `第二周`,
xz_profit/abs(xz_sales_value) as `第三周`
 ,xxz_profit/abs(xxz_sales_value) as `第四周`
 , xxxz_profit/abs(xxxz_sales_value) as `第五周`
from
(select
a.*,
row_number() over(order by sales_value desc) ook
from ( 
select
	
performance_province_code      province_code
	,performance_province_name      province_name
	,performance_city_code     city_group_code
	,performance_city_name     city_group_name,
	a.customer_code,
	d.customer_name,
	sum(a.sale_amt) sales_value,
sum(case when a.sdt >='20231101'  and a.sdt <= '20231103'    then a.sale_amt end) sz_sales_value,
	sum(case when a.sdt >='20231101'  and a.sdt <= '20231103'   then a.profit end) sz_profit,
    sum(case when  a.sdt>='20231104'  and a.sdt <='20231110'    then a.sale_amt end) bz_sales_value,
    sum(case when a.sdt >='20231104'  and a.sdt <='20231110'    then a.profit end) bz_profit,
    sum(case when a.sdt >='20231111'  and a.sdt <='20231117'    then a.sale_amt end) xz_sales_value,
    sum(case when a.sdt >='20231111'  and a.sdt <='20231117'   then a.profit end) xz_profit,
    sum(case when a.sdt >='20231118'  and a.sdt <='20231124'    then a.sale_amt end) xxz_sales_value,
    sum(case when a.sdt >='20231118'  and a.sdt <='20231124'   then a.profit end) xxz_profit
    ,sum(case when a.sdt >='20231125'  and a.sdt <='20231201'    then a.sale_amt end) xxxz_sales_value,
    sum(case when a.sdt >='20231125'  and a.sdt <='20231201'   then a.profit end) xxxz_profit	
  
  from
  (
    select * from csx_dws.csx_dws_sale_detail_di
  where  sdt >='20231101' and sdt<='20231130'
  and business_type_code='1' and channel_code in('1','9')
  and inventory_dc_code not in ('W0AJ','W0G6','WB71','W0J2') -- 3海军仓 和 监狱仓W0J2
  and performance_city_name not in ('南平市',
'三明市',
'宁德市',
'龙岩市',
'东北',
'黔江区',
'宁波市',
'台州市')
  ) a 
 left join (select distinct shop_code 
				from csx_dim.csx_dim_shop 
				where sdt='current'  and  shop_low_profit_flag=1   --  低毛利DC标识(1-是,0-否)
				)c
        on a.inventory_dc_code = c.shop_code
left join  -- 首单日期
(
  select customer_code,customer_name,substr(first_business_sale_date,1,6) first_sales_date
  from csx_dws.csx_dws_crm_customer_business_active_di
  where sdt ='current' and 	business_type_code=1
)d on d.customer_code=a.customer_code 
 where c.shop_code is null and d.first_sales_date='202311'
group by performance_province_code 
		,performance_province_name 
	    ,performance_city_code     
		,performance_city_name     
	,a.customer_code,d.customer_name)a
		) a		
WHERE ook<=20
order by  sales_value desc 
;


-- 日配业务负毛利客户
select region_name, province_name,city_group_name,customer_name ,
sales_value,	profit,	prorate,	prorate_d90,	prorate_diff,	sku
from 
	(select *,case when profit <-100 then '是' else '否' end f_profit,
	if(sales_value<0,'是','否') f_amt
	from csx_analyse.csx_analyse_fr_customer_new_old_di ) a
	where sdt='20231029'
	and f_profit='是'
	and customer_name not like '%日用品%'
	and f_amt='否'
	order by profit asc


当前周：20231230- 20240105
第二周：20231223- 20231229
第三周：20231216- 20231222 
第四周：20231209- 20231215
