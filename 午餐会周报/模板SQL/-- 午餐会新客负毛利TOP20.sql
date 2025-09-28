-- 午餐会新客负毛利TOP20 
select
a.city_group_name  as `城市`,
a.customer_code  as `客户编码`,
a.customer_name as `客户名称`,
sales_value/10000 as `月至今销售额(万元)`,	 
 sz_profit/abs(sz_sales_value) as `第一周`,
 bz_profit/abs(bz_sales_value)  as `第二周`,
xz_profit/abs(xz_sales_value) as `第三周`
-- xxz_profit/abs(xxz_sales_value) as `第四周`
--  xxxz_profit/abs(xxxz_sales_value) as `第五周`
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
sum(case when a.sdt >='20231001'  and a.sdt <= '20231006'    then a.sale_amt end) sz_sales_value,
	sum(case when a.sdt >='20231001'  and a.sdt <= '20231006'   then a.profit end) sz_profit,
    sum(case when  a.sdt>='20231007'  and a.sdt <='20231013'    then a.sale_amt end) bz_sales_value,
    sum(case when a.sdt >='20231007'  and a.sdt <='20231013'    then a.profit end) bz_profit,
    sum(case when a.sdt >='20231014'  and a.sdt <='20231020'    then a.sale_amt end) xz_sales_value,
    sum(case when a.sdt >='20231014'  and a.sdt <='20231020'   then a.profit end) xz_profit
    -- sum(case when a.sdt >='20230923'  and a.sdt <='20230929'    then a.sale_amt end) xxz_sales_value,
    -- sum(case when a.sdt >='20230923'  and a.sdt <='20230929'   then a.profit end) xxz_profit
    -- sum(case when a.sdt >='20230826'  and a.sdt <='20230831'    then a.sale_amt end) xxxz_sales_value,
    -- sum(case when a.sdt >='20230826'  and a.sdt <='20230831'   then a.profit end) xxxz_profit	
  
  from
  (
    select * from csx_dws.csx_dws_sale_detail_di
  where  sdt >='20231001' and sdt<='20231020'
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
 where c.shop_code is null and d.first_sales_date='202310'

group by performance_province_code 
		,performance_province_name 
	    ,performance_city_code     
		,performance_city_name     
	,a.customer_code,d.customer_name)a
		) a		
WHERE ook<=20
order by  sales_value desc   limit 500000