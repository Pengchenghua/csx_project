-- 午餐会 老客环比情况
select
 a.region_code
,a.region_name
,a.province_code
,a.province_name
,a.city_group_code
,a.city_group_name
,count(c.customer_code) as customer_cn,
count(if(c.customer_code is not null and by_profit/abs(by_sales_value)-sy_profit/abs(sy_sales_value)<=-0.02 ,c.customer_code,null) ) as diff_profit_rate,
sum(by_sales_value),
sum(if(c.customer_code is not null,by_sales_value,0)),
sum(if(c.customer_code is not null,by_profit,0)),	
sum(if(c.customer_code is not null,by_profit,0))/abs(sum(if(c.customer_code is not null,by_sales_value,0))) as by_profit_rate,
sum(if(c.customer_code is not null,by_sales_value,0))/sum(by_sales_value) as by_sale_ratio,
sum(sy_sales_value),
sum(if(c.customer_code is not null,sy_sales_value,0)),
sum(if(c.customer_code is not null,sy_profit,0)),		 
sum(if(c.customer_code is not null,sy_profit,0))/abs(sum(if(c.customer_code is not null,sy_sales_value,0))),
sum(if(c.customer_code is not null,sy_sales_value,0))/sum(sy_sales_value)
from
(
  select
    performance_region_code        region_code
        ,performance_region_name        region_name
		,performance_province_code      province_code
		,performance_province_name      province_name
	    ,performance_city_code     city_group_code
		,performance_city_name     city_group_name,
	customer_code,
    sum(case when  a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sale_amt end) by_sales_value,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.profit end) by_profit,	
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.sale_amt end) sy_sales_value,
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.profit end) sy_profit	
  
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
 and    inventory_dc_code not in ('W0AJ','W0G6','WB71','W0J2') -- 3海军仓 和 监狱仓W0J2
 and performance_city_name not in ('南平市','三明市','宁德市','龙岩市','东北','黔江区','宁波市','台州市')
  group by  
         performance_region_code 
        ,performance_region_name   
		,performance_province_code 
		,performance_province_name 
	    ,performance_city_code     
		,performance_city_name     
		,customer_code
)a  
left join
 (select * from (select
province_code,
province_name,
customer_code,
row_number() over(partition by province_name order by sales_value desc) ook
from ( select
		performance_province_code      province_code
		,performance_province_name      province_name,

	a.customer_code,
    sum(sale_amt)as sales_value
  from
  (
    select * from csx_dws.csx_dws_sale_detail_di
  where  sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
  and business_type_code='1'
  ) a 
 left join (select distinct shop_code 
				from csx_dim.csx_dim_shop 
				where sdt='current'  and  shop_low_profit_flag=1   --  低毛利DC标识(1-是,0-否)
				)c
        on a.inventory_dc_code = c.shop_code
left join  -- 首单日期
(
  select customer_code,substr(first_business_sale_date,1,6) first_sales_date
  from csx_dws.csx_dws_crm_customer_business_active_di
  where sdt ='current' 
  and 	business_type_code=1
)d on d.customer_code=a.customer_code 
 where c.shop_code is null and d.first_sales_date<>substr(regexp_replace(trunc('${i_sdate}','MM'),'-',''),1,6)
 and    inventory_dc_code not in ('W0AJ','W0G6','WB71','W0J2') -- 3海军仓 和 监狱仓W0J2
 and performance_city_name not in 
	  ('南平市','三明市','宁德市','龙岩市','东北','黔江区','宁波市','台州市')
group by performance_province_code 
		,performance_province_name   
		,a.customer_code)a
		)c
where 	ook<=20

)c	on a.customer_code=c.customer_code

group by  a.region_code
,a.region_name
,a.province_code
,a.province_name
,a.city_group_code
,a.city_group_name;



-- 午餐会老客TOP20
select a.city_group_name as `城市`,
    a.customer_code as `客户编码`,
    a.customer_name as `客户名称`,
    by_sales_value / 10000 as `月至今销售额(万元)`,
    by_profit / abs(by_sales_value) as `月至今毛利率`,
    sy_profit / abs(sy_sales_value),
    by_profit / abs(by_sales_value) - sy_profit / abs(sy_sales_value) as `毛利率环比`,
    sz_profit / abs(sz_sales_value) as `第一周`,
    bz_profit / abs(bz_sales_value) as `第二周`,
    xz_profit / abs(xz_sales_value) as `第三周` 
    , xxz_profit/abs(xxz_sales_value) as `第四周` 
    -- ,  xxxz_profit/abs(xxxz_sales_value) as `第五周` 
    from (
        select a.*,
            row_number() over(
                order by by_sales_value desc
            ) ook
        from (
                select performance_region_code region_code,
                    performance_region_name region_name,
                    performance_province_code province_code,
                    performance_province_name province_name,
                    performance_city_code city_group_code,
                    performance_city_name city_group_name,
                    a.customer_code,
                    d.customer_name,
                    sum(
                        case
                            when a.sdt >='20231230'  and a.sdt <= '20240105' then a.sale_amt
                        end
                    ) sz_sales_value,
                    sum(
                        case
                            when sdt >='20231230'  and a.sdt <= '20240105' then a.profit
                        end
                    ) sz_profit,
                    sum(
                        case
                            when sdt>='20231223'  and a.sdt <= '20231229' then a.sale_amt
                        end
                    ) bz_sales_value,
                    sum(
                        case
                            when a.sdt >='20231223'  and a.sdt <= '20231229' then a.profit
                        end
                    ) bz_profit,
                    sum(
                        case
                            when a.sdt >='20231216'  and a.sdt <= '20231222' then a.sale_amt
                        end
                    ) xz_sales_value,
                    sum(
                        case
                            when a.sdt >='20231216'  and a.sdt <= '20231222' then a.profit
                        end
                    ) xz_profit,
                     sum(case when a.sdt >='20231209' and a.sdt <='20231215'    then a.sale_amt end) xxz_sales_value,   
                     sum(case when a.sdt >='20231209' and a.sdt <='20231215'   then a.profit end) xxz_profit,     
                    sum(case when  a.sdt >= regexp_replace(trunc('2024-01-07','MM'),'-','') and a.sdt <= regexp_replace(add_months('2024-01-07',0),'-','') then a.sale_amt end) by_sales_value, 	
                    sum(case when a.sdt >= regexp_replace(trunc('2024-01-07','MM'),'-','') and a.sdt <= regexp_replace(add_months('2024-01-07',0),'-','') then a.profit end) by_profit,	 	
                    sum(case when a.sdt >= regexp_replace(add_months(trunc('2024-01-07','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('2024-01-07',-1),'-','') then a.sale_amt end) sy_sales_value, 	
                    sum(case when a.sdt >= regexp_replace(add_months(trunc('2024-01-07','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('2024-01-07',-1),'-','') then a.profit end) sy_profit	      
                    from   (     select *  	from csx_dws.csx_dws_sale_detail_di     
                    where  sdt >=regexp_replace(add_months(trunc('2024-01-07','MM'),-1),'-','')   	  
                    and sdt<= regexp_replace(add_months('2024-01-07',0),'-','')       
                    and business_type_code='1' 	  and channel_code in('1','9')       
                    and inventory_dc_code not in ('W0AJ','W0G6','WB71','W0J2') 
                    -- 3海军仓 和 监狱仓W0J2       
                    and performance_city_name not in  	  ('南平市','三明市','宁德市','龙岩市','东北','黔江区','宁波市','台州市')   
                     ) a   
                     left join 
                     (  select distinct shop_code   from csx_dim.csx_dim_shop  			 
                     where sdt='current'  and  shop_low_profit_flag=1   
                     --  低毛利DC标识(1-是,0-否) 			
                     )c         
                     on a.inventory_dc_code = c.shop_code 
                     left join  
                     -- 首单日期 
                     (   select customer_code,customer_name,substr(first_business_sale_date,1,6) first_sales_date   
                     from csx_dws.csx_dws_crm_customer_business_active_di  
                      where sdt ='current' and 	business_type_code=1 )d on d.customer_code=a.customer_code   
                      where c.shop_code is null 
                      and d.first_sales_date<substr(regexp_replace(trunc('2024-01-07','MM'),'-',''),1,6)   
                      group by performance_region_code            
                      ,performance_region_name    		
                      ,performance_province_code  		
                      ,performance_province_name  	    
                      ,performance_city_code      		
                      ,performance_city_name      	
                      ,a.customer_code,d.customer_name)a 
                      WHERE    by_profit/abs(by_sales_value)-sy_profit/abs(sy_sales_value)<=-0.02  		) a		 
                      WHERE ook<=20 ;

-- 下滑2%客户明细


select
ook,
 a.region_code
,a.region_name
,a.province_code
,a.province_name
,a.city_group_code
,a.city_group_name
,c.customer_code,
customer_name,
nvl(f.fir_price_type,e.price_type1) as price_type1, -- 定价类型1
	nvl(f.sec_price_type,e.price_type2) as price_type2, -- 定价类型2
	e.price_period_name, -- 报价周期
	e.price_date_name, -- 报价日
count(if(c.customer_code is not null and by_profit/abs(by_sales_value)-sy_profit/abs(sy_sales_value)<=-0.02 ,c.customer_code,null) ) as diff_profit_rate,
sum(by_sales_value),
sum(if(c.customer_code is not null,by_sales_value,0)),
sum(if(c.customer_code is not null,by_profit,0)),	
sum(if(c.customer_code is not null,by_profit,0))/abs(sum(if(c.customer_code is not null,by_sales_value,0))) as by_profit_rate,
sum(if(c.customer_code is not null,by_sales_value,0))/sum(by_sales_value) as by_sale_ratio,
sum(sy_sales_value),
sum(if(c.customer_code is not null,sy_sales_value,0)),
sum(if(c.customer_code is not null,sy_profit,0)),		 
sum(if(c.customer_code is not null,sy_profit,0))/abs(sum(if(c.customer_code is not null,sy_sales_value,0))),
sum(if(c.customer_code is not null,sy_sales_value,0))/sum(sy_sales_value)
from
(
  select
    performance_region_code        region_code
        ,performance_region_name        region_name
		,performance_province_code      province_code
		,performance_province_name      province_name
	    ,performance_city_code     city_group_code
		,performance_city_name     city_group_name,
	customer_code,
    sum(case when  a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sale_amt end) by_sales_value,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.profit end) by_profit,	
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.sale_amt end) sy_sales_value,
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.profit end) sy_profit	
  
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
 and    inventory_dc_code not in ('W0AJ','W0G6','WB71','W0J2') -- 3海军仓 和 监狱仓W0J2
 and performance_city_name not in ('南平市','三明市','宁德市','龙岩市','东北','黔江区','宁波市','台州市')
  group by  
         performance_region_code 
        ,performance_region_name   
		,performance_province_code 
		,performance_province_name 
	    ,performance_city_code     
		,performance_city_name     
		,customer_code
)a  
 join
 (select * from (select
province_code,
province_name,
customer_code,
customer_name,
row_number() over(partition by province_name order by sales_value desc) ook
from ( select
		performance_province_code      province_code
		,performance_province_name      province_name,

	a.customer_code,
	customer_name,
    sum(sale_amt)as sales_value
  from
  (
    select * from csx_dws.csx_dws_sale_detail_di
  where  sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
  and business_type_code='1'
  ) a 
 left join (select distinct shop_code 
				from csx_dim.csx_dim_shop 
				where sdt='current'  and  shop_low_profit_flag=1   --  低毛利DC标识(1-是,0-否)
				)c
        on a.inventory_dc_code = c.shop_code
left join  -- 首单日期
(
  select customer_code,substr(first_business_sale_date,1,6) first_sales_date
  from csx_dws.csx_dws_crm_customer_business_active_di
  where sdt ='current' 
  and 	business_type_code=1
)d on d.customer_code=a.customer_code 
 where c.shop_code is null and d.first_sales_date<>substr(regexp_replace(trunc('${i_sdate}','MM'),'-',''),1,6)
 and    inventory_dc_code not in ('W0AJ','W0G6','WB71','W0J2') -- 3海军仓 和 监狱仓W0J2
 and performance_city_name not in 
	  ('南平市','三明市','宁德市','龙岩市','东北','黔江区','宁波市','台州市')
group by performance_province_code 
		,performance_province_name   
		,a.customer_code
		,customer_name )a
		)c
where 	1=1 
 -- and ook<=20

)c	on a.customer_code=c.customer_code
left join  -- 线上客户定价类型
	csx_analyse_tmp.csx_analyse_tmp_customer_price_type_ky e 
	on a.customer_code=e.customer_code 
	left join  -- 线下客户定价类型
	dev.csx_ods_data_analysis_prd_cus_price_type_231206_df f  
	on a.customer_code=f.customer_code 
group by ook, a.region_code
,a.region_name
,a.province_code
,a.province_name
,a.city_group_code
,a.city_group_name
,c.customer_code
,c.customer_name
,nvl(f.fir_price_type,e.price_type1) , -- 定价类型1
	nvl(f.sec_price_type,e.price_type2) , -- 定价类型2
	e.price_period_name, -- 报价周期
	e.price_date_name -- 报价日
	;






 -- 检查

                select performance_region_code region_code,
                    performance_region_name region_name,
                    performance_province_code province_code,
                    performance_province_name province_name,
                    performance_city_code city_group_code,
                    performance_city_name city_group_name,
                    a.customer_code,
                    a.customer_name,
                    sum(case when sdt >='20230101' and sdt <='20230107' then sale_amt end ) sale_1,
                    sum(case when sdt >='20230101' and sdt <='20230114' then sale_amt end ) sale_2,
                    sum(case when sdt >='20240101' and sdt <='20240107' then sale_amt end ) sale_3,
                    sum(case when sdt >='20240101' and sdt <='20240114' then sale_amt end ) sale_4
                    
                 	from csx_dws.csx_dws_sale_detail_di  a
                    where  1=1 
                    and performance_region_name='华北大区'
                    and business_type_code=1
--                     and customer_code in ('111608',
-- '106775',
-- '107428',
-- '129671',
-- '129581'
-- )
                    group by performance_region_code ,
                    performance_region_name ,
                    performance_province_code ,
                    performance_province_name ,
                    performance_city_code ,
                    performance_city_name ,
                    a.customer_code,
                    a.customer_name