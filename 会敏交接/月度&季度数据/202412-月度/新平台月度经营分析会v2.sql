01：P8P9P19明细数据
02:日配客户数sheet页
03：p17明细 更新后记得隐藏

--月度 省区维度
 -- -- -- -- -课组数据 01 ,'WB26' -- p8p9p10明细数据
select 
	  a.performance_region_name,
	  a.performance_province_code,
	  a.performance_province_name,
	  a.smonth,
      b.classify_middle_name
     ,count(distinct a.customer_code) custno
     ,sum(a.sale_amt) sale_amt
     ,sum(a.profit) profit
from
	(
	select 
		performance_region_code,
		performance_region_name,
		a.performance_province_code,
		a.performance_province_name,
		a.performance_city_code,
		a.performance_city_name,
		a.customer_code,
		a.goods_code,
		substr(a.sdt,1,6) smonth,
        sum(a.sale_amt) sale_amt,
        sum(a.profit) profit
	from  csx_dws.csx_dws_sale_detail_di a -- -fengcun
	where 
		a.sdt>='20240101' 
		and a.sdt<='20241231'  
		and a.inventory_dc_code not in ('W0K4','W0Z7','WB38','WB26') 
		-- and a.channel_code in ('1','9') 
		and business_type_code=1 -- -日配
	group by 
		performance_region_code,
		performance_region_name,
		a.performance_province_code,
		a.performance_province_name,
		a.customer_code,
		a.goods_code,
		a.performance_city_code,
		a.performance_city_name,
		substr(a.sdt,1,6)
	)a
left join (select * from csx_dim.csx_dim_basic_goods where sdt='current') b on b.goods_code=a.goods_code
group by 
	a.performance_region_name,
	a.performance_province_name,
	a.smonth,
	b.classify_middle_name;

	-- -- -用户数 02 ,'WB26',日配客户数
	select
		-- performance_region_code,
		performance_region_name,
		-- a.performance_province_code,
		performance_province_name,
		substr(sdt,1,6) smonth,
		count(distinct customer_code) custno
	from  csx_dws.csx_dws_sale_detail_di  -- -fengcun
	where 
		sdt>='20240101' 
		and sdt<='20241231'
		and inventory_dc_code not in ('W0K4','W0Z7','WB38','WB26')
		-- and channel_code in ('1','9')  
		and business_type_code=1
	group by    
		performance_region_name,
		performance_province_name,
		substr(sdt,1,6) ;


-- -- -- -各省区月度复盘数据
-- -新签履约客户与新履约客户 03

SELECT 
	a.performance_province_code
	,a.performance_province_name
	,a.smonth
	,sum(case when a.channel_code in (1,9) then sale_amt end) b_sales_value  
	,sum(case when a.channel_code in (1,9) and substr(aa.first_sale_date,1,6)=a.smonth then a.sale_amt end) xin_sales_value
	,ab.xinqkh
	,count(distinct case when a.channel_code in (1,9) and substr(aa.first_sale_date,1,6)=a.smonth then a.customer_code end) xin_sales_value  
	,sum(case when a.channel_code in (1,9) and substr(aa.first_sale_date,1,6)<a.smonth then a.sale_amt end) old_sales_value
	,count(distinct case when a.channel_code in (1,9) and substr(aa.first_sale_date,1,6)<a.smonth then a.customer_code end) old_sales_cnt
	,avg(case when a.channel_code in (1,9) and substr(aa.first_sale_date,1,6)<a.smonth then a.sale_amt end) old_sales_value
	,sum(case when a.channel_code=7 then sale_amt end) bbc_sales_value 
	,sum(case when a.channel_code=2 then sale_amt end) m_sales_value 
	,sum(case when a.channel_code in (1,9) then profit end) b_profit
	,sum(case when a.channel_code=7 then profit end) bbc_profit 
	,sum(case when a.channel_code=2 then profit end) m_profit 
	,performance_region_name
 from 
	(
	select performance_region_name,
		performance_province_code,
       performance_province_name,
       channel_code,
	   substr(sdt,1,6) smonth,
	   customer_code,sign_time,
	   sum(profit) profit ,
	   sum(sale_amt) sale_amt
	from  csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20240101' 
		and sdt<='20241231'
		and channel_code in (1,9,2,7) 
	group by 
		performance_province_code,
        performance_province_name,
        channel_code,sign_time,
	    substr(sdt,1,6),
	    customer_code,
		performance_region_name
	) a
left join 
	(
	select 
		b.performance_province_code,
		substr(a.sign_date,1,6) smonth,
		count(distinct a.customer_code) xinqkh 
	from 
		(select * from csx_dws.csx_dws_crm_customer_active_di 
		 where sdt='current' 
			and sign_date>='20240101' 
			and sign_date<='20241231'
		)a 
	left join 
		(select * from csx_dim.csx_dim_crm_customer_info where sdt='current')b on a.customer_code=b.customer_code
	group by 
		b.performance_province_code,
		substr(a.sign_date,1,6)
	)ab on a.performance_province_code = ab.performance_province_code  and  a.smonth=ab.smonth
left join 
	(
	 select 
		customer_code,
		first_sale_date 
	 from csx_dws.csx_dws_crm_customer_active_di 
	 where sdt='current' 
	)aa on a.customer_code = aa.customer_code
group by 
	a.performance_province_code
    ,a.performance_province_name
	,ab.xinqkh
	,a.smonth,
	performance_region_name;

	   
月度 城市维度-- 福建省与重庆市

-- -- -- -- -课组数据 01
select 
	a.performance_province_code,
	a.performance_province_name,
	performance_city_code,
	performance_city_name,
	a.smonth,
    b.classify_middle_name
    ,count(distinct a.customer_code) custno
    ,sum(a.sale_amt) sale_amt
    ,sum(a.profit) profit
    -- sum(case when smonth='202006' then a.sale_amt end) sales_value_m
from
	(
	select 
		performance_city_code,
		performance_city_name,
		a.performance_province_code,
		a.performance_province_name,
		a.customer_code,
		a.goods_code,
		substr(a.sdt,1,6) smonth,
        sum(a.sale_amt) sale_amt,
        sum(a.profit) profit
	from  csx_dws.csx_dws_sale_detail_di a -- -fengcun
	where a.sdt>='20240101' 
		and a.sdt<='20241231'  
		and performance_province_name in ('福建省','重庆市') 
		and a.inventory_dc_code<>'W0K4' 
		-- and a.channel_code in ('1','9') 
		and business_type_code=1 -- -日配
	group by 
		performance_city_code,
		performance_city_name,
		a.performance_province_code,
		a.performance_province_name,
		a.customer_code,
		a.goods_code,
		substr(a.sdt,1,6)
	)a
left join (select * from csx_dim.csx_dim_basic_goods where sdt='current') b on b.goods_code=a.goods_code
group by 
	performance_city_code,
	performance_city_name,
	a.performance_province_code,
	a.performance_province_name,
	a.smonth,
	b.classify_middle_name

-- -- -用户数 02
select 
	a.performance_province_code,
	a.performance_province_name,
	performance_city_code,
	performance_city_name,
	substr(a.sdt,1,6) smonth,
    count(distinct a.customer_code) custno
from  csx_dws.csx_dws_sale_detail_di a 
where a.sdt>='20240101' 
     and a.sdt<='20241231' 
	 and performance_province_name in ('福建省','重庆市') 
	 and a.inventory_dc_code<>'W0K4'
	 -- and a.channel_code in ('1','9')  
	 and business_type_code=1
group by  
	a.performance_province_code,
	a.performance_province_name,
	performance_city_code,
	performance_city_name,
	substr(a.sdt,1,6)


-- -新签履约客户与新履约客户 03

SELECT 
	a.performance_city_code,
	a.performance_city_name,
	a.smonth,
	sum(case when a.channel_code in (1,9) then sale_amt end) b_sales_value  
	,sum(case when a.channel_code in (1,9) and substr(aa.first_sale_date,1,6)=a.smonth then a.sale_amt end) xin_sales_value
	,ab.xinqkh
	,count(distinct case when a.channel_code in (1,9) and substr(aa.first_sale_date,1,6)=a.smonth then a.customer_code end) xin_sales_value  
	,sum(case when a.channel_code in (1,9) and substr(aa.first_sale_date,1,6)<a.smonth then a.sale_amt end) old_sales_value
	,count(distinct case when a.channel_code in (1,9) and substr(aa.first_sale_date,1,6)<a.smonth then a.customer_code end) old_sales_cnt
	,avg(case when a.channel_code in (1,9) and substr(aa.first_sale_date,1,6)<a.smonth then a.sale_amt end) old_sales_value
	,sum(case when a.channel_code=7 then sale_amt end) bbc_sales_value 
	,sum(case when a.channel_code=2 then sale_amt end) m_sales_value 
	,sum(case when a.channel_code in (1,9) then profit end) b_profit
	,sum(case when a.channel_code=7 then profit end) bbc_profit 
	,sum(case when a.channel_code=2 then profit end) m_profit ,
	performance_province_name
 from 
	(
	select 
		performance_province_name,

		performance_city_code,
		performance_city_name,
		channel_code,
		substr(sdt,1,6) smonth,
		customer_code,sign_time,
		sum(profit) profit ,
		sum(sale_amt) sale_amt
	from  csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20240101' 
		and sdt<='20241231'
		and channel_code in (1,9,2,7) 
	group by 
		performance_city_code,performance_city_name,
        channel_code,sign_time,
		substr(sdt,1,6),
		customer_code,
		performance_province_name
    ) a
left join 
	(
	select 
		b.performance_city_code,
		substr(a.sign_date,1,6) smonth,
		count(distinct a.customer_code) xinqkh 
    from csx_dws.csx_dws_crm_customer_active_di a 
	left join 
		(select * from csx_dim.csx_dim_crm_customer_info where sdt='current')b on a.customer_code=b.customer_code
    where a.sdt='current' 
		and a.sign_date>='20240101' 
		and a.sign_date<='20241231'
	group by 
		b.performance_city_code,
		substr(a.sign_date,1,6)
	)ab on a.performance_city_code = ab.performance_city_code  and  a.smonth=ab.smonth
left join 
	(
	select 
		customer_code,
		first_sale_date 
	from csx_dws.csx_dws_crm_customer_active_di 
	where sdt='current' 
	)aa on a.customer_code = aa.customer_code
group by 
	a.performance_city_code,
	a.performance_city_name,
	a.smonth,
	ab.xinqkh,
	performance_province_name;
	   

	   
 -- -- -- -- -- -- -- -- -- -- -- -- 明细数据
	   	   
-- -- -- -- -- -- -客户明细 04 --只跑一个月的
SELECT 
    a.performance_region_name,
   a.performance_province_name,
   a.performance_city_name,
   a.channel_name, 
   a.customer_code,
   a.customer_name,
   substr(a.sdt,1,6) s_month,
   a.business_type_name, 
   c.first_sale_date,
   case when substr(a.sdt,1,6)=substr(c.first_sale_date,1,6) then '新履约'
         else '老客' end n_type,
   SUM(sale_amt) sale_amt,
   SUM(profit)  profit,
   b.first_category_name,
   b.second_category_name,
   b.third_category_name
FROM  csx_dws.csx_dws_sale_detail_di a
left join (select * from csx_dim.csx_dim_crm_customer_info where sdt='current')b on a.customer_code=b.customer_code
left join (select customer_code,first_sale_date from csx_dws.csx_dws_crm_customer_active_di where sdt='current' )c on a.customer_code = c.customer_code
where a.sdt>='20241201' 
	and a.sdt<='20241231'
	and a.channel_code IN ('1','9','7')
group by   
   a.performance_region_name,
   a.performance_province_name,
   a.performance_city_name,
   a.channel_name, 
   a.customer_code,
   a.customer_name,
   substr(a.sdt,1,6),
   a.business_type_name,  
   c.first_sale_date,
   case when substr(a.sdt,1,6)=substr(c.first_sale_date,1,6) then '新履约'
         else '老客' end,b.first_category_name,
   b.second_category_name,
   b.third_category_name
   ;

-- -- -- -- -- 管理中类明细	05   ,'WB26'
select 
	performance_region_code
	,a.performance_region_name
	,a.performance_province_code
	,a.performance_province_name
	,performance_city_name
	,substr(a.sdt,1,6) smonth
    ,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name,goods_code,goods_name
	,a.customer_code,customer_name,
    sum(a.sale_amt) sale_amt,
    sum(a.profit) profit
 from csx_dws.csx_dws_sale_detail_di a -- -fengcun
 
   left join ( 
	            select
                   distinct shop_code 
				from csx_dim.csx_dim_shop 
				where sdt='current' and shop_low_profit_flag=1  
			  )c      on a.inventory_dc_code = c.shop_code
WHERE c.shop_code is null 
	and  a.sdt>='20241201' 
    and a.sdt<='20241231' 
	and a.inventory_dc_code not in ('W0K4','W0Z7','WB38','WB26')
	-- and a.channel_code in ('1','7','9') 
	and  business_type_code=1 -- -日配
group by 
performance_region_code
,a.performance_region_name 
,a.performance_province_code
,a.performance_province_name
,performance_city_name,
substr(a.sdt,1,6),
     classify_middle_code,
	 classify_middle_name,
	 classify_small_code,
	 classify_small_name,
	 goods_code,goods_name
	 ,a.customer_code,customer_name
	;
	 
-- -- -- -duanyu断约客户 06
select 	
	performance_province_name, 
	after_month, 
	a.customer_code,
	c.customer_name
from
	(
	select 
		customer_code,
		substr(regexp_replace(cast(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),90) as string),'-',''),1,6) as after_month
	from 
		csx_dws.csx_dws_sale_detail_di 
	where 
		sdt between '20240101' and '20241231'
		and business_type_code=1 --  业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and channel_code in('1','7','9') --  渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and order_channel_code not in (4,6)
		
	group by 
		customer_code
	) a
left join   
	(
	select *
	from csx_dim.csx_dim_crm_customer_info
	where sdt= 'current'
	and channel_code  in ('1','7','9')
	) c on a.customer_code=c.customer_code 
where after_month ='202405'
group by 
	performance_province_name, 
	after_month, 
	a.customer_code,
	c.customer_name;

			
			
			
--======================================================================================================================================
	   
	   
-- -季度省区
 -- -- -- -- -课组数据 01,'WB26'
select a.performance_region_code,
	a.performance_region_name,
    a.performance_province_code,
	a.performance_province_name,
	new_quarter as smonth,
    b.classify_middle_name
    ,count(distinct a.customer_code) custno
    ,sum(a.sale_amt) sale_amt
    ,sum(a.profit) profit
from
(select a.sdt,
        performance_region_code,
		performance_region_name,
		a.performance_province_code,
		a.performance_province_name,
		a.customer_code,
		a.goods_code,
        sum(a.sale_amt) sale_amt,
        sum(a.profit) profit
 from  csx_analyse.csx_analyse_bi_sale_detail_di a -- -fengcun
WHERE a.sdt>='20230101' 
     AND a.sdt<'20240701'  
     and a.inventory_dc_code not in ('W0K4','W0Z7','WB38','WB26')
    and a.channel_code in ('1','9') 
    and   business_type_code=1 -- - 日配
    and performance_region_code='4'
group by sdt,
    performance_region_code,
    performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.customer_code,
    a.goods_code
)a
left join (select * from csx_dim.csx_dim_basic_goods where sdt='current') b on b.goods_code=a.goods_code
left join (select * , concat_ws('Q',year,substr(quarter,-1,1)) as new_quarter from csx_dim.csx_dim_basic_date ) c on a.sdt=c.calday
group by a.performance_region_code,
a.performance_region_name,
		a.performance_province_code,
		a.performance_province_name ,
		b.classify_middle_name,
		new_quarter;


				 
-- -- -用户数 02 ,'WB26'
select performance_region_code,
		performance_region_name,
		a.performance_province_code,
		a.performance_province_name, 
	 	new_quarter as smonth,
      	count(distinct a.customer_code) custno	 
 from  csx_dws.csx_dws_sale_detail_di a -- -fengcun
 left join (select * , concat_ws('Q',year,substr(quarter,-1,1)) as new_quarter from csx_dim.csx_dim_basic_date ) c on a.sdt=c.calday

WHERE a.sdt>='20230101' 
     AND a.sdt<'20240701'  
	 and a.inventory_dc_code  not in ('W0K4','W0Z7','WB38','WB26')
 	 and a.channel_code in ('1','9')  
	 and   business_type_code=1
group by  performance_region_code,
		performance_region_name,
		a.performance_province_code,
		a.performance_province_name,
		new_quarter;
	

-- -新签履约客户与新履约客户 03

SELECT a.performance_province_code
       ,a.performance_province_name
	   ,a.smonth
       ,sum(case when a.channel_code in (1,9) then sale_amt end) b_sales_value  
	   ,sum(case when a.channel_code in (1,9) and aa.first_sale_date=a.smonth then a.sale_amt end) xin_sales_value
	   ,ab.xinqkh
	   ,count(distinct case when a.channel_code in (1,9) and aa.first_sale_date=a.smonth then a.customer_code end) xin_sales_value  
	   ,sum(case when a.channel_code in (1,9) and aa.first_sale_date<a.smonth then a.sale_amt end) old_sales_value
	   ,count(distinct case when a.channel_code in (1,9) and aa.first_sale_date<a.smonth then a.customer_code end) old_sales_cnt
	   ,avg(case when a.channel_code in (1,9) and aa.first_sale_date<a.smonth then a.sale_amt end) old_sales_value
	   ,sum(case when a.channel_code=7 then sale_amt end) bbc_sales_value 
	   ,sum(case when a.channel_code=2 then sale_amt end) m_sales_value 
	   ,sum(case when a.channel_code in (1,9) then profit end) b_profit
	   ,sum(case when a.channel_code=7 then profit end) bbc_profit 
	   
	   ,sum(case when a.channel_code=2 then profit end) m_profit 
 from (select performance_province_code,
       performance_province_name,
       channel_code,
	   '202303'   smonth,
	   customer_code,sign_time,
	   sum(profit) profit ,
	   sum(sale_amt) sale_amt
	   FROM  csx_dws.csx_dws_sale_detail_di
	   
  WHERE sdt>='20230701' 
     AND sdt<'20240201'

     AND channel_code IN (1,9,2,7) 
   group by performance_province_code,
       performance_province_name,
       channel_code,sign_time,
	   customer_code) a
left join (select b.performance_province_code,
				'202303'  smonth,
	              count(distinct a.customer_code) xinqkh 
			from csx_dws.csx_dws_crm_customer_active_di a 
			left join (select customer_code,performance_province_code from csx_dim.csx_dim_crm_customer_info where sdt='current')b on a.customer_code=b.customer_code
              where sdt='current' and sign_date>='20230701' and sign_date<'20240201'
			  group by b.performance_province_code)ab 
		ON a.performance_province_code = ab.performance_province_code and a.smonth=ab.smonth

left join (select customer_code,if(first_sale_date>='20230701' and  first_sale_date<'20240201','202303','202302') first_sale_date
		  from csx_dws.csx_dws_crm_customer_active_di 
		  where sdt='current' and  first_sale_date<'20240201')aa ON a.customer_code = aa.customer_code
group by a.performance_province_code
       ,a.performance_province_name
	   ,a.smonth,ab.xinqkh;
	   
-- -半年省区
 -- -- -- -- -课组数据 01,'WB26'
select a.performance_region_code,a.performance_region_name,
     a.performance_province_code,a.performance_province_name,
	if(sdt='2023','2023上半年','2022下半年') as smonth,
      b.classify_middle_name
     ,count(distinct a.customer_code) custno
     ,sum(a.sale_amt) sale_amt
     ,sum(a.profit) profit
from
(select performance_region_code,performance_region_name,a.performance_province_code,a.performance_province_name,a.customer_code,a.goods_code,
       substr(sdt,1,4) sdt,
        sum(a.sale_amt) sale_amt,
        sum(a.profit) profit
 from  csx_dws.csx_dws_sale_detail_di a -- -fengcun
WHERE a.sdt>='20220701' 
     AND a.sdt<'20240201'  and a.inventory_dc_code not in ('W0K4','W0Z7','WB38','WB26')
 and a.channel_code in ('1','9') and   business_type_code=1 -- - 日配
group by performance_region_code,performance_region_name,a.performance_province_code,a.performance_province_name,a.customer_code,a.goods_code,sdt
)a
left join (select * from csx_dim.csx_dim_basic_goods where sdt='current') b on b.goods_code=a.goods_code

group by a.performance_region_code,a.performance_region_name,
				a.performance_province_code,a.performance_province_name,smonth ,
				 b.classify_middle_name;
				 
-- -- -用户数 02 ,'WB26'
select performance_region_code,performance_region_name,a.performance_province_code,a.performance_province_name, 
	 if(substr(sdt,1,4)='2023','2023上半年','2022下半年') as smonth,
         count(distinct a.customer_code) custno	 
 from  csx_dws.csx_dws_sale_detail_di a -- -fengcun
WHERE a.sdt>='20220701' 
     AND a.sdt<'20240201'  and a.inventory_dc_code  not in ('W0K4','W0Z7','WB38','WB26')
 and a.channel_code in ('1','9')  and   business_type_code=1
group by  performance_region_code,performance_region_name,a.performance_province_code,a.performance_province_name,smonth;
	

-- -新签履约客户与新履约客户 03

SELECT a.performance_province_code
       ,a.performance_province_name
	   ,a.smonth
       ,sum(case when a.channel_code in (1,9) then sale_amt end) b_sales_value  
	   ,sum(case when a.channel_code in (1,9) and aa.first_sale_date=a.smonth then a.sale_amt end) xin_sales_value
	   ,ab.xinqkh
	   ,count(distinct case when a.channel_code in (1,9) and aa.first_sale_date=a.smonth then a.customer_code end) xin_sales_value  
	   ,sum(case when a.channel_code in (1,9) and aa.first_sale_date<a.smonth then a.sale_amt end) old_sales_value
	   ,count(distinct case when a.channel_code in (1,9) and aa.first_sale_date<a.smonth then a.customer_code end) old_sales_cnt
	   ,avg(case when a.channel_code in (1,9) and aa.first_sale_date<a.smonth then a.sale_amt end) old_sales_value
	   ,sum(case when a.channel_code=7 then sale_amt end) bbc_sales_value 
	   ,sum(case when a.channel_code=2 then sale_amt end) m_sales_value 
	   ,sum(case when a.channel_code in (1,9) then profit end) b_profit
	   ,sum(case when a.channel_code=7 then profit end) bbc_profit 
	   
	   ,sum(case when a.channel_code=2 then profit end) m_profit 
 from (select performance_province_code,
       performance_province_name,
       channel_code,
	   substr(sdt,1,4)   smonth,
	   customer_code,sign_time,
	   sum(profit) profit ,
	   sum(sale_amt) sale_amt
	   FROM  csx_dws.csx_dws_sale_detail_di
	   
  WHERE sdt>='20220701' 
     AND sdt<'20240201'

     AND channel_code IN (1,9,2,7) 
   group by performance_province_code,
       performance_province_name, substr(sdt,1,4),
       channel_code,sign_time,
	   customer_code) a
left join (select b.performance_province_code,
				substr(sign_date,1,4)   smonth,
	              count(distinct a.customer_code) xinqkh 
			from csx_dws.csx_dws_crm_customer_active_di a 
			left join (select customer_code,performance_province_code from csx_dim.csx_dim_crm_customer_info where sdt='current')b on a.customer_code=b.customer_code
              where sdt='current' and sign_date>='20220701' and sign_date<'20240201'
			  group by b.performance_province_code,substr(sign_date,1,4)
			  )ab 
		ON a.performance_province_code = ab.performance_province_code and a.smonth=ab.smonth

left join (select customer_code,if(first_sale_date>='20220701' and  first_sale_date<'20240201',substr(first_sale_date,1,4),'2021') first_sale_date
		  from csx_dws.csx_dws_crm_customer_active_di 
		  where sdt='current' and  first_sale_date<'20240201')aa ON a.customer_code = aa.customer_code -- and a.smonth=aa.first_sale_date
group by a.performance_province_code
       ,a.performance_province_name
	   ,a.smonth,ab.xinqkh;
	   
	   
----------------- 年度末明细	   
select
    performance_region_name,
    performance_province_name,
	substr(sdt,1,6)  smonth,
    channel_name,
	business_type_name,
	sum(sale_amt) sale_amt,	   
	sum(profit) profit ,
	sum(sale_amt_no_tax) sale_amt_no_tax,	   
	sum(profit_no_tax) profit_no_tax
FROM  csx_dws.csx_dws_sale_detail_di	   
WHERE sdt>='20240101' 
     AND sdt<'20240201'
     AND channel_code IN (1,9,2,7) 
group by performance_region_name,
    performance_province_name,
	smonth,
    channel_name,
	business_type_name;  