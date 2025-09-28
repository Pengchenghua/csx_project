
 --B端数据

SELECT 
	e.region_code,
  e.region_name,
  a.province_code,
  a.province_name,
  a.city_name,
  a.smonth,
  'B端',
  a.order_kind,
  a.profit,
  a.sales_value 
FROM
  ( 
    SELECT 
    	province_code, --省区
      province_name,
      city_name,
      month smonth,
      CASE
          WHEN channel=7 THEN 'BBC'
          WHEN channel IN ('1','9') AND attribute='合伙人客户' THEN '城市服务商'
          WHEN channel IN ('1','9') AND attribute='贸易客户'  THEN '贸易客户'
          WHEN channel IN ('1','9') AND order_kind='WELFARE' THEN '福利单'
          ELSE  '日配单'
      END AS order_kind, --订单类型：NORMAL-普通单，WELFARE-福利单
      sum(profit) profit,
      sum(sales_value) sales_value
   FROM csx_dw.dws_sale_r_m_customer_sale_archives
   WHERE month>='202004'
     and month<'202008' and month=substr(sales_date,1,6)
     AND sales_type IN ('qyg','sapqyg','sapgc','sc','bbc')
     AND channel IN ('1','9','7')
   GROUP BY province_code,
            province_name,city_name,
            month,
            CASE
               WHEN channel=7 THEN 'BBC'
               WHEN channel IN ('1','9') AND attribute='合伙人客户' THEN '城市服务商'
               WHEN channel IN ('1','9') AND attribute='贸易客户'  THEN '贸易客户'
               WHEN channel IN ('1','9')  AND order_kind='WELFARE' THEN '福利单'
               ELSE  '日配单'
           END
  )a
  LEFT JOIN
  (
  	SELECT 
  		province_code,
      province_name,
      region_code,
      region_name
   FROM csx_dw.dim_area
   WHERE area_rank='13'
  )e ON e.province_code=a.province_code 
union all
   ---M 端数据


SELECT 
	e.region_code ,
  e.region_name ,
  a.province_code,
  a.province_name,
  a.smonth,
  'M端',
  a.order_kind,
  a.profit,
  a.sales_value 
FROM
  (
  	SELECT 
  		province_code,
      province_name,
      month smonth,
      case
      when customer_no in ('103097', '103903','104842') then '红旗/中百'
      when sales_belong_flag in ('2_云创会员店','6_云创到家') then '2_云创永辉生活' else sales_belong_flag
      end order_kind,
      sum(profit) profit,
      sum(sales_value) sales_value
   FROM csx_dw.dws_sale_r_m_customer_sale_archives a
   LEFT JOIN
     ( SELECT shop_id,
              sales_belong_flag
      FROM csx_dw.dws_basic_w_a_csx_shop_m
      WHERE sdt = 'current'  
     )b  
    ON a.customer_no = concat('S', b.shop_id)
   WHERE sales_date>='20200603' 
     AND sales_date<'20200801'
     AND month>='202006'
     and month<'202008' and month=substr(sales_date,1,6)
     AND sales_type IN ('qyg','sapqyg','sapgc','sc','bbc')
     AND channel=2
   GROUP BY province_code,
           province_name ,
           month,
           sales_belong_flag
   union all 
   SELECT 
    	province_code,
      province_name,
      month smonth,
      case
      when customer_no in ('103097', '103903','104842') then '红旗/中百'
      when sales_belong_flag in ('2_云创会员店','6_云创到家') then '2_云创永辉生活' else sales_belong_flag
      end order_kind,
      sum(profit) profit,
      sum(sales_value) sales_value
   FROM csx_dw.dws_sale_r_m_customer_sale_archives a
   LEFT JOIN
    ( 
     	SELECT 
     		shop_id,
        sales_belong_flag
      FROM csx_dw.shop_m
      WHERE sdt = 'current' 
    )b ON a.customer_no = concat('S',b.shop_id)
   WHERE sales_date>='20200401' 
     AND sales_date<'20200603'
     AND month>='202004'
     and month<'202007' and month=substr(sales_date,1,6)
     AND sales_type IN ('qyg','sapqyg','sapgc','sc','bbc')
     AND channel=2
   GROUP BY province_code,
           province_name ,
           month,sales_belong_flag
 ) a
LEFT JOIN
  (SELECT province_code,
          province_name,
          region_code,
          region_name
   FROM csx_dw.dim_area
   WHERE area_rank='13') e ON e.province_code=a.province_code




 ---------课组数据
select d.region_code,d.region_name,
     a.province_code,a.province_name,a.city_name,a.smonth,
     case when b.department_name in
      ('猪肉课','蔬菜课','家禽课','干性杂货课','干货课','日配课','水果课','冰鲜课',
     '休闲食品课','活鲜课','熟食课','饮料香烟课','清洁用品课','包点课',
     '家庭用品课','家电课','贝类课','纺织用品课') then b.department_name --课组销售额top18
      else '其他' end department_name
     ,count(distinct a.customer_no) custno
     ,sum(a.sales_value) sales_value
     ,sum(a.profit) profit
     --sum(case when smonth='202006' then a.sales_value end) sales_value_m
from
(select a.province_code,a.province_name,a.city_name,a.customer_no,a.goods_code,a.month smonth,
        sum(a.sales_value) sales_value,
        sum(a.profit) profit
 from csx_dw.dws_sale_r_m_customer_sale_archives a ---fengcun
 left join (select * from csx_dw.dws_crm_w_a_customer_m_v1 where sdt='20200801') c on c.customer_no=a.customer_no
 where a.month>='202005'
 and a.month<='202007' and a.month=substr(a.sales_date,1,6)
 and a.sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
 and a.channel in ('1','9') and (a.order_kind<>'WELFARE' and c.attribute NOT IN ('贸易客户','合伙人客户'))
group by a.province_code,a.province_name,a.city_name,a.customer_no,a.goods_code,a.month
)a
left join (select * from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') b on b.goods_id=a.goods_code
left join (select province_code,province_name,region_code,region_name from csx_dw.dim_area where area_rank='13')d 
  on d.province_code=a.province_code
group by d.region_code,d.region_name,
				a.province_code,a.province_name,a.city_name,a.smonth,
				case when b.department_name in ('猪肉课','蔬菜课','家禽课','干性杂货课','干货课','日配课','水果课','冰鲜课',
																				'休闲食品课','活鲜课','熟食课','饮料香烟课','清洁用品课','包点课',
																		'家庭用品课','家电课','贝类课','纺织用品课') then b.department_name
else '其他' end 
--order by sales_value desc;

-------各省区月度复盘数据
with aa as (select
customer_no,min(sales_date) as min_sales_date,max(sales_date) as max_sales_date,count(distinct sales_date) as count_day
from 
(select customer_no,sales_date,sales_value from csx_dw.sale_item_m where sdt>='20180101' and sdt<'20190101' and sales_type in('qyg','sapqyg','sapgc','sc','bbc','gc','anhui') 
union all 
select customer_no,sales_date,sales_value from csx_dw.dws_sale_r_d_customer_sale 
where sdt>='20190101' and sdt<=regexp_replace(date_sub(current_date,1),'-','') and sales_type in('qyg','sapqyg','sapgc','sc','bbc') 
and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046') or order_no is null)
) a
group by customer_no)


SELECT a.province_code
     ,a.province_name,a.city_name
	   ,a.smonth
     ,sum(case when a.channel in (1,9) then sales_value end) b_sales_value  
	   ,sum(case when a.channel in (1,9) and substr(aa.min_sales_date,1,6)=a.smonth then a.sales_value end) xin_sales_value  
	   ,count(distinct case when a.channel in (1,9) and substr(regexp_replace(to_date(b.sign_time),'-',''),1,6)=a.smonth then a.customer_no end) sin_sales_value  
	   ,count(distinct case when a.channel in (1,9) and substr(aa.min_sales_date,1,6)=a.smonth then a.customer_no end) xin_sales_value  
	   ,sum(case when a.channel in (1,9) and substr(aa.min_sales_date,1,6)<a.smonth then a.sales_value end) old_sales_value
	   ,count(distinct case when a.channel in (1,9) and substr(aa.min_sales_date,1,6)<a.smonth then a.customer_no end) old_sales_cnt
	   ,avg(case when a.channel in (1,9) and substr(aa.min_sales_date,1,6)<a.smonth then a.sales_value end) old_sales_value
	   ,sum(case when a.channel=7 then sales_value end) bbc_sales_value 
	   ,sum(case when a.channel=2 then sales_value end) m_sales_value 
	   ,sum(case when a.channel in (1,9) then profit end) b_profit
	   ,sum(case when a.channel=7 then profit end) bbc_profit 
	   ,sum(case when a.channel=2 then profit end) m_profit 
 from (select province_code,
       province_name,
	   city_name,
       channel,
	   month smonth,
	   customer_no,
	   sum(profit) profit ,
	   sum(sales_value) sales_value
	   FROM csx_dw.dws_sale_r_m_customer_sale_archives
   WHERE month in ('202007') and month=substr(sales_date,1,6)
     AND sales_type IN ('qyg','sapqyg','sapgc','sc','bbc')
     AND channel IN (1,9,2,7) and province_name='福建省'
   group by province_code,
       province_name,city_name,
       channel,
	   month,
	   customer_no) a
LEFT JOIN (SELECT *
               FROM csx_dw.dws_crm_w_a_customer_m_v1
               WHERE sdt=regexp_replace(date_sub(current_date,1),'-','') ) b ON a.customer_no=b.customer_no
LEFT JOIN aa on a.customer_no=aa.customer_no
group by a.province_code
       ,a.province_name,city_name,
	   ,a.smonth;

