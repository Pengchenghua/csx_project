select a.*,B.dd from

(
SELECT 
A.*,Row_Number() OVER (partition by sales_city ORDER BY aa) rank -- desc
FROM 
(
SELECT
  a.province_name,
  a.sales_city,
  case when a.division_name in ('生鲜部','加工部')then '生鲜'
  when a.division_name in ('食品类')then '食品'
  when a.division_name in ('用品类','易耗品','服装')then '非食品'
  end,
  a.department_name,
  a.goods_code,
  a.goods_name,
  sum(c)cc,
  sum(a)aa,
  sum(a)/sum(c) as dd,
  sum(b)bb,
  count(distinct a.customer_no)
FROM
(
  SELECT   
  substr(sdt,1,6)smonth,
  customer_no,
  province_name,
  sales_city,
  goods_code,
  regexp_replace(goods_name, '\n|\t|\r', '') as goods_name,
  department_name,division_name,
  sum(sales_qty)b,
  sum(sales_value)c,
  sum(profit) a 
  FROM csx_dw.dws_sale_r_d_customer_sale
  WHERE sdt >= '20200817' AND sdt <= '20200817'
  AND attribute_code != '5'
  AND channel_name LIKE '大客户%'
  AND sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
  group by substr(sdt,1,6),
  customer_no,
  province_name,
  sales_city,
  sales_date,
  goods_code,
  regexp_replace(goods_name, '\n|\t|\r', ''),
  department_name,
  division_name
  ) a 

left join 
(select 
     distinct customer_no,
     substr(sdt,1,6)smonth 
from csx_dw.csx_partner_list 
) d on d.customer_no= a.customer_no and d.smonth=a.smonth


where d.customer_no is null

group by
  a.province_name,
  a.sales_city,
  case when a.division_name in ('生鲜部','加工部')then '生鲜'
  when a.division_name in ('食品类')then '食品'
  when a.division_name in ('用品类','易耗品','服装')then '非食品'
  end,
  a.department_name,
  a.goods_code,
  a.goods_name

having sum(c)>0 and sum(a)<0

) A

)a


left join
(

select 
  province_name,
  sales_city,
  goods_code,
  count(distinct b.sales_date) dd

FROM
(
  SELECT  substr(sdt,1,6)smonth,
  customer_no,
  province_name,
  sales_city,
  sales_date,
  goods_code,
  department_name,
  sum(profit)BBB FROM csx_dw.dws_sale_r_d_customer_sale
  WHERE sdt >= '20200801' AND sdt <= '20200817'
  AND attribute_code != '5'
  AND channel_name LIKE '大客户%'
  AND sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
  group by substr(sdt,1,6),
  customer_no,
  province_name,
  sales_city,
  sales_date,department_name,
  goods_code
  having BBB<0
) b 

left join 
(select 
     distinct customer_no,
     substr(sdt,1,6)smonth 
from csx_dw.csx_partner_list 
) d on d.customer_no= b.customer_no and b.smonth=d.smonth

where d.customer_no is null

group by province_name,
  sales_city,
  goods_code

) B

on B.province_name=a.province_name
and a.sales_city=B.sales_city
and a.goods_code=B.goods_code


where rank<=10