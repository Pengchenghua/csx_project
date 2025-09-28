select 
  sales_city,
  department_name,
sum(cc),
sum(dd),
  count(distinct customer_no),
sum(aa),
sum(bb)

from

(



 select 
  province_name,
  sales_city,
  department_name,
  customer_no,
  count(distinct sales_date)aa,
  sum(a)bb,
  sum(b)cc,
  sum(c)dd

from
(
select 
  province_name,
  sales_city,
  department_name,
  b.customer_no,
  sales_date,
  count(distinct goods_code)a,
  sum(AAA)b,
  sum(BBB)c

FROM
(
  SELECT  substr(sdt,1,6)smonth,
  customer_no,
  province_name,
  sales_city,
  sales_date,
  goods_code,
  department_name,
  sum(sales_value)AAA,
  sum(profit)BBB 
  FROM csx_dw.dws_sale_r_d_customer_sale
  WHERE sdt >= '20200801' AND sdt <= '20200831'
  AND attribute_code != '5'
  AND channel_name LIKE '大客户%'
  AND sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
  and sales_city in 
  (
  '成都市',
'北京市',
'福州市',
'贵阳市',
'杭州市',
'合肥市',
'南京市',
'南平市',
'宁波市',
'莆田市',
'泉州市',
'厦门市',
'上海市',
'石家庄市',
'苏州市',
'西安市',
'重庆市',
'深圳市')
  group by substr(sdt,1,6),
  customer_no,
  province_name,
  sales_city,
  sales_date,
  goods_code,department_name
) b 

left join 
(select 
     distinct customer_no,
     substr(sdt,1,6)smonth 
from csx_dw.csx_partner_list 
) d 
on d.customer_no= b.customer_no 
and b.smonth=d.smonth

where d.customer_no is null

group by   province_name,
  sales_city,
  department_name,
  b.customer_no,
  sales_date

)f
group by province_name,
  sales_city,
  department_name,
  customer_no

)e

group by 
  sales_city,
  department_name
  
  order by sales_city,department_name