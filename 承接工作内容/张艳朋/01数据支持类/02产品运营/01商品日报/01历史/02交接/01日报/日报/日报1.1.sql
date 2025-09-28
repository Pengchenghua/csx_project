insert overwrite directory '/tmp/xumengyang1/0803/daily-s' row format delimited fields terminated by '\t' 
SELECT 
A.province_name AS `销售省区`,
A.sales_city as `城市`,
A.goods_code as`商品编码`,
A.name as`商品名称`,
sum(A.sales_value)as`销售额`,
sum(A.profit)as`毛利额`,
count(distinct AAA)as`客户数`
FROM 
(
SELECT
  substr(sdt,1,6)smonth,
  a.customer_no AAA,
  province_name,
  sales_city,
  a.goods_code,
  regexp_replace(goods_name, '\n|\t|\r', '') AS `name`,
  sales_value,
  profit

FROM
(
  SELECT * FROM csx_dw.dws_sale_r_d_customer_sale
  WHERE sdt >= '20200801' AND sdt <= '20200803'
  AND attribute_code != '5'
  AND channel_name LIKE '大客户%'
  AND sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
) a LEFT JOIN
(
  select
    customer_no,
    first_sale_day
  from csx_dw.ads_sale_w_d_ads_customer_sales_q
  where sdt = '20200803'
) b on a.customer_no = b.customer_no
) A
left join 
(select 
     distinct customer_no,
     substr(sdt,1,6)smonth 
from csx_dw.csx_partner_list 
) d on d.customer_no=A.AAA and d.smonth=A.smonth
where d.customer_no is null


group by A.province_name,
A.sales_city,
A.goods_code,
A.name 