-- 投标 品牌销售情况 
select
  ---每日业绩
  performance_province_code ,
  performance_province_name ,
  performance_city_code ,
  performance_city_name,
  company_code,
  company_name,
  customer_code,
  customer_name,
  a.goods_code,
  goods_name,
  b.brand_name,
  b.classify_large_name,
  b.classify_middle_name,
  sum(sale_amt) sale_amt,
  sum(profit) profit
from
  csx_dws.csx_dws_sale_detail_di a
  join (
    SELECT
      goods_code,
      brand_name,
      classify_large_name,
      classify_middle_name
    FROM
      csx_dim.csx_dim_basic_goods a
    where
      sdt = 'current'
      and(
        brand_name like '%优选%'
        OR brand_name LIKE '%优颂%'
      ) --  and classify_large_code in ('B07','B08')
     -- AND division_code = '13'
  ) b on a.goods_code = b.goods_code
where
  (sdt >= '20210101') --	and channel_code in('1','7','9')
group by
    company_name,
performance_province_code ,
  performance_province_name ,
  performance_city_code ,
  performance_city_name,
  company_code,
  customer_code,
  customer_name,
  a.goods_code,
  goods_name,
  b.brand_name,
  b.classify_large_name,
  b.classify_middle_name