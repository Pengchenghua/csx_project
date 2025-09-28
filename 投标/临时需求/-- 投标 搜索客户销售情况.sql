-- 投标 搜索客户销售情况
select
  ---每日业绩
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  company_code,
  company_name,
  a.customer_code,
  b.customer_name,
  b.first_category_name,
  b.second_category_name,
  sum(sale_amt) sale_amt,
  sum(profit) profit
from
  csx_dws.csx_dws_sale_detail_di a
   join (
    SELECT
      customer_code,
      customer_name,
      first_category_name,
      second_category_name
    FROM
      csx_dim.csx_dim_crm_customer_info a
    where
      sdt = 'current'
      and customer_name like '%邮政%'
  ) b on a.customer_code = b.customer_code
where
  (sdt >= '20230101') --	and channel_code in('1','7','9')
group by
  company_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  company_code,
  a.customer_code,
  b.customer_name,
  b.first_category_name,
  b.second_category_name