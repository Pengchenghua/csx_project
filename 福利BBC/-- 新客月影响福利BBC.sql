--   新客月影响福利BBC
------------------------- 全国新老客毛利（月至今）----------------------
with sale as (select
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  a.city_group_code,
  a.city_group_name,
  a.business_type_code,
  a.business_type_name,
  a.customer_code,
  c.customer_name,
  first_sales_date,
  if(substr(first_sales_date,1,6)=substr(regexp_replace(trunc('2023-05-21', 'YY'), '-', ''),1,6),1,0) as new_type,
  sum(sales_value) as sales_value,
  sum(profit) as profit
from
  (
    select
      sdt,
      performance_region_code region_code,
      performance_region_name region_name,
      performance_province_code province_code,
      performance_province_name province_name,
      performance_city_code city_group_code,
      performance_city_name city_group_name,
      customer_code,
      business_type_code,
      business_type_name,
      sum(sale_amt) as sales_value,
      sum(profit) as profit
    from
      (
        select
          *
        from
          csx_dws.csx_dws_sale_detail_di
        where
          sdt >= regexp_replace(trunc('2023-05-21', 'YY'), '-', '')
          and sdt <= regexp_replace(add_months('2023-05-21', 0), '-', '')
          and business_type_code in ( '2','6')
      ) a
    group by
      sdt,
      performance_region_code,
      performance_region_name,
      performance_province_code,
      performance_province_name,
      performance_city_code,
      performance_city_name,
      customer_code,
      business_type_code,
      business_type_name
  ) a
  left join -- 首单日期
  (
    select
      customer_code,
      customer_name,
      business_type_code,
      min(first_business_sale_date) first_sales_date
    from
        csx_dws.csx_dws_crm_customer_business_active_di
    where
      sdt = 'current'
      and business_type_code in (2, 6)
    group by
      customer_code,
      business_type_code,
      customer_name
  ) c on c.customer_code = a.customer_code and a.business_type_code=c.business_type_code
group by
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  a.city_group_code,
  a.city_group_name,
  a.business_type_code,
  a.business_type_name,
  a.customer_code,
  c.customer_name,
  first_sales_date,
  if(substr(first_sales_date,1,6)=substr(regexp_replace(trunc('2023-05-21', 'YY'), '-', ''),1,6),1,0),
  first_sales_date
  )  select * from sale where new_type=1;