--BBC授信金额
select
  a.performance_region_name,
  a.performance_province_name,
  a.performance_city_name,
  a.customer_code,
  a.customer_name,
  sales_user_number,
  sales_user_name,
  second_category_name,
  credit_balance,
  count_person
from
  (
    select
      performance_region_name,
      performance_province_name,
      performance_city_name,
      customer_code,
      customer_name,
      second_category_name,
      credit_balance,
      count_person
    from
      csx_analyse.csx_analyse_fr_bbc_wshop_user_credit_di
    where
      sdt in( '20250731','20250831', '20250930' )
      and (
        credit_balance is not null
        or count_person is not null
      )
  ) a
  left join (
    select
      customer_code,
      sales_user_number,
      sales_user_name
    from
      csx_dim.csx_dim_crm_customer_info
    where sdt='current'
  ) b on a.customer_code = b.customer_code