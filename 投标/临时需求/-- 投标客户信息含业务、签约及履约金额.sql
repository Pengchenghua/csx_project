-- 投标客户信息
select
  channel_name,
  business_type_name,
  performance_region_name,
  performance_province_name,
  performance_city_name,
  a.customer_code,
  customer_name,
  contact_person,
  contact_phone,
  first_category_name,
  second_category_name,
  third_category_name,
  sales_user_number,
  sales_user_name,
  city_manager_user_name,
  sign_time,
  business_sign_date,
  first_business_sign_date,
  first_business_sale_date,
  last_business_sale_date,
  sale_business_total_amt,
  sign_company_code
from
  csx_dws.csx_dws_crm_customer_business_active_di a
  left join 
  (
    select
      customer_code,
      city_manager_user_name,
      first_category_name,
      second_category_name,
      third_category_name,
      contact_person,
      contact_phone,
      sign_time,
      sign_company_code
    from
      csx_dim.csx_dim_crm_customer_info
    where
      sdt = 'current'
  ) b on a.customer_code=b.customer_code
where
  sdt = 'current'