-- 搜索签约客户清单
with temp_company_credit as 
  ( select
  performance_province_name,
  customer_code,
  credit_code,
  customer_name,
  business_attribute_code,
  business_attribute_name,
  company_code,
  company_name,
  status,
  is_history_compensate
from
        csx_dim.csx_dim_crm_customer_company_details
where
  sdt = 'current'
  -- and status=1
  and(customer_name like '%大学%' or customer_name like '%学院%')
group by customer_code,
    credit_code,
    customer_name,
    business_attribute_code,
    business_attribute_name,
    company_code,
    status,
    company_name,
    performance_province_name,
  is_history_compensate
) select a.performance_province_name,
    a.customer_code,
  credit_code,
  a.customer_name,
  business_attribute_code,
  business_attribute_name,
  company_code,
  company_name,
  second_category_name
  from temp_company_credit a 
join 
(select * from    csx_dim.csx_dim_crm_customer_info where sdt='current') b on a.customer_code=b.customer_code


--有销售的
select substr(a.sdt,1,4) sale_year,
        a.performance_province_name,
        a.business_type_name,
        a.customer_code,
        b.customer_name,
        b.second_category_name,
        a.sign_company_code,
        a.sign_company_name,
        sum(sale_amt) sale_amt
    from    csx_dws.csx_dws_sale_detail_di a   
join 
(select * from    csx_dim.csx_dim_crm_customer_info 
where sdt='current'
and(customer_name like '%大学%' or customer_name like '%学院%')
) b on a.customer_code=b.customer_code
where a.sdt>='20210101'
group by substr(a.sdt,1,4),
        a.performance_province_name,
        a.business_type_name,
        a.customer_code,
        b.customer_name,
        b.second_category_name,
        a.sign_company_code,
        a.sign_company_name
