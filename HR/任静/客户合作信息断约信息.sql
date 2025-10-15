
-- 客户合作信息及断约
with tmp_terminate_info as 
(select customer_code,terminate_date,business_attribute_name,
row_number()over(partition by customer_code,business_attribute_name order by terminate_date desc ) as rn from 
(select customer_code,terminate_date,business_attribute_name from csx_dim.csx_dim_crm_terminate_customer  
    where sdt='current' 
        -- and business_attribute_code=1
        and status=2
 union all 
 select customer_code,terminate_date,business_attribute_name from csx_dim.csx_dim_crm_terminate_customer_attribute  
    where sdt='current'  
        -- and business_attribute_code=1
        and approval_status=2
) a 
)
select
  a.performance_region_name,
  a.performance_province_name,
  a.performance_city_name,
  c.channel_name,
  a.customer_code,
  c.customer_name,
  c.sales_user_number,
  c.sales_user_name,
  c.first_category_name,
  c.second_category_name,
  c.customer_acquisition_type_name ,      -- 获客方式
  c.dev_source_name,                      -- 开发来源
  c.active_flag,                          -- 活跃标识(0：禁用，1：启用)
  c.status,                               -- 状态(是否有效 0无效 1有效)
  c.strategy_status,                      --是否为战略客户 0否 1是
  c.strategy_user_name,                   --战略负责人名称
  c.business_agent_user_name,              -- 业务代理用户名称
  a.business_type_name,
  a.business_sign_date,
  a.first_business_sign_date,
  a.first_business_sale_date,
  a.last_business_sale_date,
  a.sale_business_active_days,
  a.sale_business_total_amt,
  a.business_attribute_name,
  if(b.customer_code is not null ,'断约','') as terminate_flag
from
  csx_dws.csx_dws_crm_customer_business_active_di a 
  left join
  (select * from tmp_terminate_info where rn=1) b on a.customer_code=b.customer_code and a.business_attribute_name=b.business_attribute_name
  left join 
  (select channel_name,
  customer_code,
  customer_name,
  sales_user_number,
  sales_user_name,
  first_category_name,
  second_category_name,
  customer_acquisition_type_name ,      -- 获客方式
  dev_source_name,                      -- 开发来源
  active_flag,                          -- 活跃标识(0：禁用，1：启用)
  status,                               -- 状态(是否有效 0无效 1有效)
  strategy_status,                      --是否为战略客户 0否 1是
  strategy_user_name,                   --战略负责人名称
  business_agent_user_name              -- 业务代理用户名称
  from    csx_dim.csx_dim_crm_customer_info 
  where sdt='current'
  and customer_type_code=4) c on a.customer_code=c.customer_code
where
  sdt = 'current'