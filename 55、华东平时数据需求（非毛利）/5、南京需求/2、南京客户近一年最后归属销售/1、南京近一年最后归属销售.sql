select 
    a.performance_region_name as `大区`,
    a.performance_province_name as `省区`,
    a.performance_city_name as `城市`,
    a.customer_code as `客户编码`,
    a.customer_name as `客户名称`,
    a.sales_user_id as `业务员id`,
    a.sales_user_number as `业务员工号`,
    a.sales_user_name as `业务员名称` 
from 
(select 
    *,
    row_number()over(partition by customer_code order by sdt desc) as pm 
from csx_dim.csx_dim_crm_customer_info 
where sdt>=regexp_replace(date_sub('${yes_date}',360),'-','') 
and sdt<=regexp_replace('${yes_date}','-','') 
and performance_province_name='江苏南京'
and sales_user_name is not null 
and length(sales_user_name)>0 
) a 
where a.pm=1 