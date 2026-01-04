select 
    a.month as `月份`,
    a.performance_city_name as `城市`,
    a.customer_code as `客户编码`,
    b.customer_name as `客户名称`,
    a.sub_customer_code as `子客户编码`,
    a.sub_customer_name as `子客户名称`,
    b.sales_user_name as `销售员`,
    c.rp_service_user_name_new as `日配服务管家`,
    a.sale_amt as `销售额`,
    a.profit as `毛利额` 
from 
(select 
    performance_city_name,
    substr(sdt,1,6) as month,
    customer_code,
    sub_customer_code,
    max(sub_customer_name) as sub_customer_name,
    sum(sale_amt) as sale_amt,
    sum(profit) as profit 
from csx_dws.csx_dws_sale_detail_di 
where sdt>='20250701' 
and business_type_code=1 
and performance_province_name='江苏南京' 
group by 
    performance_city_name,
    substr(sdt,1,6),
    customer_code,
    sub_customer_code 
) a 
left join 
(select 
    * 
from csx_dim.csx_dim_crm_customer_info 
where sdt='current'
) b 
on a.customer_code=b.customer_code 
left join 
(select * 
from csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df  
where sdt='20250908'
) c 
on a.customer_code=c.customer_no 