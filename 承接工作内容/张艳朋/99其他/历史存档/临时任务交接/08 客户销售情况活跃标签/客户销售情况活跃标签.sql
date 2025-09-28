--财务 客户销售情况活跃标签
insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t'
select b.*,a.sign_company_code,a.last_sales_date,a.last_to_now_days,customer_active_status_code,
	   case when customer_active_status_code = 1 then '活跃客户'
	   	when customer_active_status_code = 2 then '沉默客户'
	   	when customer_active_status_code = 3 then '预流失客户'
	   	when customer_active_status_code = 4 then '流失客户'
	   	else '其他'
	   	end  as  customer_active_sts
from 
  (
    select distinct customer_no,sign_company_code,
      last_sales_date,
      last_to_now_days,
      customer_active_status_code  --客户活跃状态标签编码（1 活跃客户；2 沉默客户；3预流失客户；4 流失客户）
    from csx_dw.dws_sale_w_a_customer_company_active
    where sdt = 'current'
  )a 
right join 
  (
    select sales_region_name,province_name,city_group_name,channel_name,customer_no,customer_name
    from csx_dw.dws_crm_w_a_customer
    where sdt='current' 
    and channel_code in('1','7','9')
  )b on a.customer_no=b.customer_no;