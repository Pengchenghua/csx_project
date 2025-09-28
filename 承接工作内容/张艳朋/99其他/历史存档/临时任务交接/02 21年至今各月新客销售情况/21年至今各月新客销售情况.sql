--21年至今各月新客销售情况

insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t'
select 
  b.first_order_month,a.province_name,a.channel_name,a.customer_no,a.customer_name,
  a.first_category_name,a.second_category_name,a.third_category_name,
  a.attribute_desc,a.dev_source_name,a.cooperation_mode_name,
  a.work_no,a.sales_name,a.first_supervisor_work_no,a.first_supervisor_name,a.sign_date,
  c.sign_company_code,d.name,c.sales_value
from
  (
  select province_name,channel_name,customer_no,customer_name,
    attribute_desc,dev_source_name,cooperation_mode_name,
    work_no,sales_name,first_supervisor_work_no,first_supervisor_name,
	first_category_name,second_category_name,third_category_name,
    regexp_replace(split(first_sign_time, ' ')[0], '-', '') as sign_date,
    estimate_contract_amount
  from csx_dw.dws_crm_w_a_customer
  where sdt='current'
  and channel_code in('1','7','9')
  )a
join
--客户最早销售月 新客月、新客季度
  (select customer_no,first_order_date,substr(first_order_date,1,6) first_order_month
  from csx_dw.dws_crm_w_a_customer_active
  where sdt = 'current'
  and first_order_date>='20210101' 
  --and first_order_date<'20210601'
  )b on b.customer_no=a.customer_no
left join 
  (	
  select region_name,province_name,customer_no,sign_company_code,substr(sdt,1,6) smonth,
    sum(sales_value) sales_value,   --含税销售额
    sum(sales_cost) sales_cost,   --含税销售成本
    sum(profit) profit,   --含税毛利
    sum(front_profit) front_profit   --前端含税毛利
  from csx_dw.dws_sale_r_d_detail
  where sdt>='20210101'
  and channel_code in('1','7','8','9')
  group by region_name,province_name,customer_no,sign_company_code,substr(sdt,1,6)	
  )c on c.customer_no=b.customer_no and c.smonth=b.first_order_month
left join
  (
    select code,name
    from csx_dw.dws_basic_w_a_company_code
    where sdt = regexp_replace(date_sub(current_date, 1), '-', '')
  )d on c.sign_company_code = d.code;