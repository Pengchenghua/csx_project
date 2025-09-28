
--第二部分
--2.1、各省区销售主管-月度业务类型的客户数、销售额、毛利
drop table csx_tmp.tmp_third_supervisor_sale_1;
create table csx_tmp.tmp_third_supervisor_sale_1
as 
select 
  a.smonth,a.business_type_code,a.business_type_name,
  a.region_name,a.province_name,a.city_group_name,
  c.third_supervisor_work_no,c.third_supervisor_name,c.first_supervisor_work_no,c.first_supervisor_name,
  count(distinct a.customer_no) count_all,
  sum(a.sales_value)/10000 sales_value_all,
  sum(a.profit)/10000 profit_all,  
  count(distinct case when substr(b.min_sdt,1,6)=a.smonth then a.customer_no end) count_new,
  sum(case when substr(b.min_sdt,1,6)=a.smonth then a.sales_value end)/10000 sales_value_new,
  sum(case when substr(b.min_sdt,1,6)=a.smonth then a.profit end)/10000 profit_new
from 
(
select 
case when (business_type_code='1' and dc_code in('W0K4')) then '9' else business_type_code end business_type_code,
case when (business_type_code='1' and dc_code in('W0K4')) then 'W0K4' else business_type_name end business_type_name,
--business_type_code,business_type_name,
region_name,province_name,city_group_name,customer_no,substr(sdt,1,6) smonth,
sales_value,profit
from csx_dw.dws_sale_r_d_detail
where sdt>='20210701'
and channel_code in('1','7','9')
--and business_type_code='1'
  and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
  					'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
)a 
left join csx_tmp.tmp_cust_sale_1 b on a.customer_no=b.customer_no and b.business_type_code=a.business_type_code
left join 
  (
	select customer_no,substr(sdt,1,6) smonth,
	third_supervisor_work_no,third_supervisor_name,first_supervisor_work_no,first_supervisor_name
	from csx_dw.dws_crm_w_a_customer
	where sdt in('20210731','20210813')
  )c on c.customer_no=a.customer_no and c.smonth=a.smonth
group by a.smonth,a.business_type_code,a.business_type_name,a.region_name,a.province_name,a.city_group_name,
c.third_supervisor_work_no,c.third_supervisor_name,c.first_supervisor_work_no,c.first_supervisor_name
union all
select 
  a.smonth,
  '0' business_type_code,'总体' business_type_name,
  a.region_name,a.province_name,a.city_group_name,
  c.third_supervisor_work_no,c.third_supervisor_name,c.first_supervisor_work_no,c.first_supervisor_name,
  count(distinct a.customer_no) count_all,
  sum(a.sales_value)/10000 sales_value_all,
  sum(a.profit)/10000 profit_all,  
  count(distinct case when substr(b.first_order_date,1,6)=a.smonth then a.customer_no end) count_new,
  sum(case when substr(b.first_order_date,1,6)=a.smonth then a.sales_value end)/10000 sales_value_new,
  sum(case when substr(b.first_order_date,1,6)=a.smonth then a.profit end)/10000 profit_new
from 
(
select 
case when (business_type_code='1' and dc_code in('W0K4')) then '9' else business_type_code end business_type_code,
case when (business_type_code='1' and dc_code in('W0K4')) then 'W0K4' else business_type_name end business_type_name,
--business_type_code,business_type_name,
region_name,province_name,city_group_name,customer_no,substr(sdt,1,6) smonth,
sales_value,profit
from csx_dw.dws_sale_r_d_detail 
where sdt>='20210701'
and channel_code in('1','7','9')
--and business_type_code='1'
  and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
  					'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
)a 
left join 
(
select customer_no,first_order_date
from csx_dw.dws_crm_w_a_customer_active
where sdt='current'
)b on a.customer_no=b.customer_no 
left join 
  (
	select customer_no,substr(sdt,1,6) smonth,
	third_supervisor_work_no,third_supervisor_name,first_supervisor_work_no,first_supervisor_name
	from csx_dw.dws_crm_w_a_customer
	where sdt in('20210731','20210813')
  )c on c.customer_no=a.customer_no and c.smonth=a.smonth
group by a.smonth,a.region_name,a.province_name,a.city_group_name,
c.third_supervisor_work_no,c.third_supervisor_name,c.first_supervisor_work_no,c.first_supervisor_name;


--2.2、新签约客户数 省区销售主管月度
drop table csx_tmp.tmp_third_supervisor_sale_2;
create table csx_tmp.tmp_third_supervisor_sale_2
as 
select 
  a.smonth,a.sales_region_name,a.province_name,a.city_group_name,
  a.third_supervisor_work_no,a.third_supervisor_name,a.first_supervisor_work_no,a.first_supervisor_name,
  count(distinct case when b.emp_status='on' then a.work_no end) count_work_no,
  count(distinct case when a.smonth=a.first_sign_month then a.customer_no end ) count_sign
from 
--CRM客户信息昨日
  (select *,substr(sdt,1,6) smonth,
    regexp_replace(split(first_sign_time, ' ')[0], '-', '') as first_sign_date,
    substr(regexp_replace(split(first_sign_time, ' ')[0], '-', ''),1,6) as first_sign_month
  from csx_dw.dws_crm_w_a_customer 
  where sdt in('20210731','20210813')
  and channel_code in('1','7','9')
  )a 
left join
  (
  select substr(sdt,1,6) smonth,employee_code,employee_name,emp_status
  from csx_dw.dws_basic_w_a_employee_org_m
  where sdt in('20210731','20210813')
  --and emp_status='on'
  )b on b.employee_code=a.work_no and b.smonth=a.smonth
group by a.smonth,a.sales_region_name,a.province_name,a.city_group_name,
a.third_supervisor_work_no,a.third_supervisor_name,a.first_supervisor_work_no,a.first_supervisor_name;

--2.3、75%商机与50%商机 省区销售主管月度
drop table csx_tmp.tmp_third_supervisor_sale_3;
create table csx_tmp.tmp_third_supervisor_sale_3
as 
select 
  sales_region_name,city_group_name,
  case when city_group_name='苏州市' then '江苏苏州'
       when city_group_name='南京市' then '江苏南京'
       else sales_province_name end province_name,
  third_supervisor_work_no,third_supervisor_name,first_supervisor_work_no,first_supervisor_name,
  count(distinct case when business_stage='4' then id end ) count_75,
  count(distinct case when business_stage='3' then id end ) count_50
from csx_dw.dws_crm_w_a_business_customer 
where sdt='current'
and status='1'
group by sales_region_name,city_group_name,
  case when city_group_name='苏州市' then '江苏苏州'
       when city_group_name='南京市' then '江苏南京'
       else sales_province_name end,
third_supervisor_work_no,third_supervisor_name,first_supervisor_work_no,first_supervisor_name;

--结果表
drop table csx_tmp.tmp_third_supervisor_sale_4;
create table csx_tmp.tmp_third_supervisor_sale_4
as 
select
region_name,province_name,city_group_name,
third_supervisor_work_no,third_supervisor_name,first_supervisor_work_no,first_supervisor_name,
business_type_name,
sum(case when smonth='202107' then count_work_no end) count_cust_4,
sum(case when smonth='202108' then count_work_no end) count_cust_5,

sum(case when smonth='202107' then count_all end) count_all_4,
sum(case when smonth='202108' then count_all end) count_all_5,
sum(case when smonth='202107' then sales_value_all end) sales_value_all_4,
sum(case when smonth='202108' then sales_value_all end) sales_value_all_5,
sum(case when smonth='202107' then profit_all end) profit_all_4,
sum(case when smonth='202108' then profit_all end) profit_all_5,
sum(case when smonth='202107' then count_sign end) count_sign_4,
sum(case when smonth='202108' then count_sign end) count_sign_5,
sum(count_75) count_75,
sum(count_50)count_50
from 
(
select 
smonth,business_type_code,business_type_name,region_name,province_name,city_group_name,
third_supervisor_work_no,third_supervisor_name,first_supervisor_work_no,first_supervisor_name,
'' count_work_no,count_all,sales_value_all,profit_all,'' count_sign,'' count_75,'' count_50
from csx_tmp.tmp_third_supervisor_sale_1
union all
select
smonth,'0' business_type_code,'总体' business_type_name,
sales_region_name as region_name,province_name as province_name,city_group_name,
third_supervisor_work_no,third_supervisor_name,first_supervisor_work_no,first_supervisor_name,
count_work_no,'' count_all,'' sales_value_all,'' profit_all,count_sign,'' count_75,'' count_50
from csx_tmp.tmp_third_supervisor_sale_2
union all
select
'202108' smonth,'0' business_type_code,'总体' business_type_name,
sales_region_name as region_name,province_name as province_name,city_group_name,
third_supervisor_work_no,third_supervisor_name,first_supervisor_work_no,first_supervisor_name,
'' count_work_no,'' count_all,'' sales_value_all,'' profit_all,'' count_sign,count_75,count_50
from csx_tmp.tmp_third_supervisor_sale_3
)a
group by 
region_name,province_name,city_group_name,
third_supervisor_work_no,third_supervisor_name,first_supervisor_work_no,first_supervisor_name,
business_type_name;



--INVALIDATE METADATA csx_tmp.tmp_third_supervisor_sale_4;






























