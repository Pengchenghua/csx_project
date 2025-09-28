-- 动态分区
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

--select ${hiveconf:sdate_by1},${hiveconf:sdate_by2},${hiveconf:sdate_sy1},${hiveconf:sdate_sy2};

-- 昨日月1日，昨日、上月1日，上月昨日; 
set sdate_by1 =regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','');
set sdate_by2 =regexp_replace(date_sub(current_date,1),'-','');
set sdate_sy1 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');
set sdate_sy2 =concat(substr(regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-',''),1,6),
          if(date_sub(current_date,1)=last_day(date_sub(current_date,1))
          ,substr(regexp_replace(last_day(add_months(trunc(date_sub(current_date,1),'MM'),-1)),'-',''),7,2)
          ,substr(regexp_replace(date_sub(current_date,1),'-',''),7,2)));
set i_sdate_dd =from_utc_timestamp(current_timestamp(),'GMT');  ------当前时间


--01指定客户业绩（业务类型-日配业绩）
--insert overwrite directory '/tmp/raoyanhua/zhidingkehu' row format delimited fields terminated by '\t'

insert overwrite table csx_dw.report_sale_r_d_customer_zhiding partition(sdt)
select
  -- 唯一主键 
  concat_ws('&', c.province_code,c.city_group_code,a.customer_no,${hiveconf:sdate_by2}) as biz_id,
  c.region_code,c.region_name,c.province_code,c.province_name,c.city_group_code,c.city_group_name,a.customer_no,b.customer_name,
  a.H_sales_value,
  a.H_profit,
  a.H_profit/abs(a.H_sales_value) H_profit_rate,  
  a.D_sales_value,
  a.D_profit,
  a.D_profit/abs(a.D_sales_value) D_profit_rate,
  a.D_profit/abs(a.D_sales_value)-a.H_profit/abs(a.H_sales_value) as D_H_profit_rate,
  d.H_province_sales_value,
  c.H_city_sales_value,
  'raoyanhua' create_by,
  ${hiveconf:i_sdate_dd} create_time,
  ${hiveconf:sdate_by2} sdt    
from
(
  select 
    region_code,region_name,province_code,province_name,city_group_code,city_group_name,customer_no,
    sum(case when substr(sdt,1,6)=substr(${hiveconf:sdate_sy1},1,6) then sales_value else 0 end) H_sales_value,
    sum(case when substr(sdt,1,6)=substr(${hiveconf:sdate_sy1},1,6) then profit else 0 end) H_profit,
    sum(case when substr(sdt,1,6)=substr(${hiveconf:sdate_by1},1,6) then sales_value else 0 end) D_sales_value,
    sum(case when substr(sdt,1,6)=substr(${hiveconf:sdate_by1},1,6) then profit else 0 end) D_profit
  from csx_dw.dws_sale_r_d_detail
  where sdt >=${hiveconf:sdate_sy1} and sdt <=${hiveconf:sdate_by2}  --上月第1天至本月昨日
  and channel_code in('1','9')
  and business_type_code in('1')
  and dc_code not in('W0K4')
  group by region_code,region_name,province_code,province_name,city_group_code,city_group_name,customer_no
)a 
join (select customer_no from csx_tmp.sales_customer_qingdan)a1 on a1.customer_no=a.customer_no
left join
--客户信息
(
  select 
    channel_name,sales_province_code,sales_province_name,customer_no,customer_name,  
    attribute_desc as attribute_name,first_category_name,second_category_name,third_category_name,
    work_no,sales_name,
    contact_person,contact_phone
  from csx_dw.dws_crm_w_a_customer 
  where sdt=${hiveconf:sdate_by2} 
  --and attribute_code ='1'
  and channel_code in('1','7','9')
)b on b.customer_no=a.customer_no
right join
(
  --城市上月整月日配单业绩
  select 
    region_code,region_name,province_code,province_name,city_group_code,city_group_name, 
  sum(sales_value)as H_city_sales_value,
    sum(profit)as H_city_profit
  from csx_dw.dws_sale_r_d_detail
  where sdt >=${hiveconf:sdate_sy1} and sdt <${hiveconf:sdate_by1}  --上月整月
  and channel_code in('1','9')
  and business_type_code in('1')
  and dc_code not in('W0K4')
  group by region_code,region_name,province_code,province_name,city_group_code,city_group_name
)c on c.region_code=a.region_code and c.province_code=a.province_code and c.city_group_code=a.city_group_code
left join
(
  --省区上月整月日配单业绩
  select 
    region_code,region_name,province_code,province_name,
    sum(sales_value)as H_province_sales_value,
    sum(profit)as H_province_profit
  from csx_dw.dws_sale_r_d_detail
  where sdt >=${hiveconf:sdate_sy1} and sdt <${hiveconf:sdate_by1}  --上月整月
  and channel_code in('1','9')
  and business_type_code in('1')
  and dc_code not in('W0K4')
  group by region_code,region_name,province_code,province_name
)d on c.region_code=d.region_code and c.province_code=d.province_code;

-------------------------------------------------------------------------------------

--02 指定客户本月每天业绩
insert overwrite table csx_dw.report_sale_r_d_customer_zhiding_detail partition(sdt)
select
  -- 唯一主键 
  concat_ws('&', a.province_code,a.city_group_code,a.customer_no,a.sdt) as biz_id,
  a.region_code,a.region_name,a.province_code,a.province_name,a.city_group_code,a.city_group_name,a.customer_no,b.customer_name,
  a.sales_value,
  a.profit,
  a.profit/abs(a.sales_value) profit_rate,
  day_province_sales_value,day_province_profit,
  day_province_profit/abs(day_province_sales_value) day_province_profit_rate,
  day_city_sales_value,day_city_profit,
  day_city_profit/abs(day_city_sales_value) day_city_profit_rate,
  month_province_sales_value,month_province_profit,
  month_province_profit/abs(month_province_sales_value) month_province_profit_rate,
  month_city_sales_value,month_city_profit,
  month_city_profit/abs(month_city_sales_value) month_city_profit_rate,
  'raoyanhua' create_by,
  ${hiveconf:i_sdate_dd} create_time,
  a.sdt
  --${hiveconf:sdate_by2} sdt  
from
(
  select 
    sdt,region_code,region_name,province_code,province_name,city_group_code,city_group_name,customer_no,
    sum( sales_value) sales_value,
    sum(profit) profit
  from csx_dw.dws_sale_r_d_detail
  where sdt >=${hiveconf:sdate_by1} and sdt <=${hiveconf:sdate_by2}  --本月第1天至本月昨日
  and channel_code in('1','9')
  and business_type_code in('1')
  and dc_code not in('W0K4')
  group by sdt,region_code,region_name,province_code,province_name,city_group_code,city_group_name,customer_no
)a 
join (select customer_no from csx_tmp.sales_customer_qingdan)a1 on a1.customer_no=a.customer_no
left join
--客户信息
(
  select 
    channel_name,sales_province_code,sales_province_name,customer_no,customer_name,  
    attribute_desc as attribute_name,first_category_name,second_category_name,third_category_name,
    work_no,sales_name,
    contact_person,contact_phone
  from csx_dw.dws_crm_w_a_customer 
  where sdt=${hiveconf:sdate_by2} 
  --and attribute_code ='1'
  and channel_code in('1','7','9')
)b on b.customer_no=a.customer_no
left join
(
  --城市本月月至今日配单业绩
  select 
    region_code,region_name,province_code,province_name,city_group_code,city_group_name, 
  sum(sales_value)as month_city_sales_value,
    sum(profit)as month_city_profit
  from csx_dw.dws_sale_r_d_detail
  where sdt >=${hiveconf:sdate_by1} and sdt <=${hiveconf:sdate_by2}  --本月月至今
  and channel_code in('1','9')
  and business_type_code in('1')
  and dc_code not in('W0K4')
  group by region_code,region_name,province_code,province_name,city_group_code,city_group_name
)c on c.region_code=a.region_code and c.province_code=a.province_code and c.city_group_code=a.city_group_code
left join
(
  --省区本月月至今日配单业绩
  select 
    region_code,region_name,province_code,province_name,
    sum(sales_value)as month_province_sales_value,
    sum(profit)as month_province_profit
  from csx_dw.dws_sale_r_d_detail
  where sdt >=${hiveconf:sdate_by1} and sdt <=${hiveconf:sdate_by2}  --本月月至今
  and channel_code in('1','9')
  and business_type_code in('1')
  and dc_code not in('W0K4')
  group by region_code,region_name,province_code,province_name
)d on c.region_code=d.region_code and c.province_code=d.province_code

left join
(
  --城市本月每天日配单业绩
  select 
    sdt,region_code,region_name,province_code,province_name,city_group_code,city_group_name, 
  sum(sales_value)as day_city_sales_value,
    sum(profit)as day_city_profit
  from csx_dw.dws_sale_r_d_detail
  where sdt >=${hiveconf:sdate_by1} and sdt <=${hiveconf:sdate_by2}  --本月
  and channel_code in('1','9')
  and business_type_code in('1')
  and dc_code not in('W0K4')
  group by sdt,region_code,region_name,province_code,province_name,city_group_code,city_group_name
)c1 on c1.region_code=a.region_code and c1.province_code=a.province_code and c1.city_group_code=a.city_group_code and c1.sdt=a.sdt
left join
(
  --省区本月每天日配单业绩
  select 
    sdt,region_code,region_name,province_code,province_name,
    sum(sales_value)as day_province_sales_value,
    sum(profit)as day_province_profit
  from csx_dw.dws_sale_r_d_detail
  where sdt >=${hiveconf:sdate_by1} and sdt <=${hiveconf:sdate_by2}  --本月
  and channel_code in('1','9')
  and business_type_code in('1')
  and dc_code not in('W0K4')
  group by sdt,region_code,region_name,province_code,province_name
)d1 on c1.region_code=d1.region_code and c1.province_code=d1.province_code and c1.sdt=d1.sdt;
