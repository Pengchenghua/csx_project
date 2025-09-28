-- ******************************************************************** 
-- @功能描述：
-- @创建者： 彭承华 
-- @创建者日期：2025-06-04 19:02:10 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 


-- create table csx_analyse_tmp.csx_analyse_tmp_new_cust_sale_01 as 
with tmp_sale_info as   
(select
  case when a.province_name  ='上海市' then '上海'
        when city_name in ('苏州市') then '上海'
        when city_name in ('深圳市') then '广东深圳' 
        when city_name in ('广州市') then '广东广州'
        when city_name in ('南京市') then '江苏南京'
        else province_name 
        end province_name  ,
  case when province_name in ('重庆市') then '重庆市' 
        when city_name in ('松江区') then '上海松江'
        when city_name in ('深圳市') then '广东深圳'
        when city_name in ('广州市') then '广东广州'
        when city_name in ('苏州市') then '江苏苏州'
        when city_name in ('南京市') then '南京主城'
        else a.city_name end city_name ,
  user_number,
  a.user_name,
  sub_position_name,
  begin_date,
  ceil(months_between(last_day(add_months('${sdt_yes_date}',-1)), from_unixtime(unix_timestamp(begin_date,'yyyyMMdd'),'yyyy-MM-dd'))) as work_age -- 工䶖月向上取
from csx_analyse.csx_analyse_fr_hr_red_black_sale_info a 
where smt=substr(regexp_replace(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),'-',''),1,6) 
 ) 
  ,tmp_sale_detail as (
  select
    b.month_of_year,
    quarter_of_year,
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    sales_user_number,
    sales_user_name,
    customer_code,
    customer_name,
    sub_customer_code,
    sub_customer_name,
    business_type_code,
    business_type_name,
    sum(sale_amt) sale_amt,
    sum(profit) profit
  from
    csx_dws.csx_dws_sale_detail_di a
    left join (
      select
        month_of_year,
        quarter_of_year,
        calday
      from
          csx_dim.csx_dim_basic_date
    ) b on a.sdt = b.calday
  where
    sdt >= '20250101'
    and business_type_code in (1) 
  group by
    month_of_year,
    quarter_of_year,
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    sales_user_number,
    sales_user_name,
    customer_code,
    customer_name,
    business_type_code,
    business_type_name,
    sub_customer_code,
    sub_customer_name
),
tmp_sever_info as (
  select
    *
  from
    (
      select
        substr(sdt, 1, 6) s_month,
        customer_code as customer_no,
        service_manager_user_number service_user_work_no,
        service_manager_user_name service_user_name,
        service_manager_user_id service_user_id,
        if( business_attribute_code = 5, 6,  business_attribute_code  ) attribute_code,
        business_attribute_name attribute_name,
        row_number() over( partition by customer_code, business_attribute_code order by service_manager_user_id asc ) as ranks,
        count(*) over( partition by customer_code, business_attribute_code  ) as manager_number
      from
        csx_dim.csx_dim_crm_customer_business_ownership
      where
        sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
        and shipper_code = 'YHCSX'
        and service_manager_user_id <> 0      
        and business_attribute_code = 1 
    ) a distribute by customer_no,
    attribute_code sort by customer_no,
    attribute_code,
    ranks
)
insert overwrite table csx_analyse.csx_analyse_service_user_cust_cnt_mf partition(smt)
select
  a.month_of_year as s_month,
  a.quarter_of_year,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  customer_code,
  customer_name,
  sub_customer_code,
  sub_customer_name,
  b.service_user_work_no,
  b.service_user_name,
  c.begin_date,
  c.work_age,
  c.sub_position_name,
  sale_amt sale_amt,
  profit profit,
  current_timestamp() update_time,
  sale_amt/manager_number as avg_sale_amt,
  profit/manager_number as avg_profit,
  manager_number,
  a.month_of_year
from
  tmp_sale_detail a
  left join tmp_sever_info b on a.customer_code = b.customer_no
  and a.business_type_code = b.attribute_code
  left join tmp_sale_info c on b.service_user_work_no = c.user_number


  -- 根据子客户地址计算管家服务客户数
 -- 管家数帆软报表SQL
with tmp_sale_info as   
(select  
  
  case when a.province_name  ='上海市' then '上海'
        when city_name in ('苏州市') then '上海'
        when city_name in ('深圳市') then '广东深圳' 
        when city_name in ('广州市') then '广东广州'
        when city_name in ('南京市') then '江苏南京'
        when city_name in ('盐城市') then '江苏南京'
        when province_name in ('东丰合资') then '东北'
        else province_name 
        end province_name
    ,
  case when province_name in ('重庆市') then '重庆市' 
        when city_name in ('松江区') then '上海松江'
        when city_name in ('深圳市') then '广东深圳'
        when city_name in ('广州市') then '广东广州'
        when city_name in ('苏州市') then '江苏苏州'
        when city_name in ('南京市') then '南京主城'
        when city_name in ('盐城市') then '江苏盐城'
        else a.city_name end city_name ,
  user_number,
  a.user_name,
  sub_position_name,
  begin_date
from
  csx_analyse.csx_analyse_fr_hr_red_black_sale_info a 
where smt='${smt}' 
  and  begin_date!=''
  ) 
  
select a.performance_region_name,
a.performance_province_name,
a.performance_city_name,
a.service_user_work_no,
a.service_user_name	,
a.begin_date,
b.sub_position_name,
max(work_age )work_age,
sum(cust_cnt) as cust_cnt,
sum(sub_customer_cnt) as sub_customer_cnt,
sum(sale_amt) as sale_amt,
sum(profit) as profit,
if(coalesce(sum(sale_amt),0)=0,0,sum(profit)/sum(sale_amt)) as profit_rate,
sum(last_cust_cnt) as last_cust_cnt,
sum(last_sub_customer_cnt) as last_sub_customer_cnt,
sum(last_sale_amt) as last_sale_amt,
sum(last_profit) as last_profit,
if(coalesce(sum(last_sale_amt),0)=0,0,sum(last_profit)/sum(last_sale_amt)) as last_profit_rate
from 
(select 
performance_region_name,
performance_province_name,
performance_city_name,
service_user_work_no,
service_user_name	,
begin_date,
max(work_age )work_age,
count(distinct case when s_month='${smt}' then  customer_code end ) cust_cnt,
sum(if(s_month='${smt}',1 ,0)) sub_customer_cnt,
sum(if(s_month='${smt}',avg_sale_amt,0))/10000 sale_amt,
sum(if(s_month='${smt}',avg_profit ,0))/10000 profit ,
count(distinct case when s_month='${l_smt}' then  customer_code end ) last_cust_cnt,
sum(if(s_month='${l_smt}',1 ,0)) last_sub_customer_cnt,
sum(if(s_month='${l_smt}',avg_sale_amt,0))/10000 last_sale_amt,
sum(if(s_month='${l_smt}',avg_profit ,0))/10000 last_profit 
(
 select s_month,
        performance_region_name,
        performance_province_name,
        performance_city_name,
        service_user_work_no,
        service_user_name	,
        begin_date,
        work_age,
        customer_code,
        receive_addr,
        sum(avg_sale_amt) as avg_sale_amt,
        sum(avg_profit) as avg_profit
 from  csx_analyse.csx_analyse_service_user_cust_cnt_mf

where smt >= '${l_smt}'
  and smt <= '${smt}'
  and sub_position_name in('服务管家（旧）','服务管家岗','客服经理','销售助理')
  group by s_month,
        performance_region_name,
        performance_province_name,
        performance_city_name,
        service_user_work_no,
        service_user_name	,
        begin_date,
        work_age,
        customer_code,
        receive_addr
)a 
group by 
performance_region_name,
performance_province_name,
performance_city_name,
begin_date,
service_user_work_no,
service_user_name
)a 
left join tmp_sale_info b on a.service_user_work_no=b.user_number
where service_user_work_no!='' 
and a.begin_date!=''
group by a.performance_region_name,
a.performance_province_name,
a.performance_city_name,
a.service_user_work_no,
a.service_user_name	,
a.begin_date,
b.sub_position_name
 order by 
 case a.performance_region_name  when '华南大区' then 1 
	when '华北大区' then 2 
	when '华西大区' then 3
	when '华东大区' then 4
	else 5 end ,
case
        when a.performance_province_name in('重庆市','安徽省', '北京市','福建省') then '1'
        when a.performance_province_name in('四川省', '江苏南京',  '河北省','江西省') then '2'
        when a.performance_province_name in('贵州省','广东深圳','河南省','浙江省') then '3'
        when a.performance_province_name in('上海','陕西省','广东广州') then '4'
        else '5'
    end,  
    case when a.performance_city_name in ('福州市','重庆主城区','杭州市','上海宝山','合肥市') then '1'  
when a.performance_city_name in ('厦门市','黔江区','宁波市','上海松江') then '2'  
when a.performance_city_name in ('泉州市','舟山市','江苏苏州','陕西') then '3' 
when a.performance_city_name in ('莆田市','台州市','江苏南京') then '4' 
when a.performance_city_name in ('南平市','东丰市','合肥市') then '5' 
when a.performance_city_name in ('三明市','大连市') then '6' 
when a.performance_city_name in ('宁德市','阜阳市') then '7' 
when a.performance_city_name in ('龙岩市') then '8' 
else '9' end