-- ******************************************************************** 
-- @功能描述：新开客管家客户数
-- @创建者： 彭承华 
-- @创建者日期：2025-06-04 19:02:10 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 


-- 调整am内存
SET
  tez.am.resource.memory.mb = 4096;
-- 调整container内存
SET
  hive.tez.container.size = 8192;
  
select * from  csx_analyse_tmp.csx_analyse_tmp_new_cust_sale_01 
drop table csx_analyse_tmp.csx_analyse_tmp_new_cust_sale_01;
create table csx_analyse_tmp.csx_analyse_tmp_new_cust_sale_01 as 
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
-- where smt='202506'
 ) 
  ,
 tmp_sub_customer_info as (select
        c.customer_code,
        c.customer_name,
        b.sap_sub_cus_code as sub_customer_code,
        regexp_replace(regexp_replace(d.sub_customer_name,'\n',''),'\r','') as sub_customer_name,  -- d.delivery_area_code,d.delivery_area_name,
        regexp_replace(regexp_replace(a.receive_address_details,'\n',''),'\r','') as receive_address_details,
        d.sub_customer_status,
        a.status,
        row_number()over(partition by c.customer_code,b.sap_sub_cus_code order by a.create_time desc  ) rn 

from 
(
select id,
        customer_id,province_code,city_code,receive_address_details,status,create_time
from csx_ods.csx_ods_csx_crm_prod_receive_address_df
 )a 
left join 
(
select address_code,sap_cus_code,sap_sub_cus_code,create_time
from csx_ods.csx_ods_b2b_mall_prod_yszx_customer_address_relation_df
)b on a.id=b.address_code
left join 
(
select customer_id,customer_code,customer_name,channel_code
from csx_dim.csx_dim_crm_customer_info
where sdt='current'
)c on a.customer_id=c.customer_id
left join 
(
select sub_customer_code,sub_customer_name,delivery_area_code,delivery_area_name,
sub_customer_status   -- 子客户状态 0-禁用 1-正常
from csx_dim.csx_dim_csms_yszx_customer_relation
where sdt='current'
)d on b.sap_sub_cus_code=d.sub_customer_code
where c.customer_code is not null
),
 tmp_sale_detail as (
  select
    b.month_of_year,
    quarter_of_year,
    a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name,
    a.sales_user_number,
    a.sales_user_name,
    a.customer_code,
    a.customer_name,
    a.sub_customer_code,
    a.sub_customer_name,
    a.business_type_code,
    a.business_type_name,
    if(c.receive_address_details!='',receive_address_details, a.sub_customer_name) receive_addr,
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
    left join 
     (select * from  tmp_sub_customer_info  where rn=1) c
    on a.customer_code=c.customer_code and a.sub_customer_code=c.sub_customer_code
  where
    sdt >= '20250101'
    and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
    and business_type_code in (1) 
  group by
    month_of_year,
    quarter_of_year,
    a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name,
    a.sales_user_number,
    a.sales_user_name,
    a.customer_code,
    a.customer_name,
    a.business_type_code,
    a.business_type_name,
    a.sub_customer_code,
    a.sub_customer_name,
    if(c.receive_address_details!='',receive_address_details, a.sub_customer_name) 
),
tmp_customer_info as (
  select     
    customer_code as customer_no,
    customer_name,
    sales_user_number as work_no,
    sales_user_name as sales_name,
    first_sign_time,
    b.sub_position_name
  from  csx_dim.csx_dim_crm_customer_info a 
  left join tmp_sale_info b on a.sales_user_number=b.user_number
  where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
   and shipper_code = 'YHCSX'
  -- customer_type_code	int	客户类型编码(1线索 4合作)
  and customer_type_code=4
  ),
tmp_sever_info as (
select * ,
-- 求人数
count(distinct service_user_number)over(partition by customer_no) manager_number
from (
SELECT
  customer_no,
  t1.service_user_work_no AS service_user_number,
  t2.service_user_name AS service_user_name,
  '1' AS attribute_code
FROM csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
LATERAL VIEW posexplode(split(rp_service_user_work_no_new, '、')) t1 AS pos1, service_user_work_no
LATERAL VIEW posexplode(split(rp_service_user_name_new, '、')) t2 AS pos2, service_user_name
WHERE sdt = regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
  AND (rp_service_user_work_no_new <> '' OR rp_service_user_work_no_new IS NOT NULL)
  AND t1.pos1 = t2.pos2
  )a
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
  b.service_user_number,
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
  receive_addr,
  a.month_of_year
from
  tmp_sale_detail a
  left join tmp_sever_info b on a.customer_code = b.customer_no
  and a.business_type_code = b.attribute_code
  left join tmp_sale_info c on b.service_user_number = c.user_number
;



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
-- where smt='202506'
 ) ,
tmp_customer_info as (
  select     
    customer_code as customer_no,
    customer_name,
    sales_user_number as work_no,
    sales_user_name as sales_name,
    first_sign_time,
    b.sub_position_name
  from  csx_dim.csx_dim_crm_customer_info a 
  left join tmp_sale_info b on a.sales_user_number=b.user_number
  where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
   and shipper_code = 'YHCSX'
  -- customer_type_code	int	客户类型编码(1线索 4合作)
  and customer_type_code=4
  )
  ,
tmp_sever_info as 
(SELECT
  customer_no,
  t1.service_user_work_no AS service_user_number,
  t2.service_user_name AS service_user_name,
  '日配' AS attribute_name
FROM csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
LATERAL VIEW posexplode(split(rp_service_user_work_no_new, '、')) t1 AS pos1, service_user_work_no
LATERAL VIEW posexplode(split(rp_service_user_name_new, '、')) t2 AS pos2, service_user_name
WHERE sdt = '20250930'
  AND (rp_service_user_work_no_new <> '' OR rp_service_user_work_no_new IS NOT NULL)
  AND t1.pos1 = t2.pos2
UNION ALL
SELECT
  customer_no,
  t1.service_user_work_no AS service_user_number,
  t2.service_user_name AS service_user_name,
  '福利' AS attribute_name
FROM csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
LATERAL VIEW posexplode(split(fl_service_user_work_no_new, '、')) t1 AS pos1, service_user_work_no
LATERAL VIEW posexplode(split(fl_service_user_name_new, '、')) t2 AS pos2, service_user_name
WHERE sdt = '20250930'
  AND (fl_service_user_work_no_new <> '' OR fl_service_user_work_no_new IS NOT NULL)
  AND t1.pos1 = t2.pos2
UNION ALL
SELECT
  customer_no,
  t1.service_user_work_no AS service_user_number,
  t2.service_user_name AS service_user_name,
  'BBC' AS attribute_name
FROM csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
LATERAL VIEW posexplode(split(bbc_service_user_work_no_new, '、')) t1 AS pos1, service_user_work_no
LATERAL VIEW posexplode(split(bbc_service_user_name_new, '、')) t2 AS pos2, service_user_name
WHERE sdt = '20250930'
  AND (bbc_service_user_work_no_new <> '' OR bbc_service_user_work_no_new IS NOT NULL)
  AND t1.pos1 = t2.pos2
  
)




SELECT
  customer_no,
  service_user_work_no AS service_user_number,
  service_user_name AS service_user_name,
  '日配' AS attribute_name
FROM csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
LATERAL VIEW explode(split(rp_service_user_work_no_new, ',')) t1 AS service_user_work_no
LATERAL VIEW explode(split(rp_service_user_name_new, ',')) t2 AS service_user_name
WHERE sdt = '20250930'
  AND (rp_service_user_work_no_new <> '' OR rp_service_user_work_no_new IS NOT NULL)

UNION ALL

SELECT
  customer_no,
  service_user_work_no,
  service_user_name,
  '福利' AS attribute_name
FROM csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
LATERAL VIEW explode(split(fl_service_user_work_no_new, ',')) t1 AS service_user_work_no
LATERAL VIEW explode(split(fl_service_user_name_new, ',')) t2 AS service_user_name
WHERE sdt = '20250930'
  AND (fl_service_user_work_no_new <> '' OR fl_service_user_work_no_new IS NOT NULL)

UNION ALL

SELECT
  customer_no,
  service_user_work_no,
  service_user_name,
  'BBC' AS attribute_name
FROM csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
LATERAL VIEW explode(split(bbc_service_user_work_no_new, ',')) t1 AS service_user_work_no
LATERAL VIEW explode(split(bbc_service_user_name_new, ',')) t2 AS service_user_name
WHERE sdt = '20250930'
  AND (bbc_service_user_work_no_new <> '' OR bbc_service_user_work_no_new IS NOT NULL)