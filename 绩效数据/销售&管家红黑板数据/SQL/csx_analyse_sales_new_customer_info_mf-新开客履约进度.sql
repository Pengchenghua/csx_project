-- ******************************************************************** 
-- @功能描述：新客-日配断约3个月以上归属新客、福利BBC当月有商机及成交新客、纯新客
-- @创建者： 彭承华 
-- @创建者日期：2025-06-04 17:43:34 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 

set tez.am.resource.memory.mb=4096;
set hive.tez.container.size=8192;

-- 新开客日配断约3个月出的新开客
drop table  csx_analyse_tmp.csx_analyse_tmp_break_cust_flag_01;
create table csx_analyse_tmp.csx_analyse_tmp_break_cust_flag_01 as 
with tmp_break_cust AS (
SELECT 
    c.performance_province_name,
    c.performance_city_name,
    a.customer_code,
    customer_name,
    max_sdt,
    regexp_replace(date_add( from_unixtime(unix_timestamp(max_sdt, 'yyyyMMdd'),'yyyy-MM-dd'), 90), '-', '') syear,
    business_type_code,
    -- 新增断约判断字段
    CASE WHEN regexp_replace(date_add( from_unixtime(unix_timestamp(max_sdt, 'yyyyMMdd'),'yyyy-MM-dd'), 90) , '-', '') <=    regexp_replace(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),'-','') 
         THEN '是' ELSE '否' END AS is_break_3month
FROM
    (
    SELECT 
        customer_code,
        business_type_code,
        MAX(sdt) AS max_sdt
    FROM 
        csx_dws.csx_dws_sale_detail_di 
    WHERE 
        sdt BETWEEN '20200101' 
        -- 判断客户销售在上上月末次销售日期
        AND   regexp_replace(last_day(add_months('${sdt_yes_date}',-2)),'-','') 
        AND business_type_code IN ('1') 
        AND channel_code IN ('1','7','9')
        AND order_channel_code    not in ('4','6','5')  -- 剔除调价、返利、价格补救
        and refund_order_flag<>1    -- 剔除退货
    GROUP BY 
        customer_code,
        business_type_code
    ) a
LEFT JOIN   
    (
    SELECT *
    FROM csx_dim.csx_dim_crm_customer_info
    WHERE sdt = 'current'
        AND channel_code IN ('1','7','9')
    ) c ON a.customer_code = c.customer_code 
-- 动态筛选断约客户（保留历史数据可注释掉）
-- WHERE  regexp_replace(date_add( from_unixtime(unix_timestamp(max_sdt, 'yyyyMMdd'),'yyyy-MM-dd'), 90) , '-', '') <=  regexp_replace(last_day(add_months('${sdt_yes_date}',-2)),'-','')
-- and a.customer_code in ('100563','101543')

)
-- select * from tmp_break_cust where customer_code='130934'
,
-- 判断3个月以上客户断约后，新建商机属于新客
tmp_business as 
(select *,
    row_number()over(partition by customer_code order by business_sign_time desc ) rn
from 
(
select a.customer_code,
    business_sign_time,
    max_sdt,
    is_break_3month,
    a.business_type_code,
    syear
    -- if(from_unixtime(unix_timestamp(max_sdt, 'yyyyMMdd'),'yyyy-MM-dd')<= to_date(business_sign_time) ,1,0) is_flag,
from  csx_dim.csx_dim_crm_business_info a 
 join tmp_break_cust b on a.customer_code=b.customer_code 
where sdt='current'
-- 签约日期小于月初，取本月之交前
and to_date(business_sign_time) <   trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM')
and business_attribute_code=1
and status=1
) a 
-- where rn=1
) ,
-- 当月有销售的客户
tmp_sale as 
    (select substr(sdt,1,6) sale_month,
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        a.business_type_code,
        a.business_type_name,
        a.customer_code,
        customer_name,
        sales_user_name,
        sales_user_number,
        min(sdt) min_sdt,
        sum(sale_amt) sale_amt,
        sum(profit) profit
    from csx_dws.csx_dws_sale_detail_di a   
    where sdt >=   regexp_replace(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),'-','')
        and sdt <=   regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','') 
        and shipper_code='YHCSX'
        and business_type_code in  ('1','2','6','10')
        AND order_channel_code    not in ('4','6','5')  
        and refund_order_flag <>1    
    group by 
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        a.business_type_code,
        a.business_type_name,
        a.customer_code,
        customer_name,
        sales_user_name,
        sales_user_number ,
         substr(sdt,1,6)
    )
 -- 断约客户
 
    select a.*,
    b.max_sdt max_sdt,
    to_date(business_sign_time) business_sign_time,
    min_sdt as min_sale_sdt,
    -- 判断 断约前最后日期小于签约日期，且签约日期小于本月履约的最近日期，且is_break_3month='是' 为断约新开客
    case when  a.business_type_code='1' 
            and  (from_unixtime(unix_timestamp(b.max_sdt, 'yyyyMMdd'),'yyyy-MM-dd')<= to_date(business_sign_time) 
                 and to_date(business_sign_time) <=from_unixtime(unix_timestamp(min_sdt, 'yyyyMMdd'),'yyyy-MM-dd') 
                 and min_sdt<= regexp_replace( trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),'-','')
                 )
        then '1' else '0' end flag,
        is_break_3month,
    row_number()over(partition by a.customer_code,a.business_type_code) rn
    from tmp_sale a 
        left join 
        (select * from tmp_business a   
            where a.is_break_3month='是'
            AND a.rn=1
        )b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code
       
;




drop table  csx_analyse_tmp.csx_analyse_tmp_break_cust_01;
create table csx_analyse_tmp.csx_analyse_tmp_break_cust_01  as 
-- 准新客表
with  tmp_cust_active as 
(select
  customer_code,
  cast(business_type_code as string ) business_type_code,
  business_type_name,
  first_business_sign_date,
  first_business_sale_date min_sdt,
  last_business_sale_date max_sdt,
  substr(last_business_sale_date,1,6) as sale_month,
  '1' active_new_flag
 from csx_dws.csx_dws_crm_customer_business_active_di a
where sdt='current'
    and  first_business_sale_date >=  regexp_replace(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),'-','') 
        and first_business_sale_date <=    regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','') 
    
),
--  select * from tmp_cust_active a where active_new_flag=1
-- --  ;,
-- 商机新客 主要BBC\福利
tmp_business_new_cust as 
(
select a.customer_code,
    a.business_type_name,
    b.business_type_code,
    min_end_date,
    max(business_sign_date)  business_sign_date
from 
 (
  select
  customer_no customer_code,
  business_type_name,
  min(end_date) min_end_date  
 from  csx_analyse.csx_analyse_sale_d_customer_sign_new_about_di a   -- 商机新客取福利BBC
 left join 
 (select customer_code,
    performance_province_name,
    performance_region_name
 from csx_dim.csx_dim_crm_customer_info
    where sdt='current'
    ) b on a.customer_no= b.customer_code
where
   smonth =  substr(regexp_replace( trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),'-',''), 1, 6) 
  and business_type_code !=1 
  group by  customer_no  ,
     business_type_name
  
  ) a 
  left  join 
  (
  select customer_code,
    regexp_replace(to_date(business_sign_time ),'-','') business_sign_date , 
    business_number,
    business_type_code ,
    business_type_name
  from csx_dim.csx_dim_crm_business_info 
    where sdt='current' 
    -- and to_date(business_sign_time )<= date_add(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),19)
  ) b on a.customer_code=b.customer_code and min_end_date>=business_sign_date and a.business_type_name=b.business_type_name
group by a.customer_code,
    a.business_type_name,
    min_end_date,
    b.business_type_code
)
    
    select substr(regexp_replace(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),'-',''),1,6)  as sale_month,
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        a.sales_user_name,
        a.sales_user_number ,
        a.business_type_code,
        a.business_type_name,
        a.customer_code,
        a.customer_name,
        coalesce(a.business_sign_time,b.first_business_sign_date,c.business_sign_date) business_sign_date,
        coalesce(a.min_sale_sdt, b.min_sdt,min_end_date  ) first_sale_data,
        a.max_sdt as break_sale_sdt,            -- 断约日期末次销售 
        is_break_3month,
        -- flag,
        case when a.flag='1' and is_break_3month='是' then '1' 
            when b.customer_code is not null and b.active_new_flag='1' then '1'
            when  c.customer_code is not null then  '1'
        else '0' end new_flag 
    from 
      (select * from csx_analyse_tmp.csx_analyse_tmp_break_cust_flag_01
        where rn=1 ) a 
    left join tmp_cust_active b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code
    left join tmp_business_new_cust c on a.customer_code=c.customer_code and a.business_type_code=c.business_type_code
 ;
 
 
 
-- 新开客明细

-- create table csx_analyse_tmp.csx_analyse_tmp_new_cust_sale_01 as 
with tmp_sale_info as   
(select
  case when a.province_name  ='上海市' then '上海'
        when city_name in ('苏州市') then '上海'
        when city_name in ('深圳市') then '广东深圳' 
        when city_name in ('广州市') then '广东广州'
        when city_name in ('南京市') then '江苏南京'
        else province_name 
        end province_name
    ,
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
--   and sub_position_name in ('销售岗', '首席销售', '销售经理', '高级销售经理', '福利销售经理', '福利销售BD')
  and employee_status='在职'
 ) 
  ,
tmp_sale_detail as 
(select
  substr(sdt,1,6) s_month,
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
  sum(sale_amt) sale_amt,
  sum(profit) profit 
from
    csx_dws.csx_dws_sale_detail_di a 
where
   sdt >= regexp_replace(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),'-','')
  and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
 group by substr(sdt,1,6)  ,
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
    business_type_name
),
tmp_new_cust_sale as 
 (select
  b.s_month,
  b.performance_region_code,
  b.performance_region_name,
  b.performance_province_code,
  b.performance_province_name,
  b.performance_city_code,
  b.performance_city_name,
  b.sales_user_number,
  b.sales_user_name,
  c.begin_date,
  c.work_age,
  c.sub_position_name,
  b.customer_code,
  b.customer_name,
  b.business_type_code,
  b.business_type_name,
  (sale_amt) sale_amt,
  (profit) profit ,
  first_sale_data,
  business_sign_date,
  break_sale_sdt,
  is_break_3month,
  new_flag
from
  csx_analyse_tmp.csx_analyse_tmp_break_cust_01 a 
  left join 
  tmp_sale_detail b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code
  left join tmp_sale_info c on b.sales_user_number=c.user_number 
  where new_flag=1
  
  )
  insert overwrite table csx_analyse.csx_analyse_sales_new_customer_info_mf  partition(smt)
  select 
    s_month,
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    sales_user_number,
    sales_user_name,
    begin_date,
    work_age,
    sub_position_name,
    customer_code,
    customer_name,
    business_type_code,
    business_type_name,
    (sale_amt) sale_amt,
    (profit) profit ,
    first_sale_data,
    business_sign_date,
    break_sale_sdt,
    is_break_3month,
    new_flag,
   count(customer_code)over(partition by sales_user_number) cnt,
   current_timestamp() update_time,
   s_month as smt
  from  tmp_new_cust_sale 
--    where sub_position_name in ('销售岗', '首席销售', '销售经理', '高级销售经理', '福利销售经理', '福利销售BD')

; 