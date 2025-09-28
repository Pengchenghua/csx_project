
-- 客户提成分配系数
with tmp_sale_detail as 
(
SELECT 
  substr(sdt,1,6) mon,
  a.performance_region_name,
  a.performance_province_name,
  a.performance_city_name,
  sum(sale_amt) sale_amt,
  sum(a.profit) profit,
  case 
  when abs(sum(a.sale_amt)) = 0 then 0 
  else sum(a.profit)/abs(sum(a.sale_amt)) 
end as profit_rate
from    csx_dws.csx_dws_sale_detail_di a
  where sdt>='20250801'
  and sdt<='20250831'
  and business_type_code='1'
  group by 
  substr(sdt,1,6) ,
  a.performance_region_name,
  a.performance_province_name,
  a.performance_city_name
  ) ,
tmp_customer_sale_detail as  
(SELECT 
  substr(sdt,1,6) mon,
  a.performance_region_name,
  a.performance_province_name,
  a.performance_city_name,
  a.customer_code,
  business_type_code,
  a.business_type_name,  
  sales_user_number,
  sum(sale_amt) sale_amt,
  sum(a.profit) profit,
  case 
  when abs(sum(a.sale_amt)) = 0 then 0 
  else sum(a.profit)/abs(sum(a.sale_amt)) 
end as profit_rate
from     csx_dws.csx_dws_sale_detail_di a
  where sdt>='20250801'
  and sdt<='20250831'
--   and business_type_code='1'
  group by 
  substr(sdt,1,6) ,
  a.performance_region_name,
  a.performance_province_name,
  a.performance_city_name,
  a.business_type_name,
  business_type_code,
  customer_code,
  sales_user_number
  ) ,
  tmp_service_info as 
  (select customer_no,
    work_no_new,
    sales_name_new,
    rp_service_user_work_no_new,
    rp_service_user_name_new,
    fl_service_user_work_no_new,
    fl_service_user_name_new,
    bbc_service_user_work_no_new,
    bbc_service_user_name_new
  from  csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
  where sdt='20250831'  
  )
  select a.*,b.profit_rate as are_profit_rate,
   case when c.work_no_new='' or c.work_no_new is null then '0.4'
        when a.profit_rate>=b.profit_rate then '0.2'
        when a.profit_rate< b.profit_rate then '0.1'
        else '0.4' end as allocation_coefficient,
       c.work_no_new,
    c.sales_name_new,
    c.rp_service_user_work_no_new,
    c.rp_service_user_name_new,
    c.fl_service_user_work_no_new,
    c.fl_service_user_name_new,
    c.bbc_service_user_work_no_new,
    c.bbc_service_user_name_new
  from tmp_customer_sale_detail a 
  left join tmp_sale_detail b on a.performance_city_name=b.performance_city_name
  left join tmp_service_info c on a.customer_code=c.customer_no
  ;

 A类 北京、福州、重庆主城、深圳、成都、松江、南京、合肥、陕西、河北、苏州、杭州、
 B类 厦门、宁波、泉州、莆田、南平、江西、贵阳市\宜宾、湖北
 C类 阜阳、台州、龙岩、三明、万州、盐城、黔江

-- 取值说明：商机BBC餐卡与日配新客
-- 1、新客：2025年8月首单的客户销售与新客表进行 join，再通过城市目标值进行判断达成系数，
-- 新客履约含餐卡
-- 新客履约含餐卡
with tmp_business_info as (
  select
    customer_code,
    business_number,
    business_type_code,
    business_sign_time,
    start_date,
    other_needs_code,
    row_number() over(
      partition by customer_code,
      business_type_code
      order by
        business_sign_time desc
    ) rn
  FROM
    (
      select
        customer_code,
        business_number,
        cast(business_type_code as string) business_type_code,
        from_unixtime(
          unix_timestamp(business_sign_time, 'yyyy-MM-dd HH:mm:ss')
        ) business_sign_time,
        regexp_replace(substr(business_sign_time, 1, 10), '-', '') start_date,
        case
          when business_type_code = 6
          and other_needs_code = '1' then '餐卡'
          when business_type_code = 6
          and (
            other_needs_code <> '1'
            or other_needs_code is null
          ) then '非餐卡'
          else '其他'
        end as other_needs_code
      FROM
        csx_dim.csx_dim_crm_business_info
      WHERE
        sdt = 'current'
        and business_stage = 5
        and business_sign_time < '2025-09-01 00:00:00' --
        and business_type_code in (1, 2, 6, 10)
        and shipper_code = 'YHCSX'
    
    ) a
),
-- 日配、bbc 新客1-20号，算全部的客户，20号的新客，算6月份的销售数据
tmp_cust_active as 
(select
  customer_code,
  cast(business_type_code as string ) business_type_code,
  business_type_name,
  first_business_sign_date,
  first_business_sale_date ,
  last_business_sale_date ,
  substr(last_business_sale_date,1,6) as sale_month,
  case when first_business_sale_date >=  regexp_replace(trunc(last_day(add_months('2025-09-23',-1)),'MM'),'-','') 
        and first_business_sale_date <   regexp_replace(date_add(trunc(last_day(add_months('2025-09-23',-1)),'MM'),19),'-','') 
        and business_type_code in (1,6) then '1' 
    when  first_business_sale_date >=  regexp_replace(trunc(last_day(add_months('2025-09-23',-1)),'MM'),'-','') 
        and first_business_sale_date <=    regexp_replace(last_day(add_months('2025-09-23',-1)),'-','') 
        and business_type_code in (2,10) 
        then '1'
    else '0' end active_new_flag,
    '1' new_cust_flag
 from csx_dws.csx_dws_crm_customer_business_active_di a
where sdt=regexp_replace(last_day(add_months('2025-09-23',-1)),'-','')
    and  first_business_sale_date >=  regexp_replace(date_add(trunc(last_day(add_months('2025-09-23',-1)),'MM'),0),'-','') 
),
tmp_sale_detail as (
  select
    performance_province_name,
    performance_city_name,
    customer_code,
    customer_name,
    business_type_code,
    business_type_name,
    sales_user_number,
    sales_user_name,
    substr(sdt, 1, 6) sale_month,
    sum(sale_amt) sale_amt,
    sum(profit) profit,
    sum(sale_qty) qty
  from
    csx_dws.csx_dws_sale_detail_di
  where
    sdt >= '20250801'
    and sdt <= '20250831'
    and business_type_code in ('1','2','10' ,'6')
  group by
    performance_province_name,
    performance_city_name,
    customer_code,
    customer_name,
    business_type_code,
    business_type_name,
    sales_user_number,
    sales_user_name,
    substr(sdt, 1, 6)
),
tmp_position_dic as 
(select dic_key as code,dic_value as name
       from csx_ods.csx_ods_csx_b2b_ucenter_user_dic_df
       where sdt=regexp_replace(date_sub(current_date(),1),'-','')
       and dic_type = 'POSITION'
),
tmp_sales_leader_info as (
  
  select a.*,b.name as user_position_name,c.name as leader_position_name from 
  (select
    a.user_id,
    a.user_number,
    a.user_name,
    a.source_user_position,
    a.leader_user_id,
    b.user_number as leader_user_number,
    b.user_name as leader_user_name,
    b.source_user_position as leader_user_position
  from
       csx_dim.csx_dim_uc_user a
    left join (
      select
        user_id,
        user_number,
        user_name,
        source_user_position
      from
        csx_dim.csx_dim_uc_user a
      where
        sdt = '20250922'
        and status = 0
    ) b on a.leader_user_id = b.user_id
  where
    sdt = '20250922'
    and status = 0
    )a 
    left join tmp_position_dic b on a.source_user_position=b.code
    left join tmp_position_dic c on a.leader_user_position=c.code
),
-- 判定日配+餐卡客户
tmp_new_customer_infor as (
  SELECT
    a.customer_code,
    a.business_type_code,
    a.business_type_name,
    a.first_business_sign_date,
    a.first_business_sale_date ,
    a.last_business_sale_date ,
    a.sale_month,
    a.active_new_flag,
    b.other_needs_code,
    b.business_sign_time,
    b.start_date,
    b.business_number,
    a.new_cust_flag 
  FROM
 (select customer_code,
    business_type_code,
    business_type_name,
    first_business_sign_date,
    first_business_sale_date ,
    last_business_sale_date ,
    sale_month,
    active_new_flag,
    new_cust_flag
  from tmp_cust_active
    ) a
    left join (
      select
        *
      from
        tmp_business_info
      where
        rn = 1
    ) b on a.customer_code = b.customer_code
    and a.business_type_code = b.business_type_code
) ,
tmp_city_plan as 
( select distinct performance_city_name,
CASE 
    WHEN a.performance_city_name IN ('北京市', '福州市', '重庆主城', '深圳市', '成都市', '上海松江', '南京主城', '合肥市', '西安市', '石家庄市', '江苏苏州', '杭州市','郑州市','广东广州') THEN 100
    WHEN a.performance_city_name IN ('厦门市', '宁波市', '泉州市', '莆田市', '南平市', '南昌市', '贵阳市', '宜宾', '武汉市') THEN 60
    WHEN a.performance_city_name IN ('三明市','阜阳市','台州市','龙岩市','万州区','江苏盐城','黔江区','永川区') then 0
    ELSE 0
  END AS city_category_score,
   CASE 
    WHEN a.performance_city_name IN ('北京市', '福州市', '重庆主城', '深圳市', '成都市', '上海松江', '南京主城', '合肥市', '西安市', '石家庄市', '江苏苏州', '杭州市','郑州市','广东广州') THEN 60
    WHEN a.performance_city_name IN ('厦门市', '宁波市', '泉州市', '莆田市', '南平市', '南昌市', '贵阳市', '宜宾', '武汉市') THEN 40
    WHEN a.performance_city_name IN ('三明市','阜阳市','台州市','龙岩市','万州区','江苏盐城','黔江区','永川区') then 20
    ELSE 20
  END AS sales_person_target
  from csx_dim.csx_dim_shop  a 
  where sdt='current'
  )
 
select
  a.performance_province_name,
  a.performance_city_name,
  b.business_type_name,
  b.other_needs_code,
  business_sign_time,
  b.first_business_sale_date,
  a.sales_user_number,
  a.sales_user_name,
  c.user_position_name, 
  a.customer_code,
  a.customer_name,
  case when c.user_position_name like '%销售经理%' then a.sales_user_number else c.leader_user_number end leader_user_number,
  case when c.user_position_name like '%销售经理%' then a.sales_user_name else c.leader_user_name end leader_user_name,
  case when c.user_position_name like '%销售经理%' then c.user_position_name else c.leader_position_name end leader_position_name,
  a.sale_amt/10000 sale_amt,
  a.profit/10000 profit,
  d.sales_person_target,
  d.city_category_score,
  case when (a.business_type_code in ('2','6','10') and user_position_name in ('销售岗','销售员（旧）','销售员','销售经理','高级销售经理') )
            or (a.business_type_code ='1' and user_position_name in ('福利销售BD','福利销售经理') )  then 1 
            else 0 end cross_business_flag,
  b.first_business_sign_date,
  b.first_business_sale_date ,
  b.last_business_sale_date ,
  b.sale_month,
  b.active_new_flag
--   case when  c.user_position_name in ('销售岗','销售员（旧）','销售员') then  a.sale_amt/10000/d.sales_person_target 
--   when  c.user_position_name like '%销售经理%'  then a.sale_amt/10000/d.city_category_score end   as sales_coefficient
from
  tmp_sale_detail a
  left join tmp_new_customer_infor b on a.customer_code = b.customer_code  and a.business_type_code = b.business_type_code
  left join tmp_sales_leader_info c on a.sales_user_number=c.user_number   and  a.sales_user_number = c.user_number
  left join tmp_city_plan d on a.performance_city_name=d.performance_city_name
  where b.customer_code is not null 
 
  ;



    
 select distinct performance_city_name,
CASE 
    WHEN a.performance_city_name IN ('北京市', '福州市', '重庆主城', '深圳市', '成都市', '上海松江', '南京主城', '合肥市', '西安市', '石家庄市', '江苏苏州', '杭州市','郑州市','广东广州') THEN 100
    WHEN a.performance_city_name IN ('厦门市', '宁波市', '泉州市', '莆田市', '南平市', '南昌市', '贵阳市', '宜宾', '武汉市') THEN 60
    WHEN a.performance_city_name IN ('三明市','阜阳市','台州市','龙岩市','万州区','江苏盐城','黔江区','永川区') then 0
    ELSE 0
  END AS city_category_score,
   CASE 
    WHEN a.performance_city_name IN ('北京市', '福州市', '重庆主城', '深圳市', '成都市', '上海松江', '南京主城', '合肥市', '西安市', '石家庄市', '江苏苏州', '杭州市','郑州市','广东广州') THEN 60
    WHEN a.performance_city_name IN ('厦门市', '宁波市', '泉州市', '莆田市', '南平市', '南昌市', '贵阳市', '宜宾', '武汉市') THEN 40
    WHEN a.performance_city_name IN ('三明市','阜阳市','台州市','龙岩市','万州区','江苏盐城','黔江区','永川区') then 20
    ELSE 20
  END AS sales_person_target
  from csx_dim.csx_dim_shop  a 
  where sdt='current'
  ;



-- 跨业务销售员
with tmp_position_dic as 
(select dic_key as code,dic_value as name
       from csx_ods.csx_ods_csx_b2b_ucenter_user_dic_df
       where sdt=regexp_replace(date_sub(current_date(),1),'-','')
       and dic_type = 'POSITION'
),
tmp_sales_leader_info as (
  select a.*,b.name as user_position_name,c.name as leader_position_name from 
  (select
    a.user_id,
    a.user_number,
    a.user_name,
    a.source_user_position,
    a.leader_user_id,
    b.user_number as leader_user_number,
    b.user_name as leader_user_name,
    b.source_user_position as leader_user_position
  from
       csx_dim.csx_dim_uc_user a
    left join (
      select
        user_id,
        user_number,
        user_name,
        source_user_position
      from
        csx_dim.csx_dim_uc_user a
      where
        sdt = '20250922'
        and status = 0
    ) b on a.leader_user_id = b.user_id
  where
    sdt = '20250922'
    and status = 0
    )a 
    left join tmp_position_dic b on a.source_user_position=b.code
    left join tmp_position_dic c on a.leader_user_position=c.code
)
      SELECT
        performance_region_name,
        performance_province_name,
        performance_city_name,
        customer_code,
        customer_name,
        sales_user_number,
        sales_user_name,
        sales_user_position,
        b.user_position_name,
        business_type_code,
        business_type_name,
        business_attribute_name,
        first_business_sale_date,
        substr(first_business_sale_date, 1, 6) first_sale_month
      FROM
          csx_dws.csx_dws_crm_customer_business_active_di a 
          left join tmp_sales_leader_info b on a.sales_user_number=b.user_number
      WHERE
        sdt = 'current' --   AND business_type_code = '1'
        AND shipper_code = 'YHCSX'
        and substr(first_business_sale_date, 1, 6) = '202508'
    