-- ******************************************************************** 
-- @功能描述：
-- @创建者： 饶艳华 
-- @创建者日期：2025-09-30 17:56:40 
-- @修改者日期：
-- @修改人：
-- @修改内容：更新系数,新方案测试
-- ******************************************************************** 

-- 计算管家提成分配系数
-- drop table  csx_analyse_tmp.csx_analyse_customer_sale_service_info_rate_qc_mi 
create table  csx_analyse_tmp.csx_analyse_customer_sale_service_info_rate_qc_mi as  
with 
tmp_position_dic as 
(select dic_key as code,dic_value as name
       from csx_ods.csx_ods_csx_b2b_ucenter_user_dic_df
       where sdt=regexp_replace(date_sub(current_date(),1),'-','')
       and dic_type = 'POSITION'
),
tmp_sales_info as (
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
        sdt = regexp_replace(last_day(add_months('${edate}',-1)),'-','')
        and status = 0
    ) b on a.leader_user_id = b.user_id
  where
    sdt = regexp_replace(last_day(add_months('${edate}',-1)),'-','')
    and status = 0
    )a 
    left join tmp_position_dic b on a.source_user_position=b.code
    left join tmp_position_dic c on a.leader_user_position=c.code
) 
,
tmp_business_sale_info as
(
select a.performance_region_name,
  a.performance_province_name,
  a.performance_city_name,
  a.customer_code,
  a.customer_name,
  rp.business_attribute_user_id as rp_user_id,
  rp.business_attribute_user_number as rp_user_number,
  rp.business_attribute_user_name as rp_user_name,
  rp.business_attribute_user_position as rp_user_position,
  fl.business_attribute_user_id as fl_user_id,
  fl.business_attribute_user_number as fl_user_number,
  fl.business_attribute_user_name as fl_user_name,
  fl.business_attribute_user_position as fl_user_position,
  bbc.business_attribute_user_id as bbc_user_id,
  bbc.business_attribute_user_number as bbc_user_number,
  bbc.business_attribute_user_name as bbc_user_name,
  bbc.business_attribute_user_position as bbc_user_position
from (
  select performance_region_name,
    performance_province_name,
    performance_city_name,
    customer_code,
    customer_name
  from csx_dim.csx_dim_crm_customer_business_ownership
  where sdt = regexp_replace(last_day(add_months('${edate}', -1)), '-', '')
    and business_attribute_user_id <> 0
  group by performance_region_name,
    performance_province_name,
    performance_city_name,
    customer_code,
    customer_name
) a
left join (
  select customer_code,
    business_attribute_user_id,
    business_attribute_user_name,
    business_attribute_user_number,
    business_attribute_user_position
  from csx_dim.csx_dim_crm_customer_business_ownership
  where sdt = regexp_replace(last_day(add_months('${edate}', -1)), '-', '')
    and business_attribute_user_id <> 0
    and business_attribute_name = '日配'
  group by customer_code,
    business_attribute_user_id,
    business_attribute_user_name,
    business_attribute_user_number,
    business_attribute_user_position
) rp on a.customer_code = rp.customer_code
left join (
  select customer_code,
    business_attribute_user_id,
    business_attribute_user_name,
    business_attribute_user_number,
    business_attribute_user_position
  from csx_dim.csx_dim_crm_customer_business_ownership
  where sdt = regexp_replace(last_day(add_months('${edate}', -1)), '-', '')
    and business_attribute_user_id <> 0
    and business_attribute_name = '福利'
  group by customer_code,
    business_attribute_user_id,
    business_attribute_user_name,
    business_attribute_user_number,
    business_attribute_user_position
) fl on a.customer_code = fl.customer_code
left join (
  select customer_code,
    business_attribute_user_id,
    business_attribute_user_name,
    business_attribute_user_number,
    business_attribute_user_position
  from csx_dim.csx_dim_crm_customer_business_ownership
  where sdt = regexp_replace(last_day(add_months('${edate}', -1)), '-', '')
    and business_attribute_user_id <> 0
    and business_attribute_name = 'BBC'
  group by customer_code,
    business_attribute_user_id,
    business_attribute_user_name,
    business_attribute_user_number,
    business_attribute_user_position
) bbc on a.customer_code = bbc.customer_code
),
tmp_customer_info as (
  select distinct concat_ws('-',substr(regexp_replace(add_months('${edate}', -1), '-', ''),1, 6  ), a.customer_no ) as biz_id,
    a.customer_id,
    a.customer_no,
    a.customer_name,
    a.channel_code,
    a.channel_name,
    a.region_code,
    a.region_name,
    a.province_code,
    a.province_name,
    a.city_group_code,
    a.city_group_name,
    sales_id_new as sales_id,
    work_no_new as work_no,
    sales_name_new as sales_name,
    rp_user_id as rp_sales_id,
    rp_user_number as rp_sales_number,
    rp_user_name as rp_sales_name,
    rp_user_position as rp_sales_position,
    fl_user_id as fl_sales_id,
    fl_user_number as fl_sales_number,
    fl_user_name as fl_sales_name,
    fl_user_position as fl_sales_position,
    bbc_user_id as bbc_sales_id,
    bbc_user_number as bbc_sales_number,
    bbc_user_name as bbc_sales_name,
    bbc_user_position as bbc_sales_position,
    rp_service_user_id_new as rp_service_user_id,
    rp_service_user_work_no_new as rp_service_user_work_no,
    rp_service_user_name_new as rp_service_user_name,
    fl_service_user_id_new as fl_service_user_id,
    fl_service_user_work_no_new as fl_service_user_work_no,
    fl_service_user_name_new as fl_service_user_name,
    bbc_service_user_id_new as bbc_service_user_id,
    bbc_service_user_work_no_new as bbc_service_user_work_no,
    bbc_service_user_name_new as bbc_service_user_name,
    -- 销售系数
    0.6 as rp_sales_fp_rate,
    case
      when length(fl_service_user_id_new) <> 0
      and length(work_no_new) > 0 then 0.6
      when length(work_no_new) > 0 then 1
    end as fl_sales_fp_rate,
    case
      when length(bbc_service_user_id_new) <> 0
      and length(work_no_new) > 0 then 0.6
      when length(work_no_new) > 0 then 1
    end as bbc_sales_fp_rate,
    -- 管家系数
    0 as rp_service_user_fp_rate,
    0 as fl_service_user_fp_rate,
    0 as bbc_service_user_fp_rate,
    0 customer_profit_rate,
    0 city_profit_rate,
    from_utc_timestamp(current_timestamp(), 'GMT') update_time,
    substr(regexp_replace(add_months('${edate}', -1), '-', ''), 1,  6) as smt -- 统计日期
  from (
      select a.*
      from csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df a
      where sdt = regexp_replace(last_day(add_months('${edate}', -1)), '-', '')
    ) a
    left join tmp_business_sale_info b on a.customer_no = b.customer_code
) -- insert overwrite table   csx_analyse.csx_analyse_customer_sale_service_info_rate_qc_mi partition(smt)
select biz_id,
  a.customer_id,
  a.customer_no,
  b.customer_name,
  a.channel_code,
  a.channel_name,
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  a.city_group_code,
  a.city_group_name,
  sales_id,
  work_no,
  sales_name,
  c.user_position_name,
  rp_sales_id as rp_sales_id,
  rp_sales_number,
  rp_sales_name,
  d.user_position_name as rp_sales_position,
  fl_sales_id,
  fl_sales_number,
  fl_sales_name,
  f.user_position_name as fl_sales_position,
  bbc_sales_id,
  bbc_sales_number,
  bbc_sales_name,
  j.user_position_name as bbc_sales_position,
  rp_service_user_id,
  rp_service_user_work_no,
  rp_service_user_name,
  fl_service_user_id,
  fl_service_user_work_no,
  fl_service_user_name,
  bbc_service_user_id,
  bbc_service_user_work_no,
  bbc_service_user_name,
  case
    when strategy_status = 1
    and coalesce(rp_service_user_work_no, '') = '' then 0.5
    when strategy_status = 1
    and coalesce(rp_service_user_work_no, '') != '' then 0.2
    else rp_sales_fp_rate
  end rp_sales_fp_rate,
  case
    when strategy_status = 1
    and coalesce(fl_service_user_work_no, '') = '' then 0.5
    when strategy_status = 1
    and coalesce(fl_service_user_work_no, '') != '' then 0.2
    when f.user_position_name in('福利销售BD','福利销售经理') then 0 
    else fl_sales_fp_rate
  end fl_sales_fp_rate,
  case
    when strategy_status = 1
    and coalesce(bbc_service_user_work_no, '') = '' then 0.5
    when strategy_status = 1
    and coalesce(bbc_service_user_work_no, '') != '' then 0.2
    when j.user_position_name in('福利销售BD','福利销售经理') then 0 
    else bbc_sales_fp_rate
  end bbc_sales_fp_rate,
  -- 按照新方案浙江、华西无销售员的管家按照旧方案系数	 
  -- 安徽(11)、浙江未挂销售的客户包含B,由管家独立维护,该客户分配系数申请按40%进行核算提成。
  -- 北京延迟一个月按照原方案执行
  rp_service_user_fp_rate,
  fl_service_user_fp_rate,
  bbc_service_user_fp_rate,
  update_time,
  customer_profit_rate,
  city_profit_rate,
  CASE
    WHEN a.city_group_name IN ('北京市','福州市','重庆主城','深圳市','成都市','上海松江','南京主城','合肥市','西安市','石家庄市','江苏苏州','杭州市','郑州市','广东广州') THEN 'A/B'
    WHEN a.city_group_name IN ('厦门市','宁波市','泉州市','莆田市','南平市','南昌市','贵阳市','宜宾','武汉市' ) THEN 'C'
    WHEN a.city_group_name IN ('三明市', '阜阳市', '台州市', '龙岩市', '万州区', '江苏盐城', '黔江区', '永川区') then 'D'
    else 'D'
  END as city_type_name,
  smt
from tmp_customer_info a
  left join (
    select customer_code,
      customer_name,
      strategy_status
    from csx_dim.csx_dim_crm_customer_info
    where sdt = 'current'
  ) b on a.customer_no = b.customer_code
 left join 
 tmp_sales_info c on a.work_no=c.user_number
 left join 
 tmp_sales_info d on a.rp_sales_number=d.user_number
  left join 
 tmp_sales_info f on a.fl_sales_number=f.user_number
   left join 
 tmp_sales_info j  on a.bbc_sales_number=j.user_number
;

select * from csx_analyse_tmp.csx_analyse_customer_sale_service_info_rate_qc_mi