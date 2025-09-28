-- 客诉详情
-- ******************************************************************** 
-- @功能描述：
-- @创建者： 张艳朋 
-- @创建者日期：2023-07-18 16:08:36 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 
with comp as (
    select
      *,
      concat(substr(create_time, 1, 4),'年',substr(create_time, 6, 2), '月',substr(create_time, 9, 2),'日'  ) as complaint_date_time,
      substr(create_time, 12, 8) as complaint_time_de,
      regexp_replace(substr(complaint_deal_time, 1, 10), '-', '') as deal_date,
      round((  unix_timestamp(complaint_deal_time) - unix_timestamp(create_time)) / 3600,  2 ) as processing_time,
      case
        when complaint_status_code = 10 then '待判责'
        when complaint_status_code = 20 then '处理中'
        when complaint_status_code = 21 then '待审核'
        when complaint_status_code = 30 then '已完成'
        when complaint_status_code = -1 then '已取消'
      end as complaint_status_name,
      case
        when complaint_deal_status = 10 then '待处理'
        when complaint_deal_status = 20 then '待修改'
        when complaint_deal_status = 30 then '已处理待审'
        when complaint_deal_status = 31 then '已驳回待审核'
        when complaint_deal_status = 40 then '已完成'
        when complaint_deal_status = -1 then '已取消'
      end as complaint_deal_status_name,
      case
        when complaint_level = 0 then '一级紧急'
        when complaint_level = 1 then '一级非紧急'
        when complaint_level = 2 then '二级'
        when complaint_level = 3 then '三级'
        when complaint_level = -1
        or complaint_level is null
        or complaint_level = '' then '无等级'
        else '其他'
      end as complaint_level_name,
      coalesce(concat(first_level_department_name,  '-', second_level_department_name  ), '' ) as department_name,
      row_number() over(partition by performance_city_code, classify_small_code,sub_category_code,  first_level_department_code   order by   create_time  ) as rn,
      datediff( to_date(create_time), to_date( lead(create_time, 1, null) over( partition by performance_city_code,  classify_small_code,  sub_category_code, first_level_department_code,  complaint_node_code
            order by create_time desc )  )   ) as diff_days_1,
      datediff( to_date( lag(create_time, 1, null) over(partition by performance_city_code,  classify_small_code, sub_category_code,first_level_department_code,complaint_node_code
            order by  create_time desc    )   ),to_date(create_time)  ) as diff_days_2
    from
      csx_dws.csx_dws_oms_complaint_detail_di
  ),
  user_info as (
    select
      user_id,
      user_number,
      user_name
    from
      csx_dim.csx_dim_uc_user
    where
      sdt = 'current'
      and delete_flag = '0'
    group by
      user_id,
      user_number,
      user_name
  ),
  cust as (
    select
      customer_code,
      strategy_status,
      strategy_user_id,
      strategy_user_number,
      strategy_user_name
    from
      csx_dim.csx_dim_crm_customer_info
    where
      sdt = 'current'
      and strategy_status = 1 -- 是否为战略客户 0否 1是
  ),
  order_info as (
    select order_code,delivery_time,rn 
    from (
        select order_code,delivery_time,row_number()over(partition by order_code order by delivery_time desc ) rn from (
            select
                order_code,
                delivery_time
            from
                csx_dws.csx_dws_sale_detail_di
        where
         sdt >= '20210101' -- 销售日期
        and sdt <= '${edate}'
        group by
        order_code,
        delivery_time
      union all 
      select
     order_no order_code,
      delivery_time
    from csx_report.csx_report_csms_yszx_order_hi
    group by   
      order_no,
      delivery_time
   ) a 
   ) a
  where rn = 1
  ) ,
  service as (
    select
      distinct -- sdt,
      customer_no,
      --  customer_name,
      region_name,
      province_name,
      city_group_name,
      rp_service_user_work_no_new,
      rp_service_user_name_new,
      fl_service_user_work_no_new,
      fl_service_user_name_new,
      bbc_service_user_work_no_new,
      bbc_service_user_name_new
    from
      -- csx_tmp.report_crm_w_a_customer_service_manager_info_business_new
      csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
    where
      sdt = regexp_replace( date_add( from_unixtime( unix_timestamp('${edate}', 'yyyyMMdd'),     'yyyy-MM-dd'  ),  -1),  '-',''  )
  ),
  cust_level as
  (
  select customer_no,customer_large_level,month
   from csx_analyse.csx_analyse_report_sale_customer_level_mf 
    where month='${month}'
    and tag=1
 ),
  trace as (select complaint_code,
    responsible_department_code,
    regexp_replace(concat_ws(',', collect_list(first_sponsor)), '[\\[\\]]', '') first_person,
    regexp_replace(concat_ws(',', collect_list(hand_person)), '[\\[\\]]', '') hand_person,
    regexp_replace(concat_ws(',', collect_list(end_person)), '[\\[\\]]', '') end_person
from (
  select complaint_code,
    responsible_department_code,
    case when operator_type=20 then create_by end as first_sponsor,     -- 发起人
    case when operator_type in (40,50) then create_by end as hand_person,
    case when operator_type in (70,80) then create_by end as end_person -- 完结人
  from 
       csx_dwd.csx_dwd_oms_complaint_department_trace_df
    --   where create_time >='2024-03-31 14:24:58.0'
    --   and complaint_code='KS24010900000051'
    group by complaint_code,
    responsible_department_code,
    case when operator_type=20 then create_by end,      -- 发起人
    case when operator_type in (40,50) then create_by end ,
    case when operator_type in (70,80) then create_by end
  ) a 
  group by 
  complaint_code,responsible_department_code)

insert overwrite table  csx_analyse.csx_analyse_fr_oms_complaint_detail_new_di
select
  id,
  a.complaint_code,
  complaint_time,
  complaint_status_code,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  sale_order_code,
  a.customer_code,
  customer_name,
  sub_customer_code,
  sub_customer_name,
  sales_user_id,
  sales_user_number,
  sales_user_name,
  first_category_code,
  first_category_name,
  second_category_code,
  second_category_name,
  third_category_code,
  third_category_name,
  sign_company_code,
  sign_company_name,
  inventory_dc_code,
  inventory_dc_name,
  inventory_dc_province_code,
  inventory_dc_province_name,
  inventory_dc_city_code,
  inventory_dc_city_name,
  require_delivery_date,
  complaint_dimension,
  complaint_type_code,
  complaint_type_name,
  main_category_code,
  main_category_name,
  sub_category_code,
  sub_category_name,
  goods_code,
  goods_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  classify_small_code,
  classify_small_name,
  complaint_qty,
  unit_name,
  purchase_qty,
  purchase_unit_name,
  purchase_unit_rate,
  sale_price,
  complaint_amt,
  complaint_describe,
  regexp_replace(evidence_imgs, '\\[\\"|\\"\\]|\\[|\\]', '') as evidence_imgs,
  channel_type_code,
  responsible_user_id,
  responsible_user_name,
  designee_user_id,
  designee_user_name,
  first_level_department_code,
  first_level_department_name,
  second_level_department_code,
  second_level_department_name,
  department_responsible_user_id,
  department_responsible_user_name,
  replenishment_order_code,
  create_by_id,
  create_by,
  create_time,
  update_by,
  update_time,
  complaint_deal_time,
  reason,
  regexp_replace(result, '\n|\t|\r|\,|\"|\\\\n', '') as result,
  -- result,
  plan,
  complaint_deal_status,
  disagree_reason,
  replay_report,
  reject_reason,
  disagreement_reason,
  complaint_reject_status,
  task_sync_time,
  complaint_date,
  basic_performance_province_code,
  basic_performance_province_name,
  basic_performance_city_code,
  basic_performance_city_name,
  feedback_user_id,
  feedback_user_name,
  feedback_time,
  refund_code,
  cancel_reason,
  need_process,
  generate_reason,
  process_result,
  deal_detail_id,
  complaint_node_code,
  complaint_node_name,
  recep_order_user_number,
  recep_order_by,
  complaint_date_time,
  complaint_time_de,
  deal_date,
  processing_time,
  complaint_status_name,
  complaint_deal_status_name,
  b.user_number as create_by_user_number,
  coalesce(a.detail_plan, '') as detail_plan,
  a.complaint_level,
  a.complaint_level_name,
  a.department_name,
  a.sdt,
  case
    when c.customer_code is not null then '是'
    else '否'
  end as strategy_status_name,
  coalesce(c.strategy_user_number, '') as strategy_user_number,
  coalesce(c.strategy_user_name, '') as strategy_user_name,
  case
    when a.diff_days_1 <= 6
    or a.diff_days_2 <= 6 then '是'
    else '否'
  end as is_repeat,
  d.delivery_time,
  coalesce(e.rp_service_user_work_no_new,'') rp_service_user_work_no_new,
  coalesce(e.rp_service_user_name_new,'') rp_service_user_name_new,
  complaint_source as complanit_source_code, -- 客户来源
  case when complaint_source=1 then '单独发起客诉'
    when  complaint_source=2 then '客退单生成'
    when  complaint_source=3 then '补货单生成'
    else  complaint_source 
    end complaint_source_name,
   '' customer_large_level, -- 客户等级
   customer_large_level as customer_large_level_name,
   first_person,   -- 发起人
   hand_person,
   end_person                       -- 完结人
from comp a
  left join user_info b on b.user_id = a.create_by_id
  left join cust c on c.customer_code = a.customer_code
  left join order_info d on a.sale_order_code = d.order_code
  left join service e on a.customer_code = e.customer_no
  LEFT JOIN cust_level f on a.customer_code=f.customer_no
  left join trace t on a.complaint_code = t.complaint_code and a.second_level_department_code=t.responsible_department_code
  -- where a.create_time>='2024-02-12 11:29:38.0'
  ;