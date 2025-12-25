-- ******************************************************************** 
-- @功能描述：
-- @创建者： 彭承华 
-- @创建者日期：2023-07-18 16:08:36 
-- @修改者日期：
-- @修改人：
-- @修改内容：增加操作日志操作人
-- ******************************************************************** 


/*
-- 历史有多条默认bug忽略掉
select customer_code,complaint_code,deal_detail_id,id,sdt,*
from csx_analyse.csx_analyse_fr_oms_complaint_detail_new_di
where concat(complaint_code,'_',cast(deal_detail_id as string)) in(
'KS23080200000022_21432',
'KS23080200000023_21430',
'KS23051200000065_13441',
'KS23031600000037_10400',
'KS23061100000015_15486'
)
*/







-- 小时跑数增量更新
-- ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
-- ★★★★★★★ 不可行 排名剔重会出错 因为一个客诉可能多个责任部门，且责任部门可能变化导致无法准确排名剔重 ★★★★★★★
-- ★★★★★★★ 解决：complaint_code deal_detail_id 联合可确定唯一性
-- ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★



-- 调整am内存
SET tez.am.resource.memory.mb=4096;
-- 调整container内存
SET hive.tez.container.size=8192;

with 
-- 历史数据
oms_complaint_detail_history as (
	select *
	from csx_analyse.csx_analyse_fr_oms_complaint_detail_new_di
	where 
	-- sdt < regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}', 'yyyyMMdd'),'yyyy-MM-dd'),-31),'-','')
	--   and task_sync_time < '${sdt_yes_date}'   -- 任务同步时间
	--   and 
	complaint_status_code in (30, -1)
),
comp as (
  select
    *,
    concat(
      substr(create_time, 1, 4),
      '年',
      substr(create_time, 6, 2),
      '月',
      substr(create_time, 9, 2),
      '日'
    ) as complaint_date_time,
    substr(create_time, 12, 8) as complaint_time_de,
    regexp_replace(substr(complaint_deal_time, 1, 10), '-', '') as deal_date,
    round((unix_timestamp(complaint_deal_time) - unix_timestamp(create_time)) / 3600,2) as processing_time,
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
      when complaint_level = 4 then '一级'
      when complaint_level = -1
      or complaint_level is null
      or complaint_level = '' then '无等级'
      else '其他'
    end as complaint_level_name,
    coalesce(
      concat(
        first_level_department_name,
        '-',
        second_level_department_name
      ),
      ''
    ) as department_name,
    row_number() over(
      partition by performance_city_code,
      classify_small_code,
      sub_category_code,
      first_level_department_code
      order by
        create_time
    ) as rn,
    datediff(
      to_date(create_time),
      to_date(
        lead(create_time, 1, null) over(
          partition by performance_city_code,
          classify_small_code,
          sub_category_code,
          first_level_department_code,
          complaint_node_code
          order by
            create_time desc
        )
      )
    ) as diff_days_1,
    datediff(
      to_date(
        lag(create_time, 1, null) over(
          partition by performance_city_code,
          classify_small_code,
          sub_category_code,
          first_level_department_code,
          complaint_node_code
          order by
            create_time desc
        )
      ),
      to_date(create_time)
    ) as diff_days_2
  from csx_dws.csx_dws_oms_complaint_detail_di
  where shipper_code='YHCSX'
  -- 小时更新仅取最新的
  and (sdt >= regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}', 'yyyyMMdd'),'yyyy-MM-dd'),-31),'-','')
  or task_sync_time >= '${sdt_yes_date}'   -- 任务同步时间
  or complaint_status_code not in (30, -1)) 
),
user_info as (
	select distinct a.user_id,a.user_number,a.user_name,b.cost_center_name
	from 
	(
		select user_id,user_number,user_name
		from csx_dim.csx_dim_uc_user
		where sdt = 'current'
		and delete_flag = '0'
	)a 
	left join 
	(
		select
			employee_code,employee_name,begin_date, cost_center_code,cost_center_name,employee_org_code,employee_org_name,employee_org_name_level
			from csx_dim.csx_dim_basic_employee
			where sdt = 'current'
			and card_type='0'
-- 			and shipper_code='YHCSX'
	)b on a.user_number=b.employee_code 
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
    and shipper_code='YHCSX'
    and strategy_status = 1 -- 是否为战略客户 0否 1是
),

order_info as (
select 
  order_code,goods_code,
  goods_remarks,
  case 
  when (is_partner_order = 1 and is_partner_dc = 1) or is_town_server_dc = 1 then '前置仓' 
  when order_business_type='NORMAL' then '日配'
  when order_business_type='WELFARE' then '福利'
  when order_business_type='BIGAMOUNT_TRADE' then '大宗贸易'
  when order_business_type='INNER' then '内购'
  else order_business_type end as order_business_type,
  is_town_server_dc,
  is_partner_order,
  is_partner_dc,
  delivery_time,
  delivery_date,
  send_qty
from 
(
  select order_code,goods_code,
    -- 订单类型：NORMAL-普通单，WELFARE-福利单，BIGAMOUNT_TRADE-大宗贸易，INNER-内购单  
    -- is_town_server_dc	int	是否是城镇服务商2.0dc（0-不是 1-是）
    -- is_partner_order	int	是否是合伙人订单
    -- is_partner_dc	int	(主数据)是否是合伙人dc: 0-非合伙人dc 1-合伙人dc
	max(goods_remarks) as goods_remarks,
    min(order_business_type) as order_business_type,
    max(is_town_server_dc) as is_town_server_dc,
    max(is_partner_order) as is_partner_order,
    max(is_partner_dc) as is_partner_dc,
    max(delivery_time) as delivery_time,
    max(require_delivery_date) as delivery_date,
    sum(send_qty) as send_qty
  from csx_dwd.csx_dwd_csms_yszx_order_detail_di
  where sdt>='20210101'
  and shipper_code='YHCSX'
  -- 小时更新仅取最新的 对应关联卡时间
  and sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}', 'yyyyMMdd'),'yyyy-MM-dd'),-80),'-','')
  group by order_code,goods_code
)a
),

order_business as (
select 
order_code,
max(order_business_type) as order_business_type_2
from order_info
where order_business_type is not null and order_business_type<>''
group by order_code
),

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
  where sdt = '${sdt_yes}'
),
-- 历史月份用当月的等级，本月用上月的等级，下月需将本月修正为本月实际等级
-- 如果昨天是当月最后一天则用当月无需用上月
cust_level as (
  select
    customer_no,
    customer_large_level,
    month
  from csx_analyse.csx_analyse_report_sale_customer_level_mf
  where month < '${month}'
  and tag = 1
  
  union all
  select
    customer_no,
    customer_large_level,
    '${month}' as month
  from csx_analyse.csx_analyse_report_sale_customer_level_mf
  where month =if('${sdt_yes_date}'=last_day('${sdt_yes_date}'),'${month}',substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''),1,6)) 
  and tag = 1
),
trace as (
  select
    complaint_code,
    -- responsible_department_code,
    regexp_replace(
      concat_ws(',', collect_list(first_sponsor)),
      '[\\[\\]]',
      ''
    ) first_person,
    regexp_replace(
      concat_ws(',', collect_list(hand_person)),
      '[\\[\\]]',
      ''
    ) hand_person,
    regexp_replace(
      concat_ws(',', collect_list(end_person)),
      '[\\[\\]]',
      ''
    ) end_person
  from
    (
      select
        complaint_code,
        --  responsible_department_code,
        case
          when operator_type = 20 then create_by
        end as first_sponsor,
        -- 发起人
        case
          when operator_type in (40, 50) then create_by
        end as hand_person,
        case
          when operator_type in (70, 80) then create_by
        end as end_person -- 完结人
      from
        csx_dwd.csx_dwd_oms_complaint_department_trace_df --   where create_time >='2024-03-31 14:24:58.0'
        --   and complaint_code='KS24010900000051'
        where shipper_code='YHCSX'
      group by
        complaint_code,
        --   responsible_department_code,
        case
          when operator_type = 20 then create_by
        end,
        -- 发起人
        case
          when operator_type in (40, 50) then create_by
        end,
        case
          when operator_type in (70, 80) then create_by
        end
    ) a
  group by
    complaint_code -- responsible_department_code
),
refund_responsible_reason_list as (
  -- 客退责任单明细表
  select
    d.refund_code,
    -- 退货子单号
    d.responsible_code,
    -- 判责单号
    d.stock_process_type,
    -- 库存处理方式：1-报损 2-退供 3-调拨 4-二次上架
    e.order_no,
    e.change_title,
    e.product_code,
    e.change_content_before,
    e.change_content_after
  from
    (
      select
        sale_order_code,
        parent_refund_code,
        -- 退货主单号
        refund_code,
        -- 退货子单号
        stock_process_type,
        -- 库存处理方式：1-报损 2-退供 3-调拨 4-二次上架
        stock_process_confirm,
        -- 是否确认：0-待确认 1-已确认
        product_type,
        -- 商品归属部门: 1-委外品 2-工厂品 3-物流品
        refund_responsible_version,
        -- 客退版本号
        responsible_code,
        -- 判责单号
        product_code,
        -- 商品编码
        responsible_status,
        -- 判责单状态: 10-待判责 20-申述待处理 21-判责待处理 30-库存未处理 40-已完成 -1-已取消
        regexp_replace(responsible_departments, '\\[|\\]', '') as responsible_departments,
        -- 责任部门
        supplier_refund_nos,
        -- 退供单集合
        breakage_nos,
        -- 报损单集合
        transfer_nos -- 调拨单集合
      from csx_dwd.csx_dwd_oms_refund_responsible_detail_di 
        -- where sdt>=regexp_replace(trunc(add_months(date_format('${sdt_yes_date}', 'yyyy-MM-dd'),-1),'MM'),'-','')
        where shipper_code='YHCSX'
		-- 小时更新仅取最新的 对应关联卡时间
		and smt>=regexp_replace(substr(date_add(from_unixtime(unix_timestamp('${sdt_yes}', 'yyyyMMdd'),'yyyy-MM-dd'),-100), 1, 7),'-','')
 		
    ) d -- 订单变更记录表  客退来源的修改原因记录
    left join (
      select
        order_no,
        change_title,
        product_code,
        -- change_content,
        concat(
          '原始原因、',
          first_level_reason_before,
          '-',
          second_level_reason_before,
          '、',
          create_by_before
        ) as change_content_before,
        concat(
          change_title,
          '原因、',
          first_level_reason_after,
          '-',
          second_level_reason_after,
          '、',
          create_by_after
        ) as change_content_after
      from
        (
          select
            order_no,
            change_content,
            change_title,
            product_code,
            get_json_object(before_content, '$.firstLevelReason') as first_level_reason_before,
            -- 一级原因
            get_json_object(before_content, '$.secondLevelReason') as second_level_reason_before,
            -- 二级原因
            get_json_object(before_content, '$.reasonType') as reason_type_before,
            -- 原因类型
            get_json_object(before_content, '$.createBy') as create_by_before,
            -- 提交人
            get_json_object(before_content, '$.changeTime') as change_time_before,
            -- 提交时间
            get_json_object(after_content, '$.firstLevelReason') as first_level_reason_after,
            -- 一级原因
            get_json_object(after_content, '$.secondLevelReason') as second_level_reason_after,
            -- 二级原因
            get_json_object(after_content, '$.reasonType') as reason_type_after,
            -- 原因类型
            get_json_object(after_content, '$.createBy') as create_by_after,
            -- 提交人
            get_json_object(after_content, '$.changeTime') as change_time_after -- 提交时间
          from
            (
              select
                order_no,
                change_content,
                change_title,
                get_json_object(change_content_3, '$.afterContent') as after_content,
                -- 调整后原因
                get_json_object(change_content_3, '$.beforeContent') as before_content,
                -- 原始原因
                get_json_object(change_content_3, '$.productCode') as product_code -- 商品编码
              from
                (
                  select
                    *,
                    concat('{', change_content_2, '}') as change_content_3
                  from
                    (
                      select
                        *,
                        -- id,
                        -- create_user_id,
                        -- customer_data,
                        -- 第一步，去除change_content最外层的[]
                        -- X第二步， 此处不涉及由于 change_content 是用 逗号 分割的，会和数据中其他逗号混淆，为了避免分割错误，将其转为其他字符，这里尽量使用数据中不会出现的符号，我这里是将，转为了||
                        regexp_replace(change_content, '\\[\\{|\\}\\]', '') as change_content_1
                      from
                        csx_ods.csx_ods_csx_b2b_oms_sale_order_change_log_df -- where create_time>='2024-06-01'
                        -- where order_no='RH24052500000063'
                      where
                        operate_type = 2 -- 第三步，lateral view函数将“change_content_1”字段炸开进行扩展 将 change_content_1'|| },{'使用split函数分割转成多行
                    ) b lateral view explode(split(change_content_1, '\\},\\{')) shiftList As change_content_2
                ) a
            ) a
        ) a
    ) e on d.refund_code = e.order_no
    and d.product_code = e.product_code
),
-- 客诉驳回审核后，审核通过/驳回修改的原因
complaint_modify_trace_list as (
  select
    *,
    concat(
      '共享调整原因、',
      first_level_reason,
      '-',
      second_level_reason,
      '、',
      create_by
    ) as change_content_new,
    row_number() over(
      partition by complaint_no
      order by
        create_time asc
    ) as rno
  from
    csx_ods.csx_ods_csx_b2b_oms_complaint_modify_trace_df -- where complaint_no = 'KS24061800000076'
    where  shipper_code='YHCSX'
),
oms_sale_refund_order as (
  select
    d.responsible_code,
    -- 判责单号
    case
      when d.stock_process_type = 1 then '报损'
      when d.stock_process_type = 2 then '退供'
      when d.stock_process_type = 3 then '调拨'
      when d.stock_process_type = 4 then '二次上架'
      else d.stock_process_type
    end as stock_process_type,
    -- 库存处理方式：1-报损 2-退供 3-调拨 4-二次上架
    a.inventory_dc_code,
    -- 库存DC编码
    a.inventory_dc_name,
    -- 库存DC名称
    case
      a.source_type
      when 0 then '签收差异或退货'
      when 1 then '改单退货'
    end as source_type_name,
    -- 订单来源(0-签收差异或退货 1-改单退货)
    a.sdt as sdt_refund,
    -- 退货申请日期
    a.refund_code,
    -- 退货单号
    a.sale_order_code,
    -- 销售单号
    a.customer_code,
    -- 客户编码
    regexp_replace(regexp_replace(a.customer_name, '\n', ''), '\r', '') as customer_name,
    a.sub_customer_code,
    -- 子客户编码
    a.goods_code,
    -- 商品编码
    case
      a.has_goods
      when 0 then '无实物'
      when 1 then '有实物'
    end as has_goods_name,
    -- 是否有实物退回
    a.responsibility_reason,
    -- 定责原因
    regexp_replace(regexp_replace(a.reason_detail, '\n', ''), '\r', '') as reason_detail,
    -- 原因说明
    case
      when a.city_supplier_relation_refund_code <> '' then '前置仓'
      when a.order_business_type_code = 1 then '日配'
      when a.order_business_type_code = 2 then '福利'
      when a.order_business_type_code = 3 then '大宗贸易'
      when a.order_business_type_code = 4 then '内购'
    end as business_type_name,
    case
      a.delivery_type_code
      when 1 then '配送'
      when 2 then '直送'
      when 3 then '自提'
      when 4 then '直通'
    end as delivery_type_name,
    -- 配送方式: 1-配送 2-直送 3-自提 4-直通
    case
      a.refund_order_type_code
      when 0 then '差异单'
      when 1 then '退货单'
    end as refund_order_type_name,
    -- 退货单类型(0:差异单 1:退货单）
    a.refund_qty,
    -- 退货数量
    a.real_return_qty,
    -- 实际退货数量
    a.refund_total_amt,
    -- 退货总金额
    a.refund_scale_total_amt,
    -- 处理后退货金额
    a.create_by,
    a.sdt as sdt -- 统计日期
  from
    (
      select *
      from csx_dwd.csx_dwd_oms_sale_refund_order_detail_di -- where sdt>=regexp_replace(trunc(add_months(date_format('${sdt_yes_date}', 'yyyy-MM-dd'),-1),'MM'),'-','')
      where
        sdt >= '20211101' -- 因客诉表的最早日期是20211206
        and child_return_type_code in(1)
        and shipper_code='YHCSX'
        and parent_refund_code <> '' -- and first_level_reason_name <> '送货后调整数量'  -- 剔除一级退货原因编码 001送货后调整数量
        -- and reason_detail <> '仓位调整'                  -- 剔除仓位调整
		-- 小时更新仅取最新的 对应关联卡时间
		and smt>=regexp_replace(substr(date_add(from_unixtime(unix_timestamp('${sdt_yes}', 'yyyyMMdd'),'yyyy-MM-dd'),-80), 1, 7),'-','')
    ) a
    left join csx_dwd.csx_dwd_oms_refund_responsible_detail_di d on a.refund_code = d.refund_code
    and a.goods_code = d.product_code
),
complaint_modify_trace as (
  select
    a.complaint_no,
    a.change_content_new as change_content_1,
    b.change_content_new as change_content_2,
    c.change_content_new as change_content_3
  from
    (
      select *
      from complaint_modify_trace_list
      where rno = 1
    ) a
    left join (
      select *
      from complaint_modify_trace_list
      where rno = 2
    ) b on a.complaint_no = b.complaint_no
    left join (
      select *
      from complaint_modify_trace_list
      where rno = 3
    ) c on a.complaint_no = c.complaint_no
),
-- 确认编码来源 系统or人工
scm_code_confirm as (
	select a.*,
	b.order_code,
	if(c.confirm_product_code is not null,'AI',a.claimant_flag) as submit_by_flag
	from 
	(
	  select
	  demand_order_no,     --  需求单号
	  confirm_no,     --  编码确认单号
	  product_code,     --  商品编码
	  claimant_date,     --  认领时间
	  claimant_name,   -- 认领人名称
	  if(claimant=0,'系统','人工') as claimant_flag,
	  row_number() over(partition by demand_order_no,product_code order by id desc) as rno
	  from csx_ods.csx_ods_csx_b2b_scm_scm_code_confirm_df
	  where sdt='${sdt_yes}'
	)a
	left join 
	(
	select distinct demand_order_code,order_code
	from csx_dwd.csx_dwd_csms_yszx_demand_split_order_relation_df
	)b on a.demand_order_no=b.demand_order_code
	left join 
	(
	 select confirm_no,confirm_product_code
	  from csx_ods.csx_ods_csx_b2b_scm_scm_code_confirm_ai_record_df
	 group by confirm_no,confirm_product_code
	)c on a.confirm_no=c.confirm_no and a.product_code=c.confirm_product_code
	where a.rno=1
),
-- 编码确认换品任务
order_gd_detail as (
  select order_no,replace_product_code
  from csx_ods.csx_ods_csx_crm_prod_sale_order_check_task_df
  where status=1  -- 任务状态 2：待处理 1：已处理 3:已驳回
),
-- 根据据包裹单记录的装车单司机 承运商
oms_package_order_detail as (
select 
  a.sale_order_code,
  a.goods_code,
  case
  when a.delivery_type_code=1 then '配送'
  when a.delivery_type_code=2 then '直送'
  when a.delivery_type_code=3 then '自提'
  else a.delivery_type_code end as delivery_type_name,
  a.carrier, -- 承运商编码
  b.supplier_name as carrier_name, -- 承运商
  a.driver_name -- 司机姓名
from 
  (
    select -- package_code,
      sale_order_code,
      goods_code,
	  max(delivery_type_code) delivery_type_code, --	物流模式 1-配送 2-直送 3-自提
  	  -- entrucking_code, -- 装车单号
  	  carrier, -- 承运商
  	  driver_name, -- 司机姓名
  	  max(tms_flag) as tms_flag, -- 是否tms配送
  	  row_number() over(partition by sale_order_code,goods_code order by sum(send_total_amt) desc) as rno
    from csx_dwd.csx_dwd_oms_package_order_detail_di -- 包裹单商品明细 订单金额取出库金额？
    where shipper_code='YHCSX'
	-- 小时更新仅取最新的 对应关联卡时间
	and sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}', 'yyyyMMdd'),'yyyy-MM-dd'),-80),'-','')
    -- and tms_flag=1   -- tms配送的才有承运商和司机
	group by sale_order_code,goods_code,carrier,driver_name
  )a 
  left join 
  (
    select supplier_code,supplier_name
    from 
    (
      select supplier_code,supplier_name,
      row_number() over(partition by supplier_code order by update_time desc) as rno
      from csx_dim.csx_dim_tms_transport_supplier_df
      where sdt='current'
      and shipper_code='YHCSX'
    )a where rno=1
  )b on a.carrier=b.supplier_code 
where a.rno=1
),

-- 销售表中消息履约模式
sale_direct_delivery_type as 
(
select a.order_code,a.goods_code,a.direct_delivery_type,
  a.delivery_type_code,a.delivery_type_name,
  a.business_type_code,a.business_type_name,
  a2.extra as direct_delivery_large_type,
  concat(a2.extra,'-',a2.name) as new_direct_delivery_type
from 
(
  select order_code,goods_code,direct_delivery_type,
  delivery_type_code,delivery_type_name,
  business_type_code,business_type_name,
  row_number() over(partition by order_code,goods_code order by sale_amt desc) as rno
  from csx_dws.csx_dws_sale_detail_di
  where shipper_code='YHCSX'
  -- 小时更新仅取最新的 对应关联卡时间
  and sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}', 'yyyyMMdd'),'yyyy-MM-dd'),-80),'-','')  
)a 
left join 
(
  select `code`,name,extra
  from csx_dim.csx_dim_basic_topic_dict_df
  where parent_code = 'direct_delivery_type'
)a2 on cast(a.direct_delivery_type as string)=a2.`code`
where a.rno=1
),

-- 最新信息结果表
complaint_detail_new as (
select
  a.id,
  a.complaint_code,
  a.complaint_time,
  a.complaint_status_code,
  a.performance_region_code,
  a.performance_region_name,
  a.performance_province_code,
  a.performance_province_name,
  a.performance_city_code,
  a.performance_city_name,
  a.sale_order_code,
  a.customer_code,
  a.customer_name,
  a.sub_customer_code,
  a.sub_customer_name,
  a.sales_user_id,
  a.sales_user_number,
  a.sales_user_name,
  a.first_category_code,
  a.first_category_name,
  a.second_category_code,
  a.second_category_name,
  a.third_category_code,
  a.third_category_name,
  a.sign_company_code,
  a.sign_company_name,
  a.inventory_dc_code,
  a.inventory_dc_name,
  a.inventory_dc_province_code,
  a.inventory_dc_province_name,
  a.inventory_dc_city_code,
  a.inventory_dc_city_name,
  a.require_delivery_date,
  a.complaint_dimension,
  a.complaint_type_code,
  a.complaint_type_name,
  a.main_category_code,
  a.main_category_name,
  a.sub_category_code,
  a.sub_category_name,
  a.goods_code,
  a.goods_name,
  a.classify_large_code,
  a.classify_large_name,
  a.classify_middle_code,
  a.classify_middle_name,
  a.classify_small_code,
  a.classify_small_name,
  a.complaint_qty,
  a.unit_name,
  a.purchase_qty,
  a.purchase_unit_name,
  a.purchase_unit_rate,
  a.sale_price,
  a.complaint_amt,
  a.complaint_describe,
  regexp_replace(a.evidence_imgs, '\\[\\"|\\"\\]|\\[|\\]', '') as evidence_imgs,
  a.channel_type_code,
  a.responsible_user_id,
  a.responsible_user_name,
  a.designee_user_id,
  a.designee_user_name,
  a.first_level_department_code,
  a.first_level_department_name,
  a.second_level_department_code,
  a.second_level_department_name,
  a.department_responsible_user_id,
  a.department_responsible_user_name,
  a.replenishment_order_code,
  a.create_by_id,
  a.create_by,  --录入人
  a.create_time,
  a.update_by,
  a.update_time,
  a.complaint_deal_time,
  a.reason,
  regexp_replace(a.result, '\n|\t|\r|\,|\"|\\\\n', '') as result,
  -- result,
  a.plan,
  a.complaint_deal_status,
  a.disagree_reason,
  a.replay_report,
  a.reject_reason,
  a.disagreement_reason,
  a.complaint_reject_status,
  a.task_sync_time,
  a.complaint_date,
  a.basic_performance_province_code,
  a.basic_performance_province_name,
  a.basic_performance_city_code,
  a.basic_performance_city_name,
  a.feedback_user_id,
  a.feedback_user_name,
  a.feedback_time,
  a.refund_code,
  a.cancel_reason,
  a.need_process,
  a.generate_reason,
  a.process_result,
  a.deal_detail_id,
  a.complaint_node_code,
  a.complaint_node_name,
  a.recep_order_user_number,
  a.recep_order_by,
  a.complaint_date_time,
  a.complaint_time_de,
  a.deal_date,
  a.processing_time,
  a.complaint_status_name,
  a.complaint_deal_status_name,
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
  coalesce(e.rp_service_user_work_no_new, '') rp_service_user_work_no_new,
  coalesce(e.rp_service_user_name_new, '') rp_service_user_name_new,
  complaint_source as complaint_source_code,
  -- 客户来源
  case
    when complaint_source = 1 then '单独发起客诉'
    when complaint_source = 2 then '客退单生成'
    when complaint_source = 3 then '补货单生成'
    else complaint_source
  end complaint_source_name,
  '' customer_large_level,
  -- 客户等级
  f.customer_large_level as customer_large_level_name,
  t.first_person,
  -- 发起人
  t.hand_person,
  t.end_person,
  -- 完结人
  a.relation_order_code,
  coalesce(
    g.change_content_before,
    concat(
      '原始原因、',
      a.main_category_name,
      '-',
      a.sub_category_name,
      '、',
      a.create_by
    )
  ) as reason_original,
  g.change_content_after,
  h.change_content_1,
  h.change_content_2,
  h.change_content_3,
  i.sdt_refund,
  -- 退货申请日期
  i.has_goods_name,
  -- 是否有实物退回
  i.reason_detail,
  -- 原因说明
  i.refund_qty,
  -- 退货申请数量
  -- i.real_return_qty,  -- 实际退货数量
  -- i.refund_total_amt,     -- 退货总金额
  -- i.refund_scale_total_amt,     -- 处理后退货金额
  i.create_by as refund_create_by,
  -- 退货发起人
  d.delivery_date,
  -- 出库日期
  coalesce(m.delivery_type_name,l.delivery_type_name,i.delivery_type_name) as delivery_type_name,
  -- 物流模式: 1-配送 2-直送 3-自提 4-直通
  i.stock_process_type,
  -- 库存处理方式：1-报损 2-退供 3-调拨 4-二次上架
  a.biz_id,
  a.complaint_category, -- 客诉类别:1-正常客诉 2-超时客诉
  b.cost_center_name,
  d.send_qty,
  coalesce(
  case 
  when j.submit_by_flag is not null then j.submit_by_flag
  when k.replace_product_code is not null then '人工—换品'
  end,'-') as submit_by_flag,
  l.carrier, -- 承运商编码
  l.carrier_name, -- 承运商
  l.driver_name, -- 司机姓名
  coalesce(d2.order_business_type_2,d.order_business_type,i.business_type_name) as business_type_name,   -- 业务类型
  d.goods_remarks,
  a.supplier_info,
  m.new_direct_delivery_type
from
  comp a
  left join user_info b on b.user_id = a.create_by_id
  left join cust c on c.customer_code = a.customer_code
  left join order_info d on a.sale_order_code = d.order_code and a.goods_code = d.goods_code
  left join order_business d2 on a.sale_order_code = d2.order_code
  left join service e on a.customer_code = e.customer_no
  left join cust_level f on a.customer_code = f.customer_no and substr(a.sdt,1,6)=f.month
  left join trace t on a.complaint_code = t.complaint_code
  left join refund_responsible_reason_list g on a.relation_order_code = g.responsible_code and a.goods_code = g.product_code
  left join complaint_modify_trace h on a.complaint_code = h.complaint_no
  left join oms_sale_refund_order i on a.relation_order_code = i.responsible_code and a.goods_code = i.goods_code -- and a.second_level_department_code=t.responsible_department_code
  -- 确认编码来源 系统or人工
  left join scm_code_confirm j on a.sale_order_code = j.order_code and a.goods_code = j.product_code
  -- 编码确认换品任务
  left join order_gd_detail k on a.sale_order_code = k.order_no and a.goods_code = k.replace_product_code
  -- 根据据包裹单记录的装车单司机 承运商
  left join oms_package_order_detail l on a.sale_order_code = l.sale_order_code and a.goods_code = l.goods_code
  left join sale_direct_delivery_type m on a.sale_order_code = m.order_code and a.goods_code = m.goods_code
  -- left join sale_direct_delivery_type n on a.relation_order_code = n.order_code and a.goods_code = n.goods_code  
),

complaint_detail_new_and_history as
(
-- 历史与新数据合并
 select *,
    row_number() over(partition by biz_id order by task_sync_time desc, table_no asc) as complaint_code_rank,
	count(1) over(partition by complaint_code) as complaint_cnt_win
from
( 
select
  '1' as table_no,
  a.id,
  a.complaint_code,
  a.complaint_time,
  a.complaint_status_code,
  a.performance_region_code,
  a.performance_region_name,
  a.performance_province_code,
  a.performance_province_name,
  a.performance_city_code,
  a.performance_city_name,
  a.sale_order_code,
  a.customer_code,
  a.customer_name,
  a.sub_customer_code,
  a.sub_customer_name,
  a.sales_user_id,
  a.sales_user_number,
  a.sales_user_name,
  a.first_category_code,
  a.first_category_name,
  a.second_category_code,
  a.second_category_name,
  a.third_category_code,
  a.third_category_name,
  a.sign_company_code,
  a.sign_company_name,
  a.inventory_dc_code,
  a.inventory_dc_name,
  a.inventory_dc_province_code,
  a.inventory_dc_province_name,
  a.inventory_dc_city_code,
  a.inventory_dc_city_name,
  a.require_delivery_date,
  a.complaint_dimension,
  a.complaint_type_code,
  a.complaint_type_name,
  a.main_category_code,
  a.main_category_name,
  a.sub_category_code,
  a.sub_category_name,
  a.goods_code,
  a.goods_name,
  a.classify_large_code,
  a.classify_large_name,
  a.classify_middle_code,
  a.classify_middle_name,
  a.classify_small_code,
  a.classify_small_name,
  a.complaint_qty,
  a.unit_name,
  a.purchase_qty,
  a.purchase_unit_name,
  a.purchase_unit_rate,
  a.sale_price,
  a.complaint_amt,
  a.complaint_describe,
  a.evidence_imgs,
  a.channel_type_code,
  a.responsible_user_id,
  a.responsible_user_name,
  a.designee_user_id,
  a.designee_user_name,
  a.first_level_department_code,
  a.first_level_department_name,
  a.second_level_department_code,
  a.second_level_department_name,
  a.department_responsible_user_id,
  a.department_responsible_user_name,
  a.replenishment_order_code,
  a.create_by_id,
  a.create_by,  -- 录入人
  a.create_time,
  a.update_by,
  a.update_time,
  a.complaint_deal_time,
  a.reason,
  a.`result`,
  -- result,
  a.plan,
  a.complaint_deal_status,
  a.disagree_reason,
  a.replay_report,
  a.reject_reason,
  a.disagreement_reason,
  a.complaint_reject_status,
  a.task_sync_time,
  a.complaint_date,
  a.basic_performance_province_code,
  a.basic_performance_province_name,
  a.basic_performance_city_code,
  a.basic_performance_city_name,
  a.feedback_user_id,
  a.feedback_user_name,
  a.feedback_time,
  a.refund_code,
  a.cancel_reason,
  a.need_process,
  a.generate_reason,
  a.process_result,
  a.deal_detail_id,
  a.complaint_node_code,
  a.complaint_node_name,
  a.recep_order_user_number,
  a.recep_order_by,
  a.complaint_date_time,
  a.complaint_time_de,
  a.deal_date,
  a.processing_time,
  a.complaint_status_name,
  a.complaint_deal_status_name,
  a.create_by_user_number,
  a.detail_plan,
  a.complaint_level,
  a.complaint_level_name,
  a.department_name,
  a.sdt,
  a.strategy_status_name,
  a.strategy_user_number,
  a.strategy_user_name,
  a.is_repeat,
  a.delivery_time,
  a.rp_service_user_work_no_new,
  a.rp_service_user_name_new,
  a.complaint_source_code,
  -- 客户来源
  a.complaint_source_name,
  '' customer_large_level,
  -- 客户等级
  a.customer_large_level_name,
  a.first_person,
  -- 发起人
  a.hand_person,
  a.end_person,
  -- 完结人
  a.relation_order_code,
  a.reason_original,
  a.change_content_after,
  a.change_content_1,
  a.change_content_2,
  a.change_content_3,
  a.sdt_refund,
  -- 退货申请日期
  a.has_goods_name,
  -- 是否有实物退回
  a.reason_detail,
  -- 原因说明
  a.refund_qty,
  -- 退货申请数量
  a.refund_create_by,
  -- 退货发起人
  a.delivery_date,
  -- 出库日期
  a.delivery_type_name,
  -- 物流模式: 1-配送 2-直送 3-自提 4-直通
  a.stock_process_type,
  -- 库存处理方式：1-报损 2-退供 3-调拨 4-二次上架
  a.biz_id,
  a.complaint_category, -- 客诉类别:1-正常客诉 2-超时客诉
  a.cost_center_name,
  a.send_qty,
  a.submit_by_flag,
  a.carrier, -- 承运商编码
  a.carrier_name, -- 承运商
  a.driver_name, -- 司机姓名
  a.business_type_name,   -- 业务类型
  a.goods_remarks,
  a.supplier_info,
  a.new_direct_delivery_type
from complaint_detail_new a 
union all 
-- 历史数据
select 
  '2' as table_no,
  a.id,
  a.complaint_code,
  a.complaint_time,
  a.complaint_status_code,
  a.performance_region_code,
  a.performance_region_name,
  a.performance_province_code,
  a.performance_province_name,
  a.performance_city_code,
  a.performance_city_name,
  a.sale_order_code,
  a.customer_code,
  a.customer_name,
  a.sub_customer_code,
  a.sub_customer_name,
  a.sales_user_id,
  a.sales_user_number,
  a.sales_user_name,
  a.first_category_code,
  a.first_category_name,
  a.second_category_code,
  a.second_category_name,
  a.third_category_code,
  a.third_category_name,
  a.sign_company_code,
  a.sign_company_name,
  a.inventory_dc_code,
  a.inventory_dc_name,
  a.inventory_dc_province_code,
  a.inventory_dc_province_name,
  a.inventory_dc_city_code,
  a.inventory_dc_city_name,
  a.require_delivery_date,
  a.complaint_dimension,
  a.complaint_type_code,
  a.complaint_type_name,
  a.main_category_code,
  a.main_category_name,
  a.sub_category_code,
  a.sub_category_name,
  a.goods_code,
  a.goods_name,
  a.classify_large_code,
  a.classify_large_name,
  a.classify_middle_code,
  a.classify_middle_name,
  a.classify_small_code,
  a.classify_small_name,
  a.complaint_qty,
  a.unit_name,
  a.purchase_qty,
  a.purchase_unit_name,
  a.purchase_unit_rate,
  a.sale_price,
  a.complaint_amt,
  a.complaint_describe,
  a.evidence_imgs,
  a.channel_type_code,
  a.responsible_user_id,
  a.responsible_user_name,
  a.designee_user_id,
  a.designee_user_name,
  a.first_level_department_code,
  a.first_level_department_name,
  a.second_level_department_code,
  a.second_level_department_name,
  a.department_responsible_user_id,
  a.department_responsible_user_name,
  a.replenishment_order_code,
  a.create_by_id,
  a.create_by,  -- 录入人
  a.create_time,
  a.update_by,
  a.update_time,
  a.complaint_deal_time,
  a.reason,
  a.`result`,
  -- result,
  a.plan,
  a.complaint_deal_status,
  a.disagree_reason,
  a.replay_report,
  a.reject_reason,
  a.disagreement_reason,
  a.complaint_reject_status,
  a.task_sync_time,
  a.complaint_date,
  a.basic_performance_province_code,
  a.basic_performance_province_name,
  a.basic_performance_city_code,
  a.basic_performance_city_name,
  a.feedback_user_id,
  a.feedback_user_name,
  a.feedback_time,
  a.refund_code,
  a.cancel_reason,
  a.need_process,
  a.generate_reason,
  a.process_result,
  a.deal_detail_id,
  a.complaint_node_code,
  a.complaint_node_name,
  a.recep_order_user_number,
  a.recep_order_by,
  a.complaint_date_time,
  a.complaint_time_de,
  a.deal_date,
  a.processing_time,
  a.complaint_status_name,
  a.complaint_deal_status_name,
  a.create_by_user_number,
  a.detail_plan,
  a.complaint_level,
  a.complaint_level_name,
  a.department_name,
  a.sdt,
  a.strategy_status_name,
  a.strategy_user_number,
  a.strategy_user_name,
  a.is_repeat,
  a.delivery_time,
  a.rp_service_user_work_no_new,
  a.rp_service_user_name_new,
  a.complaint_source_code,
  -- 客户来源
  a.complaint_source_name,
  '' customer_large_level,
  -- 客户等级
  a.customer_large_level_name,
  a.first_person,
  -- 发起人
  a.hand_person,
  a.end_person,
  -- 完结人
  a.relation_order_code,
  a.reason_original,
  a.change_content_after,
  a.change_content_1,
  a.change_content_2,
  a.change_content_3,
  a.sdt_refund,
  -- 退货申请日期
  a.has_goods_name,
  -- 是否有实物退回
  a.reason_detail,
  -- 原因说明
  a.refund_qty,
  -- 退货申请数量
  a.refund_create_by,
  -- 退货发起人
  a.delivery_date,
  -- 出库日期
  a.delivery_type_name,
  -- 物流模式: 1-配送 2-直送 3-自提 4-直通
  a.stock_process_type,
  -- 库存处理方式：1-报损 2-退供 3-调拨 4-二次上架
  a.biz_id,
  a.complaint_category, -- 客诉类别:1-正常客诉 2-超时客诉
  a.cost_center_name,
  a.send_qty,
  a.submit_by_flag,
  a.carrier, -- 承运商编码
  a.carrier_name, -- 承运商
  a.driver_name, -- 司机姓名
  a.business_type_name,   -- 业务类型
  a.goods_remarks,
  a.supplier_info,
  a.new_direct_delivery_type
from oms_complaint_detail_history a
)a
)

insert overwrite table csx_analyse.csx_analyse_fr_oms_complaint_detail_new_di
select
  a.id,
  a.complaint_code,
  a.complaint_time,
  a.complaint_status_code,
  a.performance_region_code,
  a.performance_region_name,
  a.performance_province_code,
  a.performance_province_name,
  a.performance_city_code,
  a.performance_city_name,
  a.sale_order_code,
  a.customer_code,
  a.customer_name,
  a.sub_customer_code,
  a.sub_customer_name,
  a.sales_user_id,
  a.sales_user_number,
  a.sales_user_name,
  a.first_category_code,
  a.first_category_name,
  a.second_category_code,
  a.second_category_name,
  a.third_category_code,
  a.third_category_name,
  a.sign_company_code,
  a.sign_company_name,
  a.inventory_dc_code,
  a.inventory_dc_name,
  a.inventory_dc_province_code,
  a.inventory_dc_province_name,
  a.inventory_dc_city_code,
  a.inventory_dc_city_name,
  a.require_delivery_date,
  a.complaint_dimension,
  a.complaint_type_code,
  a.complaint_type_name,
  a.main_category_code,
  a.main_category_name,
  a.sub_category_code,
  a.sub_category_name,
  a.goods_code,
  a.goods_name,
  a.classify_large_code,
  a.classify_large_name,
  a.classify_middle_code,
  a.classify_middle_name,
  a.classify_small_code,
  a.classify_small_name,
  a.complaint_qty,
  a.unit_name,
  a.purchase_qty,
  a.purchase_unit_name,
  a.purchase_unit_rate,
  a.sale_price,
  a.complaint_amt,
  a.complaint_describe,
  a.evidence_imgs,
  a.channel_type_code,
  a.responsible_user_id,
  a.responsible_user_name,
  a.designee_user_id,
  a.designee_user_name,
  a.first_level_department_code,
  a.first_level_department_name,
  a.second_level_department_code,
  a.second_level_department_name,
  a.department_responsible_user_id,
  a.department_responsible_user_name,
  a.replenishment_order_code,
  a.create_by_id,
  a.create_by,  -- 录入人
  a.create_time,
  a.update_by,
  a.update_time,
  a.complaint_deal_time,
  a.reason,
  a.`result`,
  -- result,
  a.plan,
  a.complaint_deal_status,
  a.disagree_reason,
  a.replay_report,
  a.reject_reason,
  a.disagreement_reason,
  a.complaint_reject_status,
  a.task_sync_time,
  a.complaint_date,
  a.basic_performance_province_code,
  a.basic_performance_province_name,
  a.basic_performance_city_code,
  a.basic_performance_city_name,
  a.feedback_user_id,
  a.feedback_user_name,
  a.feedback_time,
  a.refund_code,
  a.cancel_reason,
  a.need_process,
  a.generate_reason,
  a.process_result,
  a.deal_detail_id,
  a.complaint_node_code,
  a.complaint_node_name,
  a.recep_order_user_number,
  a.recep_order_by,
  a.complaint_date_time,
  a.complaint_time_de,
  a.deal_date,
  a.processing_time,
  a.complaint_status_name,
  a.complaint_deal_status_name,
  a.create_by_user_number,
  a.detail_plan,
  a.complaint_level,
  a.complaint_level_name,
  a.department_name,
  a.sdt,
  a.strategy_status_name,
  a.strategy_user_number,
  a.strategy_user_name,
  a.is_repeat,
  a.delivery_time,
  a.rp_service_user_work_no_new,
  a.rp_service_user_name_new,
  a.complaint_source_code,
  -- 客户来源
  a.complaint_source_name,
  '' customer_large_level,
  -- 客户等级
  a.customer_large_level_name,
  a.first_person,
  -- 发起人
  a.hand_person,
  a.end_person,
  -- 完结人
  a.relation_order_code,
  a.reason_original,
  a.change_content_after,
  a.change_content_1,
  a.change_content_2,
  a.change_content_3,
  a.sdt_refund,
  -- 退货申请日期
  a.has_goods_name,
  -- 是否有实物退回
  a.reason_detail,
  -- 原因说明
  a.refund_qty,
  -- 退货申请数量
  a.refund_create_by,
  -- 退货发起人
  a.delivery_date,
  -- 出库日期
  a.delivery_type_name,
  -- 物流模式: 1-配送 2-直送 3-自提 4-直通
  a.stock_process_type,
  -- 库存处理方式：1-报损 2-退供 3-调拨 4-二次上架
  a.biz_id,
  a.complaint_category, -- 客诉类别:1-正常客诉 2-超时客诉
  a.cost_center_name,
  a.send_qty,
  a.submit_by_flag,
  a.carrier, -- 承运商编码
  a.carrier_name, -- 承运商
  a.driver_name, -- 司机姓名
  a.business_type_name,   -- 业务类型
  a.goods_remarks,  -- 商品备注
  a.supplier_info,  -- 供应商
  a.new_direct_delivery_type  
from complaint_detail_new_and_history a 
where complaint_code_rank=1
and if(complaint_cnt_win > 1 and first_level_department_code is null and second_level_department_code is null and need_process<>0,
    false,true)
;



