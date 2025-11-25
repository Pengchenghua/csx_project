
-- ******************************************************************** 
-- @功能描述：客户提成对账结果表 csx_analyse_hr_customer_deferred_reconciliation_invoice_result_mf
-- @创建者： 彭承华 
-- @创建者日期：2025-08-27 17:10:22 
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
  
-- 加业务 对账结果表
-- create table csx_analyse_tmp.csx_analyse_tmp_hr_customer_bill_account_result_mi as 

with tmp_group as 
(
 select
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name ,
    customer_code,
    customer_name,
    business_attribute_name,
    work_no_new,
    sales_name_new,
    service_user_work_no,
    service_user_name,
    reconciliation_period,
    sum(last_sale_amt) last_sale_amt,
    sum(bill_amt_all) bill_amt_all,
    sum(last_bill_amt) last_bill_amt,
    sum(unbill_amt) unbill_amt,
    sum(unbill_amount_history) unbill_amount_history,
    sum(invoice_amount) invoice_amount,
    sum(no_invoice_amt) no_invoice_amt,
    sum(invoice_amount_all) invoice_amount_all,
    sum(no_invoice_amt_history) no_invoice_amt_history,
    sum(unbill_amt_all) unbill_amt_all,
    sum(no_invoice_amount_all) no_invoice_amount_all
from  ( select
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name ,
    customer_code,
    customer_name,
    business_attribute_name,
    work_no_new,
    sales_name_new,
    service_user_work_no,
    service_user_name,
    reconciliation_period,
    (last_sale_amt) last_sale_amt,
    (bill_amt_all) bill_amt_all,
    (last_bill_amt) last_bill_amt,
    (unbill_amt) unbill_amt,
    (unbill_amount_history) unbill_amount_history,
    (invoice_amount) invoice_amount,
    (no_invoice_amt) no_invoice_amt,
    (invoice_amount_all) invoice_amount_all,
    (no_invoice_amt_history) no_invoice_amt_history,
    unbill_amt_all,
    no_invoice_amount_all
from
     csx_analyse.csx_analyse_hr_customer_deferred_reconciliation_invoice_detail_mf 
     where smt=substr('${partition_date}',1,6)
 group by performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name ,
    customer_code,
    customer_name,
    business_attribute_name,
    work_no_new,
    sales_name_new,
    service_user_work_no,
    service_user_name,
    reconciliation_period,
    last_sale_amt,
    bill_amt_all,
    last_bill_amt,
    (unbill_amt) ,
    (unbill_amount_history) ,
    (invoice_amount) ,
    (no_invoice_amt) ,
    (invoice_amount_all) ,
    (no_invoice_amt_history),
    unbill_amt_all,
    no_invoice_amount_all
) a 
    group by performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name ,
    customer_code,
    customer_name,
    business_attribute_name,
    work_no_new,
    sales_name_new,
    service_user_work_no,
    service_user_name,
    reconciliation_period 
),
tmp_ruslt_01 as (
  select
    a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name,
    a.customer_code,
    a.customer_name,
    a.business_attribute_name,
    a.work_no_new,
    a.sales_name_new,
    a.service_user_work_no,
    a.service_user_name,
    a.reconciliation_period ,
    -- 判断未对账金额大于1000
    case when a.unbill_amt_all=0 then b.statement_date
    -- 当未对账不25年未对账<>0 且 累计未对账不等于0
        when coalesce(b1.unstatement_amount_all,0) >= 1000  and  a.unbill_amt_all>=1000 then  b1.statement_date else '' end  statement_date,
    -- a.bill_amt,
    case when a.unbill_amt_all=0 then b.confirm_date else '' end confirm_date,
    case when a.unbill_amt_all=0  then b.diff_confirm_days 
        when coalesce(b1.unstatement_amount_all,0) >= 1000  and  a.unbill_amt_all>=1000 then  b1.no_confirm_days 
    else 0 end  diff_confirm_days,      -- 未对帐天数
    case when a.unbill_amt_all>=1000 then b.statement_amount else 0 end  statement_amount,
    a.last_sale_amt, -- 上一结算期销售金额 
    a.last_bill_amt,     -- 上一结算期对账金额 
    if(coalesce(a.unbill_amt_all,0) <=1000,0,a.unbill_amt) unbill_amt,    -- 上一结算期未对账金额
    a.bill_amt_all, -- 历史对账金额 
    if(coalesce(b1.unstatement_amount_all,0)<>0,a.unbill_amount_history,0) unbill_amount_history,   -- 历史未对帐金额
    if(coalesce(a.unbill_amt_all,0) <=1000, '是', '否') as confirm_flag,   -- 是否完成对账
    case when a.no_invoice_amount_all=0 then c.statement_date 
        when  coalesce(c1.no_kp_amount_all,0) >=1000 and a.no_invoice_amount_all>=1000  then  c1.statement_date else '' end  as invoice_statement_date,     -- 开票对账日期
    case when a.no_invoice_amount_all=0 then c.invoice_date 
          else '' end as max_invoice_date,             -- 开票日期
    case when a.no_invoice_amount_all=0 then c.diff_invoice_days  
        when  coalesce(c1.no_kp_amount_all,0) >=1000 and a.no_invoice_amount_all>=1000  then coalesce(c1.no_invoice_days , '') end   diff_invoice_days,                          -- 开票天数
    case when a.no_invoice_amount_all>=1000 then c.kp_amount else  0  end      kp_amount,                          -- 开票金额
    a.invoice_amount    ,                           -- 上一期开票金额
    a.no_invoice_amt,                               -- 上一期未开票金额
    a.invoice_amount_all,                           -- 历史开票金额
    if(c1.no_kp_amount_all<>0,a.no_invoice_amt_history,0)  no_invoice_amt_history,                      -- 历史未开票金额
    if(coalesce(a.no_invoice_amount_all,0) <= 1000  , '是', '否') kp_flag,
    case  when coalesce(a.unbill_amt_all,0) = 0   then '否'
      else if(coalesce(b.diff_confirm_days, 0) > 20   and coalesce(b.diff_confirm_days, 0) <= 50, '是',  '否'  )  end as tc_bill_type,
    -- 对账缓发
    case when  coalesce(a.unbill_amt_all) = 0   then '否'
      else if(coalesce(b.diff_confirm_days, 0) > 50, '是', '否')
    end as tc_history_bill_type,
    -- 对账扣发
    case when   coalesce(a.no_invoice_amount_all,0) = 0 then '否'
      else 
      if( coalesce(c.diff_invoice_days, 0) > 25   and coalesce(c.diff_invoice_days, 0) <= 55,  '是', '否' )  end tc_invoice_type,
    case
      when  coalesce(a.no_invoice_amount_all,0)= 0  then '否'
       else if(coalesce(c.diff_invoice_days, 0) > 55, '是', '否')
    end tc_history_invoice_type ,
    b.statement_date b_statement_date,
    b1.statement_date as b1_statement_date,
    c.statement_date c_statement_date,
    c1.statement_date as c1_statement_date,
    a.unbill_amt_all,
    b.unstatement_amount_all,
    a.no_invoice_amount_all,
    c.no_kp_amount_all,
    a.unbill_amt as unbill_amt_old,
    a.no_invoice_amt no_invoice_amt_old
    -- if( coalesce(b.diff_confirm_days,0)<=50  and c.tc_bill_type='是' ,'是','否') as  tc_reissue_bill_type,  -- 对账扣发
    -- if( coalesce(b.diff_invoice_days,0) <=55 and c.tc_invoice_type='是' ,'是','否') tc_reissue_invoice_type,
  from tmp_group a
-- 对账天数最大的信息
    left join (
      select
        customer_code,
        business_attribute_name,
        confirm_date,
        diff_confirm_days_ro,
        diff_confirm_days,
        statement_date,
        statement_amount,
        order_no_kp_amount_history ,
        order_unstatement_amount,
        order_unstatement_amount_history,
        order_unstatement_amount_all as unstatement_amount_all,
        order_no_kp_amount_all as no_kp_amount_all
      from
       csx_analyse.csx_analyse_hr_customer_deferred_reconciliation_invoice_detail_mf 
     where smt=substr('${partition_date}',1,6)
      and 
        diff_confirm_days_ro = 1 
    ) b on a.customer_code = b.customer_code
    and a.business_attribute_name = b.business_attribute_name
-- 对账天数最大的信息,对账未完成的 unbill_amt_all不等于0
    left join (
      select
        customer_code,
        business_attribute_name,
        confirm_date,
        diff_confirm_days_ro,
        diff_confirm_days,
        statement_date,
        statement_amount,
        no_confirm_days,
        diff_no_confirm_days_ro,
        order_no_kp_amount_history ,
        order_unstatement_amount,
        order_unstatement_amount_history,
        order_unstatement_amount_all as unstatement_amount_all,
        order_no_kp_amount_all as no_kp_amount_all
      from
        csx_analyse.csx_analyse_hr_customer_deferred_reconciliation_invoice_detail_mf 
     where smt=substr('${partition_date}',1,6)
      and 
         diff_no_confirm_days_ro=1 
    ) b1 on a.customer_code = b1.customer_code
    and a.business_attribute_name = b1.business_attribute_name
-- 开票天数最大信息
    left join (
      select
        customer_code,
        business_attribute_name,
        confirm_date,
        diff_confirm_days_ro,
        diff_confirm_days,
        statement_date,
        kp_amount,
        no_invoice_days,
        invoice_date,
        diff_invoice_days,
        diff_invoice_days_ro,
         order_no_kp_amount_history ,
        order_unstatement_amount,
        order_unstatement_amount_history,
        order_unstatement_amount_all,
        order_no_kp_amount_all as no_kp_amount_all
      from
       csx_analyse.csx_analyse_hr_customer_deferred_reconciliation_invoice_detail_mf 
     where smt=substr('${partition_date}',1,6)
      and 
         diff_invoice_days_ro = 1
    ) c on a.customer_code = c.customer_code
    and a.business_attribute_name = c.business_attribute_name -- where a.customer_code='124059'
-- 开票天数最大信息,未完成开票 no_invoice_amount_all不等于0
    left join (
      select
        customer_code,
        business_attribute_name,
        confirm_date,
        diff_confirm_days_ro,
        diff_confirm_days,
        statement_date,
        kp_amount,
        invoice_date,
        no_invoice_days,
        diff_invoice_days,
        max_statement_date,
         order_no_kp_amount_history ,
        order_unstatement_amount,
        order_unstatement_amount_history,
        order_unstatement_amount_all,
        order_no_kp_amount_all as no_kp_amount_all
      from
       csx_analyse.csx_analyse_hr_customer_deferred_reconciliation_invoice_detail_mf 
     where smt=substr('${partition_date}',1,6)
      and 
        diff_no_invoice_days_ro=1
    ) c1 on a.customer_code = c1.customer_code
    and a.business_attribute_name = c1.business_attribute_name -- where a.customer_code='124059'
)
insert overwrite table  csx_analyse.csx_analyse_hr_customer_deferred_reconciliation_invoice_result_mf partition(smt)

select
  a.performance_region_code,
  a.performance_region_name,
  a.performance_province_code,
  a.performance_province_name,
  a.performance_city_code,
  a.performance_city_name,
  a.customer_code,
  a.customer_name,
  a.work_no_new   as sales_user_number,
  a.sales_name_new as sales_user_name,
  a.service_user_work_no,
  a.service_user_name,
  a.business_attribute_name,
  reconciliation_period,
  statement_date,
  -- a.bill_amt,
  (confirm_date ) as customer_confirm_date,
  (diff_confirm_days ) as diff_bill_days,
  invoice_statement_date,
  (max_invoice_date ) as invoice_date,
  (diff_invoice_days ) as diff_invoice_days,

    -- 对账缓发 
  case when confirm_flag='是' then '否'
        when diff_confirm_days >=20 and diff_confirm_days<=50 then '是'
    else '否' 
    end tc_bill_type,
-- 对账扣发
   case when confirm_flag='是' then '否'
        when diff_confirm_days>50 then '是'
    else '否' 
    end tc_bill_type_no_send,
  -- 开票缓发
   case when kp_flag='是' then '否'
        when diff_invoice_days >=25 and diff_invoice_days<=55 then '是'
    else '否' 
    end tc_invoice_type,
    -- 开票扣发
  case when kp_flag='是' then '否'
        when diff_invoice_days > 55 then '是'
    else '否' 
    end tc_history_invoice_type_no_send,
    -- 对账缓发=是或开票缓发=是 
   if(case when confirm_flag='是' then '否' when diff_confirm_days >=20 and diff_confirm_days<=50 then '是' else '否' end ='是' 
       or 
        case when kp_flag='是' then '否'  when diff_invoice_days >=25 and diff_invoice_days<=55 then '是'  else '否' end ='是'
        , '是' ,   '否'
    ) tc_delayed_release_result,
  if(case when confirm_flag='是' then '否' when diff_confirm_days >50 then '是' else '否' end ='是' 
       or 
        case when kp_flag='是' then '否'  when diff_invoice_days >55 then '是'  else '否' end ='是'
        , '是' ,   '否'
    ) tc_suspended_result,
  current_timestamp() update_time,
  substr('${partition_date}',1,6) s_month,
  (statement_amount ) as statement_amount,
  a.last_sale_amt, -- 上一结算期销售金额 
  a.last_bill_amt,     -- 上一结算期对账金额 
  a.unbill_amt,    -- 上一结算期未对账金额
--   a.bill_amt_all, -- 历史对账金额 
  a.unbill_amount_history,   -- 历史未对帐金额
  confirm_flag,               -- 是否完全对账
  (kp_amount ) as kp_amount,
  invoice_amount    ,                           -- 上一期开票金额
  no_invoice_amt,                               -- 上一期未开票金额
--   invoice_amount_all,                           -- 历史开票金额
  no_invoice_amt_history,                       -- 历史未开票金额
  kp_flag, -- 是否完全开票
    -- c_statement_date ,
    -- c1_statement_date,
    -- unbill_amt_all,
    -- unstatement_amount_all,
    -- no_invoice_amount_all,
    -- no_kp_amount_all
    substr('${partition_date}',1,6) smt
from
  tmp_ruslt_01 a

;

CREATE  TABLE IF NOT EXISTS csx_analyse.csx_analyse_hr_customer_deferred_reconciliation_invoice_result_mf( 

`performance_region_code` STRING  COMMENT '大区编码',
`performance_region_name` STRING  COMMENT '大区名称',
`performance_province_code` STRING  COMMENT '省区编码',
`performance_province_name` STRING  COMMENT '省区名称',
`performance_city_code` STRING  COMMENT '城市编码',
`performance_city_name` STRING  COMMENT '城市名称',
`customer_code` STRING  COMMENT '客户编码',
`customer_name` STRING  COMMENT '客户名称',
`sales_user_number` STRING  COMMENT '销售员工工号',
`sales_user_name` STRING  COMMENT '销售员工姓名',
`service_user_work_no` STRING  COMMENT '管家工工号',
`service_user_name` STRING  COMMENT '管家名称',
`business_attribute_name` STRING  COMMENT '业务类型属性',
`reconciliation_period` STRING  COMMENT '对账周期',
`statemen_date` STRING  COMMENT '账单日期',
`customer_confirm_date` STRING  COMMENT '客户对账日期',
`diff_bill_days` STRING  COMMENT '客户对账日期距离账单日天数',
`invoice_statement_date` STRING  COMMENT '开票账单日期',
`invoice_date` STRING  COMMENT '开票日期',
`diff_invoice_days` STRING  COMMENT '开票日期距离账单日天数',
`tc_bill_type` STRING  COMMENT '提成对账缓发标识',
`tc_bill_type_no_send` STRING  COMMENT '提成对账扣发标识',
`tc_invoice_type` STRING  COMMENT '提成开票缓发标识',
`tc_history_invoice_type_no_send` STRING  COMMENT '提成开票扣发标识',
`tc_delayed_release_result` STRING  COMMENT '缓发结果',
`tc_suspended_result` STRING  COMMENT '扣发结果',
`update_time` TIMESTAMP  COMMENT '数据更新时间',
`s_month` STRING  COMMENT '数据月份',
`statement_amount` DECIMAL (26,4) COMMENT '上一结算期销售金额',
`last_sale_amt` DECIMAL (26,4) COMMENT '上一结算期对账金额',
`last_bill_amt` DECIMAL (26,4) COMMENT '上一结算期对账金额',
`last_unbill_amt` DECIMAL (26,4) COMMENT '上一结算期未对账金额',
`unbill_amount_history` DECIMAL (26,4) COMMENT '历史未对账金额',
`confirm_flag` STRING COMMENT '是否完全对账',
`kp_amount` DECIMAL (26,4) COMMENT '开票金额',
`last_invoice_amount` DECIMAL (26,4) COMMENT '上一结算期开票金额',
`last_no_invoice_amt` DECIMAL (26,4) COMMENT '上一结算期未开票金额',
`no_invoice_amt_history` DECIMAL (26,4) COMMENT '历史未开票金额',
`kp_flag` STRING COMMENT '是否完全开票标识 标识' ) 
 COMMENT '客户缓发对账结果表' 
 PARTITIONED BY
 (
`smt` STRING  COMMENT '月分区，每月执行{format:yyyymm}{"FORMAT":"yyyymm"}' )
 STORED AS PARQUET

 
select 
performance_region_name
,performance_province_name
,performance_city_name
,customer_code
,customer_name
,credit_code
,company_code
,business_attribute_name
,work_no_new
,sales_name_new
,service_user_work_no
,service_user_name
,bill_code
,source_bill_no
,reconciliation_period
,statement_date
,happen_date
,bill_date_section
,order_amt
,statement_amount
,kp_amount
,confirm_date
,diff_confirm_days
,invoice_date
,diff_invoice_days
,row_confirm
,row_invoice
,order_total_amt
,bill_total_amt
,kp_total_amt
,last_sale_amt
,bill_amt_all
,last_bill_amt
,unbill_amt
,unbill_amount_history
,invoice_amount
,no_invoice_amt
,invoice_amount_all
,no_invoice_amt_history
,unbill_amt_all
,no_invoice_amount_all
,no_confirm_days
,no_invoice_days
,max_statement_date
,order_no_kp_amount_history
,order_unstatement_amount
,order_unstatement_amount_history
,order_unstatement_amount_all
,order_no_kp_amount_all
,diff_confirm_days_ro
,diff_invoice_days_ro
,diff_no_confirm_days_ro
,diff_no_invoice_days_ro
,rn
,update_time
,s_month
from csx_analyse.csx_analyse_hr_customer_deferred_reconciliation_invoice_detail_mf

CREATE TABLE IF NOT EXISTS data_analysis_prd.report_csx_analyse_hr_customer_reconciliation_invoice_result( 

`id` BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
`performance_region_code` VARCHAR(64)  COMMENT '大区编码',
`performance_region_name` VARCHAR(64)  COMMENT '大区名称',
`performance_province_code` VARCHAR(64)  COMMENT '省区编码',
`performance_province_name` VARCHAR(64)  COMMENT '省区名称',
`performance_city_code` VARCHAR(64)  COMMENT '城市编码',
`performance_city_name` VARCHAR(64)  COMMENT '城市名称',
`customer_code` VARCHAR(64)  COMMENT '客户编码',
`customer_name` VARCHAR(64)  COMMENT '客户名称',
`sales_user_number` VARCHAR(64)  COMMENT '销售员工工号',
`sales_user_name` VARCHAR(64)  COMMENT '销售员工姓名',
`service_user_work_no` VARCHAR(64)  COMMENT '管家工工号',
`service_user_name` VARCHAR(64)  COMMENT '管家名称',
`business_attribute_name` VARCHAR(64)  COMMENT '业务类型属性',
`reconciliation_period` VARCHAR(64)  COMMENT '对账周期',
`statemen_date` VARCHAR(64)  COMMENT '账单日期',
`customer_confirm_date` VARCHAR(64)  COMMENT '客户对账日期',
`diff_bill_days` INT NULL COMMENT '客户对账日期距离账单日天数',
`invoice_statement_date` VARCHAR(64)  COMMENT '开票账单日期',
`invoice_date` VARCHAR(64)  COMMENT '开票日期',
`diff_invoice_days` INT NULL COMMENT '开票日期距离账单日天数',
`tc_bill_type` VARCHAR(64)  COMMENT '提成对账缓发标识',
`tc_bill_type_no_send` VARCHAR(64)  COMMENT '提成对账扣发标识',
`tc_invoice_type` VARCHAR(64)  COMMENT '提成开票缓发标识',
`tc_history_invoice_type_no_send` VARCHAR(64)  COMMENT '提成开票扣发标识',
`tc_delayed_release_result` VARCHAR(64)  COMMENT '缓发结果',
`tc_suspended_result` VARCHAR(64)  COMMENT '扣发结果',
`update_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间',
`s_month` VARCHAR(64)  COMMENT '数据月份',
`statement_amount` DECIMAL (26,4) COMMENT '上一结算期销售金额',
`last_sale_amt` DECIMAL (26,4) COMMENT '上一结算期对账金额',
`last_bill_amt` DECIMAL (26,4) COMMENT '上一结算期对账金额',
`last_unbill_amt` DECIMAL (26,4) COMMENT '上一结算期未对账金额',
`unbill_amount_history` DECIMAL (26,4) COMMENT '历史未对账金额',
`confirm_flag` VARCHAR(64) COMMENT '是否完全对账',
`kp_amount` DECIMAL (26,4) COMMENT '开票金额',
`last_invoice_amount` DECIMAL (26,4) COMMENT '上一结算期开票金额',
`last_no_invoice_amt` DECIMAL (26,4) COMMENT '上一结算期未开票金额',
`no_invoice_amt_history` DECIMAL (26,4) COMMENT '历史未开票金额',
`kp_flag` VARCHAR(64) COMMENT '是否完全开票标识',

INDEX idx_s_month (`s_month`),
INDEX idx_province_city (`performance_province_code`, `performance_city_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='客户缓发对账结果表';
