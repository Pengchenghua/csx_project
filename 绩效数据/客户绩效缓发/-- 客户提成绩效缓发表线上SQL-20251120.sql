-- 客户提成绩效缓发表线上SQL 
-- 任务：job_csx_analyse_hr_customer_deferred_reconciliation_invoice_detail_mf
-- 涉及表
-- 管家销售员信息表：csx_analyse_tmp.tmp_csx_dim_crm_customer_business_ownership
-- csx_analyse_tmp_bill_settle_00、csx_analyse_tmp.csx_analyse_tmp_bill_settle_02
-- ******************************************************************** 
-- @功能描述：
-- @创建者： 彭承华 
-- @创建者日期：2025-10-23 23:56:57 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 



-- 创建管家销售员信息 
drop table csx_analyse_tmp.tmp_csx_dim_crm_customer_business_ownership;
create table csx_analyse_tmp.tmp_csx_dim_crm_customer_business_ownership as 
with  tmp_csx_dim_crm_customer_business_ownership as 
 (select
  customer_no as customer_code,
  customer_name,
  rp_sales_user_work_no_new as work_no_new,
  rp_sales_user_name_new as sales_name_new,
  a.region_code as performance_region_code,
  a.region_name as performance_region_name,
  a.province_code as performance_province_code,
  a.province_name as performance_province_name,
  a.city_group_code as performance_city_code,
  a.city_group_name as performance_city_name,
  '日配' as business_attribute_name,
  rp_service_user_work_no_new as service_user_work_no,
  rp_service_user_name_new as service_user_name
from
  csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df a
where
  sdt = '${partition_date}'
    and (rp_service_user_work_no_new<>'' or rp_sales_user_work_no_new<>'')
union all

select
  customer_no as customer_code,
  customer_name,
  fl_sales_user_work_no_new as work_no_new,
  fl_sales_user_name_new as sales_name_new,
  a.region_code as performance_region_code,
  a.region_name as performance_region_name,
  a.province_code as performance_province_code,
  a.province_name as performance_province_name,
  a.city_group_code as performance_city_code,
  a.city_group_name as performance_city_name,
  '福利' as business_attribute_name,
  fl_service_user_work_no_new as service_user_work_no,
  fl_service_user_name_new as service_user_name
from
  csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df a
where
  sdt = '${partition_date}'
  and (fl_service_user_work_no_new<>'' or fl_sales_user_work_no_new<>'')
union all
select
  customer_no as customer_code,
  customer_name,
  bbc_sales_user_work_no_new as work_no_new,
  bbc_sales_user_name_new as sales_name_new,
  a.region_code as performance_region_code,
  a.region_name as performance_region_name,
  a.province_code as performance_province_code,
  a.province_name as performance_province_name,
  a.city_group_code as performance_city_code,
  a.city_group_name as performance_city_name,
  'BBC' as business_attribute_name,
  bbc_service_user_work_no_new as service_user_work_no,
  bbc_service_user_name_new as service_user_name
from
  csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df a
where
  sdt = '${partition_date}'
  and (bbc_service_user_work_no_new<>'' or bbc_sales_user_work_no_new<>'')
  )
  ,
  tmp_sale_info as 
 (select
  a.customer_code,
  a.customer_name,
  b.work_no_new,
  b.sales_name_new,
  a.performance_region_code,
  a.performance_region_name,
  a.performance_province_code,
  a.performance_province_name,
  a.performance_city_code,
  a.performance_city_name,
  b.business_attribute_name,
  b.service_user_work_no,
  b.service_user_name
from
(select
  customer_no as customer_code,
  customer_name,
  a.region_code as performance_region_code,
  a.region_name as performance_region_name,
  a.province_code as performance_province_code,
  a.province_name as performance_province_name,
  a.city_group_code as performance_city_code,
  a.city_group_name as performance_city_name
from
  csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df a
where
  sdt = '${partition_date}'
  )a 
  left join tmp_csx_dim_crm_customer_business_ownership b
     on a.customer_code=b.customer_code
 ) select * from tmp_csx_dim_crm_customer_business_ownership
 ;


-- ******************************************************************** 
-- @功能描述：对账信息表csx_analyse_tmp_bill_settle_00
-- @创建者： 彭承华 
-- @创建者日期：2025-10-24 14:03:37 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 
SET hive.exec.parallel=true;
-- 大幅增加内存配置
SET tez.am.resource.memory.mb = 12384;      -- AM内存16GB
SET tez.task.resource.memory.mb = 8192;     -- 任务内存8GB  
SET hive.tez.container.size = 12288;        -- 容器大小12GB

drop table csx_analyse_tmp.csx_analyse_tmp_bill_settle_00 ;
create table csx_analyse_tmp.csx_analyse_tmp_bill_settle_00 as 
with  temp_company_credit as 
  ( select
  customer_code,
  credit_code,
  customer_name,
  business_attribute_code,
  business_attribute_name,
  company_code,
  status,
  account_period_code       payment_terms,  --  账期类型  Z007剔除
  account_period_name       payment_name,   --  账期名称
  account_period_value      payment_days,   --  帐期天数
  is_history_compensate
from
    csx_dim.csx_dim_crm_customer_company_details
where
  sdt = 'current'
  -- and account_period_code !='Z007'
group by customer_code,
    credit_code,
    customer_name,
    business_attribute_code,
    business_attribute_name,
    company_code,
    status,
    is_history_compensate,
    account_period_code ,
    account_period_name ,
    account_period_value
  ) ,
tmp_bill_order as (
  select 
	source_bill_no,
	bill_code,
    a.customer_code,
	a.credit_code,
    a.company_code,
    b.customer_name,
    b.business_attribute_code,
    b.business_attribute_name,
    source_sys,
    happen_date,
    payment_terms, --  账期类型  Z007剔除
    account_period_name,
    account_period_value,
    reconciliation_period,
    project_begin_date,
    project_end_date,
	bill_start_date,
	bill_end_date,
	date_add(bill_end_date,1) statement_date,
    nvl(sub_customer_code,'') as sub_customer_code,
    cast(order_amt as decimal(26,6)) order_amt,  -- 销售订单金额
    (case when check_bill_status in (15,20) then residue_total_amount else 0 end)
      + order_amt - residue_total_amount as statement_amount, -- 对账金额
    cast(invoice_amount as decimal(26,6)) as kp_amount,  -- 开票金额
    sum(order_amt)over(partition by bill_code,a.customer_code,b.business_attribute_name,a.credit_code,a.company_code,
                                    case when source_sys='BBC' and relation_order_code !='' then relation_order_code else source_bill_no end ) as order_total_amt,
    sum(order_amt)over(partition by bill_code,a.customer_code,b.business_attribute_name,a.credit_code,a.company_code) as order_bill_total_amt,    
    sum(invoice_amount)over(partition by bill_code,a.customer_code,b.business_attribute_name,a.credit_code,a.company_code) as invoice_total_amt,
    overdue_date,
    coalesce(c1.statement_mode_code,c3.statement_mode_code) as statement_mode_code
  from  csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di a
  LEFT JOIN temp_company_credit b on a.customer_code=b.customer_code and a.company_code=b.company_code and a.credit_code=b.credit_code
  left join
  (select 
    customer_code,
    company_code,
    credit_code,
    max(statement_mode_code) as statement_mode_code
  from csx_dim.csx_dim_sss_customer_statement_config
  where sdt = 'current' and table_type like '%_DETAIL' and credit_code <> '' and shipper_code = 'YHCSX'
--   and customer_code='245242'
  group by customer_code,company_code,credit_code
  ) c1 on a.customer_code = c1.customer_code and a.company_code = c1.company_code 
    and a.credit_code = c1.credit_code                                          
  left join
  (select 
     customer_code,
     company_code,
     max(statement_mode_code) as statement_mode_code
   from csx_dim.csx_dim_sss_customer_statement_config
   where sdt = 'current' and table_type like '%_ALL' and shipper_code = 'YHCSX'
   group by customer_code,company_code
  ) c3 on a.customer_code = c3.customer_code and a.company_code = c3.company_code
  where sdt = '${partition_date}' 
  and happen_date>='2025-01-01'
  and date_add(bill_end_date,1)>='2025-01-01'  
  and source_sys !='BEGIN'
  and a.customer_code not like 'C%'
  and bbc_bill_flag<>1  -- 剔除BBC无需对账
  )select * from tmp_bill_order
  ;


  -- ******************************************************************** 
-- @功能描述：开票信息与确认对账信息
-- @创建者： 彭承华 
-- @创建者日期：2025-10-23 23:11:20 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 
SET hive.exec.parallel=true;
-- 大幅增加内存配置
SET tez.am.resource.memory.mb = 12384;      -- AM内存16GB
SET tez.task.resource.memory.mb = 8192;     -- 任务内存8GB  
SET hive.tez.container.size = 12288;        -- 容器大小12GB


 drop table csx_analyse_tmp.csx_analyse_tmp_bill_settle_02;
 create table csx_analyse_tmp.csx_analyse_tmp_bill_settle_02 as 
 -- 客户对账单
with tmp_sss_customer_statement_account_di as 
(select bill_code,
        customer_code,
        company_code,
        invoice_time,
        to_date(sale_bill_date) sale_bill_date,
        customer_bill_date,
        (bill_amt) bill_amt,
        credit_pay_amt,
        confirm_status,
        tail_adjustment_amt,
        to_date(confirm_time) confirm_date
 from  csx_dwd.csx_dwd_sss_customer_statement_account_di 
    where    sdt<='${partition_date}'
    -- and sdt>='20250101'
    and delete_flag = 0
    and  to_date(confirm_time)<='${data_range_date1}'
  ) 
  ,
  -- 开票明细
 tmp_sss_invoice_detail as (
select
  company_code,
  customer_code,
  to_date(invoice_time)invoice_date,
  source_bill_no, 
  sum(residue_amt)residue_amt
from
    csx_dwd.csx_dwd_sss_invoice_di a
  left join (
    select
      order_code,
      source_bill_no, -- 销售单号
      sum(residue_amt) residue_amt
    from
      csx_dwd.csx_dwd_sss_kp_apply_goods_group_detail_di
    where
       sdt <= '${partition_date}'
    --   and sdt>='20250101'
    --   and invoice_status_code=2
    --   and sync_status=1   -- 发票更新状态
    --   and cx_invoice_no_code is null 
    group by
      order_code,
      source_bill_no
  ) b on a.order_code = b.order_code

where
   sdt <= '${partition_date}'
--   and sdt>='20250101'
   and  to_date(invoice_time)<='${data_range_date1}'
  group by company_code,
  customer_code,
  to_date(invoice_time),
  source_bill_no
 )  ,
  tmp_bill_confirm as 
(select a.*,
    b.bill_amt,
    confirm_date,
    confirm_status,
    credit_pay_amt,
    c.invoice_date,
    case when coalesce(confirm_date,'') ='' then  datediff('${data_range_date1}',statement_date) 
        when (statement_amount-order_amt) = 0 then  datediff(confirm_date,statement_date)
        else  datediff('${data_range_date1}',statement_date) end as diff_confirm_days,
    case when coalesce(c.invoice_date,'') ='' then  datediff('${data_range_date1}',statement_date)
        when (order_amt-kp_amount)=0 then datediff(c.invoice_date,statement_date) 
        else datediff('${data_range_date1}',statement_date) end  as diff_invoice_days
  from csx_analyse_tmp.csx_analyse_tmp_bill_settle_00  a 
  left join tmp_sss_customer_statement_account_di b on a.bill_code=b.bill_code 
  left join tmp_sss_invoice_detail c on   a.source_bill_no= c.source_bill_no
  where 
--   order_amt-statement_amount <>0
    order_total_amt<>0
    and order_bill_total_amt<>0
    -- and statement_amount>0
)
select source_bill_no,
	bill_code,
    a.customer_code,
    a.credit_code,
    a.company_code,
    business_attribute_code,
    business_attribute_name,
    source_sys,
    happen_date,
    payment_terms, --  账期类型  Z007剔除
    account_period_name,
    account_period_value,
    reconciliation_period,
    project_begin_date,
    project_end_date,
	bill_start_date,
	bill_end_date,
	statement_date,
    sub_customer_code,
    order_amt,  -- 销售订单金额
    statement_amount, -- 对账金额
    kp_amount,  -- 开票金额
    order_total_amt,
    invoice_total_amt,
    overdue_date,
    bill_amt,
    confirm_date,
    confirm_status,
    credit_pay_amt,
    diff_confirm_days,
    invoice_date,
    diff_invoice_days,
    statement_mode_code
  from tmp_bill_confirm a 
WHERE
--   (
--     (confirm_date BETWEEN '2025-09-23' AND '2025-10-22' OR coalesce(confirm_date,'')='')
--     OR
--     (invoice_date BETWEEN '2025-09-23' AND '2025-10-22' OR  coalesce(invoice_date,'')='')
--   )
   
  statement_date>='2025-01-01'
  and coalesce(a.payment_terms,'') !='Z007'  --  账期类型  Z007剔除
  and coalesce(a.statement_mode_code,'') !='5'  -- 项目制客户剔除
 
;
-- ******************************************************************** 
-- @功能描述：按照单号计算逾期金额
-- @创建者： 彭承华 
-- @创建者日期：2025-10-24 15:52:36 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 
SET hive.exec.parallel=true;
-- 大幅增加内存配置
SET tez.am.resource.memory.mb = 12384;      -- AM内存16GB
SET tez.task.resource.memory.mb = 8192;     -- 任务内存8GB  
SET hive.tez.container.size = 12288;        -- 容器大小12GB

drop table csx_analyse_tmp.csx_analyse_tmp_customer_credit_invoice_bill_unmount_detail;

create table csx_analyse_tmp.csx_analyse_tmp_customer_credit_invoice_bill_unmount_detail as 
with csx_dws_sss_order_credit_invoice_bill_settle_detail_di as 
(select customer_code,
        company_code,
        credit_code,
        account_period_code,
        account_period_name,
        account_period_val,
        statement_start_date,
        statement_end_date,
        bill_code,
        source_bill_no,	
        happen_date,
        statement_amount, -- 上一结算周期排除期初的对账金额
        unstatement_amount, -- 上一结算周期排除期初的未对账金额
        kp_amount,	            -- 上一结算周期排除期初的开票金额
        no_kp_amount,            -- 上一结算周期排除期初的未开票金额
        tax_sale_amount	,           -- 上一结算周期排除期初的财务含税销售额_财务业务确认以财务对账来源单为销售金额计算(张正孝)
        statement_amount_all,       -- 对账金额
        unstatement_amount_all,	          -- 未对账金额
        unstatement_amount_history,     -- 历史未对账金额(上个周期之前的所有未对账金额)
        kp_amount_history,               -- 历史开票金额
        no_kp_amount_history,            -- 历史未开票金额 
        statement_amount_history,       -- 历史对账金额
        kp_amount_all,                      -- 开票金额
        no_kp_amount_all,               -- 未开票金额
        tax_sale_amount_all	           -- 财务含税销售额_财务业务确认以财务对账来源单为销售金额计算(张正孝)
from (          select 
                      customer_code,                                                                                                   -- 客户编码
                      company_code,                                                                                                    -- 公司编码
                      credit_code,
                      bill_code,
                      source_bill_no,
                      happen_date,
                      statement_start_date,
                      statement_end_date,
                      account_period_code,
                      account_period_name,
                     account_period_val,
                      sum(case
                              when substr(regexp_replace(stat_date, '-', ''), 1, 6) =
                                   substr('${data_range_date}', 1, 6)
                                  then paid_amount
                              else 0 end)                                                             back_money_amount_month,         -- 当月回款金额_对账表中字段
                      (sum(case when statement_state in (15,20) then residue_total_amount else 0 end)
                          + sum(source_statement_amount - residue_total_amount - kp_amount) +
                       sum(kp_amount))                                                                statement_amount,                -- 上一结算周期排除期初的对账金额
                      sum(case when statement_state not in (15,20) then source_statement_amount end)         unstatement_amount,              -- 上一结算周期排除期初的未对账金额
                      sum(kp_amount)                                                                  kp_amount,                       -- 上一结算周期排除期初的开票金额
					  sum(no_kp_amount)                                                           as  no_kp_amount,                   -- 上一结算周期排除期初的未开票金额
                      sum(source_statement_amount)                                                    tax_sale_amount,                 -- 上一结算周期排除期初的财务含税销售额_财务业务确认以财务对账来源单为销售金额计算(张正孝)
                      sum(tax_amount)                                                                 tax_amount,                      -- 上一结算周期排除期初的财务含税额
                      (sum(case when statement_state in (15,20) then residue_total_amount_all else 0 end)
                          + sum(source_statement_amount_all - residue_total_amount_all - kp_amount_all) +
                       sum(kp_amount_all))                                                            statement_amount_all,            -- 对账金额
                      sum(
                              case when statement_state not in (15,20) then source_statement_amount_all end) unstatement_amount_all,          -- 未对账金额
    -- 历史未对账逻辑调整, 对账日不为1的 上周期及历史周期往前推10天进行统计
                      sum(case when statement_state not in (15,20) and ((settle_cycle <> '1' and statement_end_date < date_add(add_months('${data_range_date1}', -1),-10)) or 
                               (settle_cycle = '1' and statement_end_date < add_months('${data_range_date1}', -1)))
                               then source_statement_amount_all else 0 end) as unstatement_amount_history, -- 历史未对账金额(上个周期之前的所有未对账金额)
								  
					  sum(case when (settle_cycle <> '1' and statement_end_date < date_add(add_months('${data_range_date1}', -1),-10)) or
					                (settle_cycle = '1' and statement_end_date < add_months('${data_range_date1}', -1)) then kp_amount_all else 0 end) as kp_amount_history,      -- 历史开票金额
					  
					  sum(case when (settle_cycle <> '1' and statement_end_date < date_add(add_months('${data_range_date1}', -1),-10)) or
					                (settle_cycle = '1' and statement_end_date < add_months('${data_range_date1}', -1)) then no_kp_amount_all else 0 end) as no_kp_amount_history,      -- 历史未开票金额 
					  
					  (sum(case when statement_state in (15,20) and ((settle_cycle <> '1' and statement_end_date < date_add(add_months('${data_range_date1}', -1),-10)) or
                                                                     (settle_cycle = '1' and statement_end_date < add_months('${data_range_date1}', -1))) then residue_total_amount_all else 0 end)
                          + 
                       sum(case when ((settle_cycle <> '1' and statement_end_date < date_add(add_months('${data_range_date1}', -1),-10)) or
                                      (settle_cycle = '1' and statement_end_date < add_months('${data_range_date1}', -1))) then (source_statement_amount_all - residue_total_amount_all - kp_amount_all) else 0 end) 
                          +
                       sum(case when ((settle_cycle <> '1' and statement_end_date < date_add(add_months('${data_range_date1}', -1),-10)) or
                                      (settle_cycle = '1' and statement_end_date < add_months('${data_range_date1}', -1))) then kp_amount_all else 0 end)) as statement_amount_history,            -- 历史对账金额
					  
                      sum(kp_amount_all)                                                              kp_amount_all,                   -- 开票金额
					  sum(no_kp_amount_all)                                                    as     no_kp_amount_all,                -- 未开票金额
                      sum(source_statement_amount_all)                                                tax_sale_amount_all,             -- 财务含税销售额_财务业务确认以财务对账来源单为销售金额计算(张正孝)
                      sum(tax_amount_all)                                                             tax_amount_all                  -- 财务含税额

                     
               from (select source_bill_no,
                            relation_order_no,
                            customer_code,
                            customer_name,
                            credit_code,
                            happen_date,
                            --  应收和逾期包含期初
                            --  销售金额/开票金额 不包含期初且都是上一结算周期的数据
                            source_statement_amount                            source_statement_amount_all, -- 销售金额
                            -- case
                            --     when statement_start_date <= add_months('${data_range_date1}', -1)
                            --         and statement_end_date >= add_months('${data_range_date1}', -1)
                            --         then source_statement_amount
                            --     else 0 end                                     source_statement_amount,     -- 上一结算周期排除期初的销售金额
                            case when settle_cycle <> '1' 
                                  then case when statement_start_date <= date_add(add_months('${data_range_date1}', -1),-10)  -- 非1号对账日, 上周期前推10天统计
                                               and statement_end_date >= date_add(add_months('${data_range_date1}', -1),-10) then source_statement_amount else 0 end 
                                  else case when statement_start_date <= add_months('${data_range_date1}', -1)
                                               and statement_end_date >= add_months('${data_range_date1}', -1) then source_statement_amount else 0 end end          as source_statement_amount,     -- 上一结算周期排除期初的销售金额
                            supply_bill,
                            bill_type,
                            company_code,
                            company_name,
                            create_by,
                            create_time,
                            update_by,
                            update_time,
                            paid_amount,
                            money_back_status,
                            overdue_date_sss,
                            overdue_days_sss,
                            residual_amount,
                            residual_amount_sss,
                            unpaid_amounts,
                            unpaid_amount,
                            unpaid_amount_sss,
                            overdue_amount_sss,
                            bad_debt_amount,
                            account_period_code,
                            account_period_name,
                            account_period_val,
                            settle_cycle,
                            project_end,
                            project_begin,
                            acc_val_calculation_factor,
                            kp_amount                                          kp_amount_all,               -- 开票金额
							no_kp_amount                                    as no_kp_amount_all,
    --                         case
    --                             when statement_start_date <= add_months('${data_range_date1}', -1)
    --                                 and statement_end_date >= add_months('${data_range_date1}', -1)
    --                                 then kp_amount
    --                             else 0 end                                     kp_amount,                   -- 上一结算周期排除期初的开票金额
				-- 			case
    --                             when statement_start_date <= add_months('${data_range_date1}', -1)
    --                                 and statement_end_date >= add_months('${data_range_date1}', -1)
    --                                 then no_kp_amount
    --                             else 0 end                                     no_kp_amount,			   -- 上一结算周期未开票金额	
                            case when settle_cycle <> '1'
                                  then case when statement_start_date <= date_add(add_months('${data_range_date1}', -1),-10)
                                               and statement_end_date >= date_add(add_months('${data_range_date1}', -1),-10) then kp_amount else 0 end 
                                  else case when statement_start_date <= add_months('${data_range_date1}', -1)
                                               and statement_end_date >= add_months('${data_range_date1}', -1) then kp_amount else 0 end end             as kp_amount,                   -- 上一结算周期排除期初的开票金额
                            case when settle_cycle <> '1'
                                  then case when statement_start_date <= date_add(add_months('${data_range_date1}', -1),-10)
                                               and statement_end_date >= date_add(add_months('${data_range_date1}', -1),-10) then no_kp_amount else 0 end 
                                  else case when statement_start_date <= add_months('${data_range_date1}', -1)
                                               and statement_end_date >= add_months('${data_range_date1}', -1) then no_kp_amount else 0 end end          as no_kp_amount,			   -- 上一结算周期未开票金额
                                
                            statement_state,
                            -- case
                            --     when statement_start_date <= add_months('${data_range_date1}', -1)
                            --         and statement_end_date >= add_months('${data_range_date1}', -1)
                            --         then residue_total_amount
                            --     else 0 end                                     residue_total_amount,        -- 上一结算剩余申请金额
                            case when settle_cycle <> '1' 
                                  then case when statement_start_date <= date_add(add_months('${data_range_date1}', -1),-10)
                                               and statement_end_date >= date_add(add_months('${data_range_date1}', -1),-10) then residue_total_amount else 0 end 
                                  else case when statement_start_date <= add_months('${data_range_date1}', -1)
                                               and statement_end_date >= add_months('${data_range_date1}', -1) then residue_total_amount else 0 end end   as residue_total_amount,        -- 上一结算剩余申请金额
                                
                            residue_total_amount                               residue_total_amount_all,
                            pay_on_line_amount,
                            tax_amount                                         tax_amount_all,              -- 财务含税额
                            -- case
                            --     when statement_start_date <= add_months('${data_range_date1}', -1)
                            --         and statement_end_date >= add_months('${data_range_date1}', -1)
                            --         then tax_amount
                            --     else 0 end                                     tax_amount,                  -- 上一结算周期排除期初的财务含税额
                            case when settle_cycle <> '1' 
                                  then case when statement_start_date <= date_add(add_months('${data_range_date1}', -1),-10)  -- 非1号对账日, 上周期前推10天统计
                                               and statement_end_date >= date_add(add_months('${data_range_date1}', -1),-10) then tax_amount else 0 end 
                                  else case when statement_start_date <= add_months('${data_range_date1}', -1)
                                               and statement_end_date >= add_months('${data_range_date1}', -1) then tax_amount else 0 end end             as tax_amount,                  -- 上一结算周期排除期初的财务含税额
                                
                            source_sys,
                            payment_amount,
                            statement_start_date,
                            statement_end_date,
                            overdue_date_new,
                            substr(regexp_replace(happen_date, '-', ''), 1, 8) stat_date,
                            if((datediff('${data_range_date1}', overdue_date_new) + 1) >= 1 and unpaid_amount > 0,
                               datediff('${data_range_date1}', overdue_date_new) + 1,
                               0) as                                           overdue_days,
                            if((datediff('${data_range_date1}', overdue_date_new) + 1) >= 1 and unpaid_amount > 0,
                               unpaid_amount,
                               0) as                                           overdue_amount,
                            shipper_code,
                            shipper_name,
                            if((datediff('${data_range_date1}', overdue_date_new) + 1) >= 1 and unpaid_amount > 0,
                               no_kp_amount,
                               0) as                                           overdue_no_invoice_amt_all,
                               bill_code
                     from (select source_bill_no                    as source_bill_no,
                                  relation_order_code               as relation_order_no,
                                  customer_code                     as customer_code,
                                  customer_name                     as customer_name,
                                  credit_code,
                                  happen_date                       as happen_date,
                                  order_amt                         as source_statement_amount,
                                  is_patch_order                    as supply_bill,
                                  bill_type                         as bill_type,
                                  company_code                      as company_code,
                                  company_name                      as company_name,
                                  create_by                         as create_by,
                                  create_time                       as create_time,
                                  update_by                         as update_by,
                                  update_time                       as update_time,
                                  paid_amt                          as paid_amount,
                                  money_back_status                 as money_back_status,
                                  overdue_date_sss                  as overdue_date_sss,
                                  overdue_days_sss                  as overdue_days_sss,
                                  residue_amt                       as residual_amount,
                                  residue_amt_sss                   as residual_amount_sss,
                                  cumulative_unpaid_amount          as unpaid_amounts,
                                  negative_unpaid_total_amount      as negative_unpaid_total_amounts,
                                  positive_unpaid_total_amount      as positive_unpaid_total_amounts,
                                  negative_cumulative_unpaid_amount as negative_unpaid_amounts,
                                  positive_cumulative_unpaid_amount as positive_unpaid_amounts,
                                  unpaid_amount                     as unpaid_amount,
                                  unpaid_amount_sss                 as unpaid_amount_sss,
                                  overdue_amount_sss                as overdue_amount_sss,
                                  bad_debt_amount                   as bad_debt_amount,
                                  account_period_code               as account_period_code,
                                  account_period_name               as account_period_name,
                                  account_period_value              as account_period_val,
                                  reconciliation_period             as settle_cycle,
                                  project_end_date                  as project_end,
                                  project_begin_date                as project_begin,
                                  account_value_calculation_factor  as acc_val_calculation_factor,
                                  invoice_amount                    as kp_amount,
								  (order_amt-invoice_amount)        as no_kp_amount,
                                  check_bill_status                 as statement_state,
                                  residue_total_amount              as residue_total_amount,
                                  pay_on_line_amount                as pay_on_line_amount,
                                  tax_amt                           as tax_amount,
                                  source_sys                        as source_sys,
                                  close_bill_amount                 as payment_amount,
                                  bill_start_date                   as statement_start_date,
                                  bill_end_date                     as statement_end_date,
                                  overdue_date                      as overdue_date_new,
                                  shipper_code,
                                  bill_code,
                                  shipper_name,
                                  sdt                               as sdt
                           from csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di
                           where sdt = '${partition_date}'
                          ) t_a
                    ) a
                    -- where customer_code='110517'
               group by customer_code, company_code, credit_code,shipper_code,source_bill_no,
               happen_date,
               bill_code,
               account_period_code,
account_period_name,
account_period_val,
statement_start_date,
statement_end_date
               )a 
    )
select customer_code,
company_code,
credit_code,
account_period_code,
account_period_name,
account_period_val,
statement_start_date,
statement_end_date,
source_bill_no,
date_add(statement_end_date,1) statement_date,
bill_code,
happen_date,
substr(happen_date,1,4) happed_year,
unstatement_amount,
(unstatement_amount_history) unstatement_amount_history,
unstatement_amount_all,
no_kp_amount,
(no_kp_amount_history) no_kp_amount_history,            -- 历史未开票金额 
(statement_amount_history) statement_amount_history ,       -- 历史对账金额
(no_kp_amount_all)no_kp_amount_all              -- 未开票金额
from csx_dws_sss_order_credit_invoice_bill_settle_detail_di 
where happen_date>='2025-01-01'
-- and (unstatement_amount_all<>0 or no_kp_amount_all<>0)
;

-- ******************************************************************** 
-- @功能描述：对账信息表关联订单对账金额与未对账金额
-- @创建者： 彭承华 
-- @创建者日期：2025-10-23 23:11:15 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 

SET hive.exec.parallel=true;
-- 大幅增加内存配置
SET tez.am.resource.memory.mb = 12384;      -- AM内存16GB
SET tez.task.resource.memory.mb = 8192;     -- 任务内存8GB  
SET hive.tez.container.size = 12288;        -- 容器大小12GB

drop table csx_analyse_tmp.tmp_csx_dws_sss_customer_credit_invoice_bill_settle_stat_di;
create table csx_analyse_tmp.tmp_csx_dws_sss_customer_credit_invoice_bill_settle_stat_di as 
with  
    tmp_csx_dws_sss_customer_credit_invoice_bill_settle_stat_di as  (
      select
        customer_code,
        credit_business_attribute_name,
        -- credit_code,
        -- company_code,
        sum(sale_amt) as last_sale_amt,     -- 上期销售额
        sum(bill_amt_all) as bill_amt_all,  -- 累计对账金额
        sum(bill_amt) bill_amt,         -- 当期对账金额
        sum(coalesce(unbill_amt,0) )unbill_amt,     -- 当前未对账金额
        sum(coalesce(unbill_amount_history,0)) unbill_amount_history,  -- 历史未对账金额
        sum(invoice_amount_all) invoice_amount_all, -- 累计开票额
        sum(invoice_amount) invoice_amount,     -- 上一期开票额
        sum(coalesce(no_invoice_amt,0)) no_invoice_amt ,-- 上一期未开票额
        sum(coalesce(no_invoice_amt_history,0)) no_invoice_amt_history,  -- 历史未开票额
        sum(coalesce(unbill_amt,0))+sum(coalesce(unbill_amount_history,0)) as unbill_amt_all, -- 未对账总额
        sum(coalesce(no_invoice_amt,0))+sum(coalesce(no_invoice_amt_history,0)) as no_invoice_amount_all  -- 未开票总额
      from
          csx_dws.csx_dws_sss_customer_credit_invoice_bill_settle_stat_di
      where
        sdt = '${partition_date}'
      group by
        customer_code,
        credit_business_attribute_name
        -- credit_code,
        -- company_code
    )select * from tmp_csx_dws_sss_customer_credit_invoice_bill_settle_stat_di
;

drop table csx_analyse_tmp.csx_analyse_tmp_bill_settle_03;

create table csx_analyse_tmp.csx_analyse_tmp_bill_settle_03 as 
 with 
tmp_csx_analyse_tmp_bill_settle as  
(
select source_bill_no	,
    a.bill_code	,
    a.customer_code	,
    a.credit_code	,
    a.company_code	,
    a.business_attribute_code	,
    a.business_attribute_name	,
    a.source_sys	,
    to_date(happen_date)happen_date	,
    a.account_period_name	,
    a.account_period_value	,
    a.reconciliation_period	,
    a.project_begin_date	,
    a.project_end_date	,
    a.bill_start_date	,
    a.bill_end_date	,
    a.statement_date	,
    a.sub_customer_code	,
    a.order_amt	,
    a.statement_amount	,
    a.kp_amount	,
    a.order_total_amt	,
    a.invoice_total_amt	,
    a.overdue_date	,
    a.bill_amt	,
    a.confirm_date	,
    a.confirm_status	,
    a.credit_pay_amt	,
    a.diff_confirm_days	,
    a.invoice_date	,
    a.diff_invoice_days	,
    b.last_sale_amt,
    b.bill_amt_all,
    b.bill_amt as last_bill_amt,
    b.unbill_amt,
    b.unbill_amount_history,
    b.invoice_amount,
    b.no_invoice_amt,
    b.invoice_amount_all,
    b.no_invoice_amt_history,
    b.unbill_amt_all,
    b.no_invoice_amount_all,
    concat_ws('-',bill_start_date,bill_end_date) as bill_date_section,
    row_number()over(partition by a.customer_code ,a.business_attribute_name order by diff_confirm_days desc ) row_confirm,
    row_number()over(partition by a.customer_code ,a.business_attribute_name order by diff_invoice_days desc ) row_invoice,
    sum(statement_amount)over(partition by a.customer_code,a.bill_code) as bill_total_amt,
    sum(kp_amount)over(partition by a.customer_code,a.bill_code) as kp_total_amt,
    row_number()over(order by statement_date desc ) statement_row,
    if(b.unbill_amt=sum(order_total_amt)over(partition by statement_date ),1,0 ) as unbill_flag,
    MAX(statement_date) OVER (PARTITION BY a.customer_code, a.business_attribute_name) AS max_statement_date
from csx_analyse_tmp.csx_analyse_tmp_bill_settle_02 a 
left join 
    csx_analyse_tmp.tmp_csx_dws_sss_customer_credit_invoice_bill_settle_stat_di  b 
        on a.customer_code=b.customer_code 
        and a.business_attribute_name=b.credit_business_attribute_name 
    where  statement_date <='${data_range_date1}'
     and (coalesce(b.unbill_amt,0)>=1000 
            or coalesce(b.no_invoice_amt,0)>=1000
           or coalesce(b.no_invoice_amt_history,0)>=1000 
            or coalesce(b.unbill_amount_history,0)>=1000 )
     
 
) 
select * from tmp_csx_analyse_tmp_bill_settle

;

-- ******************************************************************** 
-- @功能描述：
-- @创建者： 彭承华 
-- @创建者日期：2025-10-23 23:11:06 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 

SET hive.exec.parallel=true;
-- 大幅增加内存配置
SET tez.am.resource.memory.mb = 12384;      -- AM内存16GB
SET tez.task.resource.memory.mb = 8192;     -- 任务内存8GB  
SET hive.tez.container.size = 12288;        -- 容器大小12GB



drop table csx_analyse_tmp.csx_analyse_tmp_bill_settle_04;
create table csx_analyse_tmp.csx_analyse_tmp_bill_settle_04 as 
select b.performance_region_code,
    b.performance_region_name,
    b.performance_province_code,
    b.performance_province_name,
    b.performance_city_code,
    b.performance_city_name ,
    a.customer_code,
    b.customer_name,
    a.credit_code,
    a.company_code,
    a.business_attribute_name,
    b.work_no_new,
    b.sales_name_new,
    b.service_user_work_no,
    b.service_user_name,
    a.bill_code,
    a.source_bill_no,
    a.reconciliation_period,
    a.statement_date,
    a.happen_date,
    a.bill_date_section,
    a.order_amt	,
    a.statement_amount	,
    a.kp_amount	,
    a.confirm_date,
    a.diff_confirm_days,
    a.invoice_date,
    a.diff_invoice_days,
    row_confirm,
    row_invoice,
    order_total_amt,
    bill_total_amt,
    kp_total_amt,
    last_sale_amt,
    last_bill_amt,
    unbill_amt,
    bill_amt_all,
    unbill_amount_history,
    invoice_amount,
    no_invoice_amt,
    invoice_amount_all,
    no_invoice_amt_history,
    unbill_amt_all,
    no_invoice_amount_all,
    max_statement_date
 from csx_analyse_tmp.csx_analyse_tmp_bill_settle_03 a 
 left join csx_analyse_tmp.tmp_csx_dim_crm_customer_business_ownership b  
    on a.customer_code=b.customer_code 
    and a.business_attribute_name=b.business_attribute_name 
where   b.customer_name is not null
group by b.performance_region_code,
    b.performance_region_name,
    b.performance_province_code,
    b.performance_province_name,
    b.performance_city_code,
    b.performance_city_name ,
    a.customer_code,
    b.customer_name,
    a.credit_code,
    a.company_code,
    a.business_attribute_name,
    b.work_no_new,
    b.sales_name_new,
    b.service_user_work_no,
    b.service_user_name,
    a.bill_code,
    a.source_bill_no,
    a.reconciliation_period,
    a.statement_date,
    a.happen_date,
    a.bill_date_section,
    a.order_amt	,
    a.statement_amount	,
    a.kp_amount	,
    a.confirm_date,
    a.diff_confirm_days,
    a.invoice_date,
    a.diff_invoice_days,
    row_confirm,
    row_invoice,
    order_total_amt,
    bill_total_amt,
    kp_total_amt,
    last_sale_amt,
    last_bill_amt,
    unbill_amt,
    bill_amt_all,
    unbill_amount_history,
    invoice_amount,
    no_invoice_amt,
    invoice_amount_all,
    no_invoice_amt_history,
    unbill_amt_all,
    no_invoice_amount_all,
    max_statement_date
    ;
 

-- drop table csx_analyse_tmp.csx_analyse_tmp_bill_settle_04_1;
-- create table  csx_analyse_tmp.csx_analyse_tmp_bill_settle_04_1 as 
with tmp_csx_analyse_tmp_bill_settle as (
  select
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name ,
    a.customer_code,
    a.customer_name,
    a.credit_code,
    a.company_code,
    business_attribute_name,
    work_no_new,
    sales_name_new,
    service_user_work_no,
    service_user_name,
    a.bill_code,
    a.source_bill_no,
    reconciliation_period,
    a.statement_date,
    a.happen_date,
    bill_date_section,
    order_amt	,
    statement_amount	,
    kp_amount	,
    confirm_date,
    diff_confirm_days,
    invoice_date,
    diff_invoice_days,
    row_confirm,
    row_invoice,
    order_total_amt,
    bill_total_amt,
    kp_total_amt,
    last_sale_amt,
    bill_amt_all,
    last_bill_amt,
    unbill_amt,
    unbill_amount_history,
    invoice_amount,
    no_invoice_amt,
    invoice_amount_all,
    no_invoice_amt_history,
    unbill_amt_all,
    no_invoice_amount_all,
    max_statement_date,
    case when unbill_amount_history<> 0 and b.unstatement_amount_history <>0 then datediff('${data_range_date1}',b.statement_date) 
        -- when unbill_amount_history<> 0 and  confirm_date is not NULL  and order_amt-statement_amount<>0 then datediff(confirm_date,statement_date) 
        when unbill_amt <> 0 and b.unstatement_amount <>0  then datediff('${data_range_date1}',b.statement_date)
        -- when unbill_amt<>0   and order_amt-statement_amount<>0   then datediff(confirm_date,statement_date)
        else 0 end as no_confirm_days,
    case when a.no_invoice_amt_history<> 0  and b.no_kp_amount_history<>0    then datediff('${data_range_date1}',b.statement_date)
        when no_invoice_amt <>0  and no_kp_amount <>0 then datediff('${data_range_date1}',b.statement_date) 
        -- when no_invoice_amt<>0   and order_amt-kp_amount<>0  and  invoice_date is not NULL   then datediff(invoice_date,statement_date)
        else 0 end as no_invoice_days,
    b.no_kp_amount_history as no_kp_amount_history,
    b.no_kp_amount,
    b.unstatement_amount,
    b.unstatement_amount_history,
    b.unstatement_amount_all,
    b.no_kp_amount_all
  from
    csx_analyse_tmp.csx_analyse_tmp_bill_settle_04 a
 join 
    (select
      customer_code,
      company_code,
      credit_code,
      bill_code,
      source_bill_no,
      statement_date,
      happen_date,
      no_kp_amount,
      (no_kp_amount_history) as no_kp_amount_history,
      unstatement_amount,
      unstatement_amount_history,
      sum(unstatement_amount+unstatement_amount_history)over(partition by customer_code,company_code,credit_code) as unstatement_amount_all,
      sum(no_kp_amount+no_kp_amount_history)over(partition by customer_code,company_code,credit_code) as no_kp_amount_all
from
  csx_analyse_tmp.csx_analyse_tmp_customer_credit_invoice_bill_unmount_detail
  ) b on a.customer_code=b.customer_code and a.source_bill_no=b.source_bill_no  
 where unstatement_amount_all>=1000 or no_kp_amount_all>=1000
)
,
tmp_customer_deferred_reconciliation_invoice_detail as 
  (SELECT
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    customer_code,
    customer_name,
    credit_code,
    company_code,
    business_attribute_name,
    work_no_new,
    sales_name_new,
    service_user_work_no,
    service_user_name,
    bill_code,
    source_bill_no,
    reconciliation_period,
    statement_date,
    happen_date,
    bill_date_section,
    order_amt,
    statement_amount,
    kp_amount,
    confirm_date,
    diff_confirm_days,
    invoice_date,
    diff_invoice_days,
    row_confirm,
    row_invoice,
    order_total_amt,
    bill_total_amt,
    kp_total_amt,
    last_sale_amt,
    bill_amt_all,
    last_bill_amt,
    unbill_amt,
    unbill_amount_history,
    invoice_amount,
    no_invoice_amt,
    invoice_amount_all,
    no_invoice_amt_history,
    unbill_amt_all,
    no_invoice_amount_all,
    no_confirm_days,
    no_invoice_days,
    max_statement_date,
    no_kp_amount_history,
    unstatement_amount,
    unstatement_amount_history,
    unstatement_amount_all,
    no_kp_amount_all,
    ROW_NUMBER() OVER(PARTITION BY customer_code, business_attribute_name ORDER BY CAST(diff_confirm_days AS INT) DESC) AS diff_confirm_days_ro,
    ROW_NUMBER() OVER(PARTITION BY customer_code, business_attribute_name ORDER BY CAST(diff_invoice_days AS INT) DESC) AS diff_invoice_days_ro,
    ROW_NUMBER() OVER(PARTITION BY customer_code, business_attribute_name ORDER BY CAST(no_confirm_days AS INT) DESC) AS diff_no_confirm_days_ro,
    ROW_NUMBER() OVER(PARTITION BY customer_code, business_attribute_name ORDER BY CAST(no_invoice_days AS INT) DESC) AS diff_no_invoice_days_ro,
    ROW_NUMBER() OVER(PARTITION BY customer_code, business_attribute_name ORDER BY statement_date DESC) AS rn,
    CURRENT_TIMESTAMP() AS update_time,
    SUBSTR('${partition_date}', 1, 6) AS s_month,
    SUBSTR('${partition_date}', 1, 6) AS smt
  FROM tmp_csx_analyse_tmp_bill_settle
 ) 
insert overwrite table csx_analyse.csx_analyse_hr_customer_deferred_reconciliation_invoice_detail_mf partition(smt)
SELECT
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    customer_code,
    customer_name,
    credit_code,
    company_code,
    business_attribute_name,
    work_no_new,
    sales_name_new,
    service_user_work_no,
    service_user_name,
    bill_code,
    source_bill_no,
    reconciliation_period,
    statement_date,
    happen_date,
    bill_date_section,
    order_amt,
    statement_amount,
    kp_amount,
    confirm_date,
    diff_confirm_days,
    invoice_date,
    diff_invoice_days,
    row_confirm,
    row_invoice,
    order_total_amt,
    bill_total_amt,
    kp_total_amt,
    last_sale_amt,
    bill_amt_all,
    last_bill_amt,
    unbill_amt,
    unbill_amount_history,
    invoice_amount,
    no_invoice_amt,
    invoice_amount_all,
    no_invoice_amt_history,
    unbill_amt_all,
    no_invoice_amount_all,
    no_confirm_days,
    no_invoice_days,
    max_statement_date,
    no_kp_amount_history,
    unstatement_amount,
    unstatement_amount_history,
    unstatement_amount_all,
    no_kp_amount_all,
    diff_confirm_days_ro,
    diff_invoice_days_ro,
    diff_no_confirm_days_ro,
    diff_no_invoice_days_ro,
    rn,
    CURRENT_TIMESTAMP() AS update_time,
    SUBSTR('${partition_date}', 1, 6) AS s_month,
    SUBSTR('${partition_date}', 1, 6) AS smt
  FROM tmp_customer_deferred_reconciliation_invoice_detail a 
  where diff_no_confirm_days_ro=1 or diff_no_invoice_days_ro=1
  ;

