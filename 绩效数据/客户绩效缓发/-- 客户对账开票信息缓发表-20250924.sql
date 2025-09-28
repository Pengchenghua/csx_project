-- 客户对账开票信息缓发表-20250924
-- drop table  csx_analyse_tmp.csx_analyse_tmp_bill_settle_02;
create table csx_analyse_tmp.csx_analyse_tmp_bill_settle_02 as 
with  temp_company_credit as 
  ( select
  customer_code,
  credit_code,
  customer_name,
  business_attribute_code,
  business_attribute_name,
  company_code,
  status,
  is_history_compensate
from
    csx_dim.csx_dim_crm_customer_company_details
where
  sdt = 'current'
group by customer_code,
    credit_code,
    customer_name,
    business_attribute_code,
    business_attribute_name,
    company_code,
    status,
    is_history_compensate
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
    sum(order_amt)over(partition by bill_code,a.customer_code,b.business_attribute_name,a.credit_code,a.company_code ) as order_total_amt,
    sum(invoice_amount)over(partition by bill_code,a.customer_code,b.business_attribute_name,a.credit_code,a.company_code) as invoice_total_amt,
    overdue_date
  from       csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di a
  LEFT JOIN temp_company_credit b on a.customer_code=b.customer_code and a.company_code=b.company_code and a.credit_code=b.credit_code
  where sdt = '20250922' 
  and happen_date>='2025-01-01'
--   and bill_end_date<='2025-09-22'
  and source_sys !='BEGIN'
  and a.customer_code not like 'C%'
  and bbc_bill_flag<>1  -- 剔除BBC无需对账
  )
 ,
 -- 客户对账单
tmp_sss_customer_statement_account_di as 
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
 where    sdt<='20250922'
--  and sdt>='20241001'
    and delete_flag = 0
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
       sdt <= '20250922'
    --   and invoice_status_code=2
    --   and sync_status=1   -- 发票更新状态
    --   and cx_invoice_no_code is null 
    group by
      order_code,
      source_bill_no
  ) b on a.order_code = b.order_code

where
   sdt <= '20250922'
--   and source_bill_no='OM25050100002822'
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
    case when coalesce(confirm_date,'') ='' then  datediff('2025-09-22',statement_date) 
        when (statement_amount-order_amt) = 0 then  datediff(confirm_date,statement_date)
        else  datediff('2025-09-22',statement_date) end as diff_confirm_days,
    case when coalesce(c.invoice_date,'') ='' then  datediff('2025-09-22',statement_date)
        when (order_amt-kp_amount)=0 then datediff(c.invoice_date,statement_date) 
        else datediff('2025-09-22',statement_date) end  as diff_invoice_days
  from tmp_bill_order a 
  left join tmp_sss_customer_statement_account_di b on a.bill_code=b.bill_code 
  left join tmp_sss_invoice_detail c on   a.source_bill_no= c.source_bill_no
  where 
--   order_amt-statement_amount <>0
    order_total_amt>0
)
select source_bill_no,
	bill_code,
    customer_code,
	credit_code,
    company_code,
    business_attribute_code,
    business_attribute_name,
    source_sys,
    happen_date,
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
    diff_invoice_days
  from tmp_bill_confirm a 
WHERE
  (
    (confirm_date BETWEEN '2025-08-26' AND '2025-09-22' OR confirm_date IS NULL)
    OR
    (invoice_date BETWEEN '2025-08-26' AND '2025-09-22' OR invoice_date IS NULL)
  )


    --  AND (bill_amt>=0 OR bill_amt IS NULL )
    --  and (diff_confirm_days>=20 or diff_invoice_days>=25)

;



-- 客户对账开票信息缓发表明细
-- drop table csx_analyse_tmp.csx_analyse_tmp_bill_settle_03;
create table csx_analyse_tmp.csx_analyse_tmp_bill_settle_03 as 
with  tmp_csx_dim_crm_customer_business_ownership as 
 (select
  customer_no as customer_code,
  customer_name,
  work_no_new,
  sales_name_new,
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
  sdt = regexp_replace('2025-09-22', '-', '')
    -- and rp_service_user_work_no_new<>''
union all

select
  customer_no as customer_code,
  customer_name,
  work_no_new,
  sales_name_new,
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
  sdt = regexp_replace('2025-09-22', '-', '')
--   and fl_service_user_work_no_new<>''
union all
select
  customer_no as customer_code,
  customer_name,
  work_no_new,
  sales_name_new,
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
  sdt = regexp_replace('2025-09-22', '-', '')
--   and bbc_service_user_work_no_new<>''
  )
  ,
  tmp_sale_info as 
 (select
  a.customer_code,
  a.customer_name,
  a.work_no_new,
  a.sales_name_new,
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
  work_no_new,
  sales_name_new,
  a.region_code as performance_region_code,
  a.region_name as performance_region_name,
  a.province_code as performance_province_code,
  a.province_name as performance_province_name,
  a.city_group_code as performance_city_code,
  a.city_group_name as performance_city_name
from
  csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df a
where
  sdt = regexp_replace('2025-09-22', '-', '')
  )a 
  left join tmp_csx_dim_crm_customer_business_ownership b on a.customer_code=b.customer_code
 )
  ,
  tmp_csx_dws_sss_customer_credit_invoice_bill_settle_stat_di as  (
      select
        customer_code,
        credit_business_attribute_name,
        -- credit_code,
        -- company_code,
        sum(sale_amt) as last_sale_amt,     -- 上期销售额
        sum(bill_amt_all) as bill_amt_all,  -- 累计对账金额
        sum(bill_amt) bill_amt,         -- 当期对账金额
        sum(unbill_amt )unbill_amt,     -- 当前未对账金额
        sum(unbill_amount_history) unbill_amount_history,  -- 历史未对账金额
        sum(invoice_amount_all) invoice_amount_all, -- 累计开票额
        sum(invoice_amount) invoice_amount,     -- 上一期开票额
        sum(no_invoice_amt) no_invoice_amt ,-- 上一期未开票额
        sum(no_invoice_amt_history) no_invoice_amt_history  -- 历史未开票额
      from
          csx_dws.csx_dws_sss_customer_credit_invoice_bill_settle_stat_di
      where
        sdt = regexp_replace('2025-09-22', '-', '')
      group by
        customer_code,
        credit_business_attribute_name
        -- credit_code,
        -- company_code
    ) ,
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
    concat_ws('-',bill_start_date,bill_end_date) as bill_date_section,
    row_number()over(partition by a.customer_code ,a.business_attribute_name order by diff_confirm_days desc ) row_confirm,
    row_number()over(partition by a.customer_code ,a.business_attribute_name order by diff_invoice_days desc ) row_invoice,
    sum(statement_amount)over(partition by a.customer_code,a.bill_code) as bill_total_amt,
    sum(kp_amount)over(partition by a.customer_code,a.bill_code) as kp_total_amt
from csx_analyse_tmp.csx_analyse_tmp_bill_settle_02 a 
left join 
    tmp_csx_dws_sss_customer_credit_invoice_bill_settle_stat_di b 
        on a.customer_code=b.customer_code 
        and a.business_attribute_name=b.credit_business_attribute_name 
        -- and a.credit_code=b.credit_code 
        -- and a.company_code=b.company_code
)
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
    no_invoice_amt_history
 from tmp_csx_analyse_tmp_bill_settle a 
 left join tmp_sale_info b  on a.customer_code=b.customer_code and a.business_attribute_name=b.business_attribute_name 
 where  statement_date is not null
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
    no_invoice_amt_history
;

-- 明细导了
select * from  csx_analyse_tmp.csx_analyse_tmp_bill_settle_03 where customer_name is not null 
and  (diff_confirm_days>=20 or diff_invoice_days>=25) and (unbill_amt+ unbill_amount_history>=1000 or no_invoice_amt+no_invoice_amt_history>=1000)

;



-- 1、需要处理的字段(a.unbill_amt+a.unbill_amount_history)  有错误值，需要做特殊处理
-- 2、剔除项目制、预付款客户
-- 3、剔除对账单号对账金额与单据金额一致的单号
-- 结果表
-- desc  csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di
create table  csx_analyse_tmp.csx_analyse_tmp_bill_settle_04 as 
with tmp_csx_analyse_tmp_bill_settle as (
  select
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name ,
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
    row_number() over(partition by customer_code,business_attribute_name order by  cast(diff_confirm_days as int) desc  ) diff_confirm_days_ro,
    row_number() over(partition by customer_code,business_attribute_name order by cast(diff_invoice_days as int) desc  ) diff_invoice_days_ro,
    row_number() over(partition by customer_code, business_attribute_name) rn
  from
    csx_analyse_tmp.csx_analyse_tmp_bill_settle_03
    where (diff_confirm_days>=20 or diff_invoice_days>=25)
     and (unbill_amt+ unbill_amount_history>=1000 or no_invoice_amt+no_invoice_amt_history>=1000)
     and customer_name is not null 
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
    b.statement_date,
    -- a.bill_amt,
    b.confirm_date,
    b.diff_confirm_days,
    b.statement_amount,
    a.last_sale_amt, -- 上一结算期销售金额 
    a.last_bill_amt,     -- 上一结算期对账金额 
    a.unbill_amt,    -- 上一结算期未对账金额
    a.bill_amt_all, -- 历史对账金额 
    a.unbill_amount_history,   -- 历史未对帐金额
    if(a.unbill_amt+a.unbill_amount_history = 0, '是', '否') as confirm_flag,   -- 是否完成对账
    c.statement_date as invoice_statement_date,     -- 开票对账日期
    c.invoice_date as max_invoice_date,             -- 开票日期
    c.diff_invoice_days,                            -- 开票天数
    c.kp_amount,                                    -- 开票金额
    a.invoice_amount    ,                           -- 上一期开票金额
    a.no_invoice_amt,                               -- 上一期未开票金额
    a.invoice_amount_all,                           -- 历史开票金额
    a.no_invoice_amt_history,                       -- 历史未开票金额
    if(a.no_invoice_amt+a.no_invoice_amt_history = 0, '是', '否') kp_flag,
    case  when (a.unbill_amt+a.unbill_amount_history) = 0   and (a.no_invoice_amt+a.no_invoice_amt_history) = 0 then '否'
      else if(coalesce(b.diff_confirm_days, 0) > 20   and coalesce(b.diff_confirm_days, 0) <= 50, '是',  '否'  )  end as tc_bill_type,
    -- 对账缓发
    case when  (a.unbill_amt+a.unbill_amount_history) = 0  and (a.no_invoice_amt+a.no_invoice_amt_history) = 0 then '否'
      else if(coalesce(b.diff_confirm_days, 0) > 50, '是', '否')
    end as tc_history_bill_type,
    -- 对账扣发
    case when  (a.unbill_amt+a.unbill_amount_history) = 0  and (a.no_invoice_amt+a.no_invoice_amt_history) = 0 then '否'
      else 
      if( coalesce(c.diff_invoice_days, 0) > 25   and coalesce(c.diff_invoice_days, 0) <= 55,  '是', '否' )  end tc_invoice_type,
    case
      when  (a.unbill_amt+a.unbill_amount_history) = 0
      and (a.no_invoice_amt+a.no_invoice_amt_history) = 0 then '否'
      else if(coalesce(c.diff_invoice_days, 0) > 55, '是', '否')
    end tc_history_invoice_type 
    -- if( coalesce(b.diff_confirm_days,0)<=50  and c.tc_bill_type='是' ,'是','否') as  tc_reissue_bill_type,  -- 对账扣发
    -- if( coalesce(b.diff_invoice_days,0) <=55 and c.tc_invoice_type='是' ,'是','否') tc_reissue_invoice_type,
  from
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
    sum(no_invoice_amt_history) no_invoice_amt_history
from
( select
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
    (no_invoice_amt_history) no_invoice_amt_history
from
    tmp_csx_analyse_tmp_bill_settle
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
(no_invoice_amt_history)
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
) a
    left join (
      select
        *
      from
        tmp_csx_analyse_tmp_bill_settle
      where
        diff_confirm_days_ro = 1
    ) b on a.customer_code = b.customer_code
    and a.business_attribute_name = b.business_attribute_name
    left join (
      select
        *
      from
        tmp_csx_analyse_tmp_bill_settle
      where
        diff_invoice_days_ro = 1
    ) c on a.customer_code = c.customer_code
    and a.business_attribute_name = c.business_attribute_name -- where a.customer_code='124059'
   
)
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
  reconciliation_period,
  statement_date,
  -- a.bill_amt,
  confirm_date,
  diff_confirm_days,
  statement_amount,
  a.last_sale_amt, -- 上一结算期销售金额 
  a.last_bill_amt,     -- 上一结算期对账金额 
  a.unbill_amt,    -- 上一结算期未对账金额
  a.bill_amt_all, -- 历史对账金额 
  a.unbill_amount_history,   -- 历史未对帐金额
  confirm_flag,
  invoice_statement_date,
  max_invoice_date,
  diff_invoice_days,
  kp_amount,
  invoice_amount    ,                           -- 上一期开票金额
  no_invoice_amt,                               -- 上一期未开票金额
  invoice_amount_all,                           -- 历史开票金额
  no_invoice_amt_history,                       -- 历史未开票金额
  kp_flag,
  tc_bill_type,
  -- 对账缓发
  tc_history_bill_type,
  -- 对账扣发
  tc_invoice_type,
  tc_history_invoice_type,
  case
    when tc_history_bill_type = '是'
    or tc_history_invoice_type = '是' then '否'
    when tc_bill_type = '是'
    or tc_invoice_type = '是' then '是'
    else '否'
  end final_outcome_type,
  if(
    tc_history_bill_type = '是'
    or tc_history_invoice_type = '是',
    '是',
    '否'
  ) final_result_withheld_type
from
  tmp_ruslt_01 a
;