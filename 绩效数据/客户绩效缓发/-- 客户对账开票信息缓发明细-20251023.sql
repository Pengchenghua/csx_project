-- 客户对账开票信息缓发明细-20251023
-- 永辉平台任务 csx_analyse_tmp_bill_settle
-- 
-- 创建管家销售员信息 
create table csx_analyse_tmp.tmp_csx_dim_crm_customer_business_ownership as 
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
  sdt = regexp_replace('2025-10-22', '-', '')
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
  sdt = regexp_replace('2025-10-22', '-', '')
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
  sdt = regexp_replace('2025-10-22', '-', '')
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
  sdt = regexp_replace('2025-10-22', '-', '')
  )a 
  left join tmp_csx_dim_crm_customer_business_ownership b on a.customer_code=b.customer_code
 ) select * from tmp_csx_dim_crm_customer_business_ownership
 ;



-- drop table csx_analyse_tmp.csx_analyse_tmp_bill_settle_00 ;
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
  where sdt = '20251022' 
  and happen_date>='2025-01-01'
  and date_add(bill_start_date,1)>='2025-01-01'  
  and source_sys !='BEGIN'
  and a.customer_code not like 'C%'
  and bbc_bill_flag<>1  -- 剔除BBC无需对账
  )select * from tmp_bill_order
  ;

--  drop table csx_analyse_tmp.csx_analyse_tmp_bill_settle_02
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
    where    sdt<='20251022'
    and sdt>='20250101'
    and delete_flag = 0
    and  to_date(confirm_time)<='2025-10-22'
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
       sdt <= '20251022'
       and sdt>='20250101'
    --   and invoice_status_code=2
    --   and sync_status=1   -- 发票更新状态
    --   and cx_invoice_no_code is null 
    group by
      order_code,
      source_bill_no
  ) b on a.order_code = b.order_code

where
   sdt <= '20251022'
   and sdt>='20250101'
   and  to_date(invoice_time)<='2025-10-22'
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
    case when coalesce(confirm_date,'') ='' then  datediff('2025-10-22',statement_date) 
        when (statement_amount-order_amt) = 0 then  datediff(confirm_date,statement_date)
        else  datediff('2025-10-22',statement_date) end as diff_confirm_days,
    case when coalesce(c.invoice_date,'') ='' then  datediff('2025-10-22',statement_date)
        when (order_amt-kp_amount)=0 then datediff(c.invoice_date,statement_date) 
        else datediff('2025-10-22',statement_date) end  as diff_invoice_days
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
  (
    (confirm_date BETWEEN '2025-09-23' AND '2025-10-22' OR coalesce(confirm_date,'')='')
    OR
    (invoice_date BETWEEN '2025-09-23' AND '2025-10-22' OR  coalesce(invoice_date,'')='')
  )
  and statement_date>='2025-01-01'
  and coalesce(a.payment_terms,'') !='Z007'  --  账期类型  Z007剔除
  and coalesce(a.statement_mode_code,'') !='5'  -- 项目制客户剔除
 

;

-- drop table csx_analyse_tmp.csx_analyse_tmp_bill_settle_03;


-- 计算未对账与历史未对账金额
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
        sdt = regexp_replace('2025-10-22', '-', '')
      group by
        customer_code,
        credit_business_attribute_name
        -- credit_code,
        -- company_code
    )select * from tmp_csx_dws_sss_customer_credit_invoice_bill_settle_stat_di
;



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
    where  statement_date <='2025-10-22'
     and (coalesce(b.unbill_amt_all,0)>=1000 or coalesce(b.no_invoice_amount_all,0)>=1000)
     
 
) 
select * from tmp_csx_analyse_tmp_bill_settle
;

-- drop table csx_analyse_tmp.csx_analyse_tmp_bill_settle_04;
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
 

;

drop table csx_analyse_tmp.csx_analyse_tmp_bill_settle_04_1;
create table  csx_analyse_tmp.csx_analyse_tmp_bill_settle_04_1 as 
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
    case when unbill_amount_history<> 0 and b.unstatement_amount_history <>0 then datediff('2025-10-22',b.statement_date) 
        -- when unbill_amount_history<> 0 and  confirm_date is not NULL  and order_amt-statement_amount<>0 then datediff(confirm_date,statement_date) 
        when unbill_amt <> 0 and b.unstatement_amount <>0  then datediff('2025-10-22',b.statement_date)
        -- when unbill_amt<>0   and order_amt-statement_amount<>0   then datediff(confirm_date,statement_date)
        else 0 end as no_confirm_days,
    case when a.no_invoice_amt_history<> 0  and b.no_kp_amount_history<>0    then datediff('2025-10-22',b.statement_date)
        when no_invoice_amt <>0  and no_kp_amount <>0 then datediff('2025-10-22',b.statement_date) 
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
    unbill_amt_all,
    no_invoice_amount_all,
    no_confirm_days,
    no_invoice_days,
    max_statement_date,
    no_kp_amount_history ,
    unstatement_amount,
    unstatement_amount_history,
    unstatement_amount_all,
    no_kp_amount_all,
    row_number() over(partition by customer_code,business_attribute_name order by  cast(diff_confirm_days as int) desc  ) diff_confirm_days_ro,
    row_number() over(partition by customer_code,business_attribute_name order by cast(diff_invoice_days as int) desc  ) diff_invoice_days_ro,
    row_number() over(partition by customer_code,business_attribute_name order by  cast(no_confirm_days as int) desc  )  diff_no_confirm_days_ro,
    row_number() over(partition by customer_code,business_attribute_name order by cast(no_invoice_days as int) desc  ) diff_no_invoice_days_ro,
    row_number() over(partition by customer_code,business_attribute_name order by statement_date desc ) rn
  from tmp_csx_analyse_tmp_bill_settle 

;



drop table csx_analyse_tmp.csx_analyse_tmp_bill_settle_05;
create table  csx_analyse_tmp.csx_analyse_tmp_bill_settle_05 as 

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
    csx_analyse_tmp.csx_analyse_tmp_bill_settle_04_1
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
      else if(coalesce(b.diff_confirm_days, 0)-7 > 20   and coalesce(b.diff_confirm_days, 0)-7 <= 50, '是',  '否'  )  end as tc_bill_type,
    -- 对账缓发
    case when  coalesce(a.unbill_amt_all) = 0   then '否'
      else if(coalesce(b.diff_confirm_days, 0)-7 > 50, '是', '否')
    end as tc_history_bill_type,
    -- 对账扣发
    case when   coalesce(a.no_invoice_amount_all,0) = 0 then '否'
      else 
      if( coalesce(c.diff_invoice_days, 0)-7 > 25   and coalesce(c.diff_invoice_days, 0)-7 <= 55,  '是', '否' )  end tc_invoice_type,
    case
      when  coalesce(a.no_invoice_amount_all,0)= 0  then '否'
       else if(coalesce(c.diff_invoice_days, 0)-7 > 55, '是', '否')
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
        no_kp_amount_history ,
        unstatement_amount,
        unstatement_amount_history,
        unstatement_amount_all
      from
        csx_analyse_tmp.csx_analyse_tmp_bill_settle_04_1
      where
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
        no_kp_amount_history ,
        unstatement_amount,
        unstatement_amount_history,
        unstatement_amount_all,
        no_kp_amount_all
      from
        csx_analyse_tmp.csx_analyse_tmp_bill_settle_04_1
      where
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
        no_kp_amount_history ,
        unstatement_amount,
        unstatement_amount_history,
        unstatement_amount_all,
        no_kp_amount_all
      from
        csx_analyse_tmp.csx_analyse_tmp_bill_settle_04_1
      where
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
        no_kp_amount_history ,
        unstatement_amount,
        unstatement_amount_history,
        unstatement_amount_all,
        no_kp_amount_all
      from
        csx_analyse_tmp.csx_analyse_tmp_bill_settle_04_1
      where
        diff_no_invoice_days_ro=1
    ) c1 on a.customer_code = c1.customer_code
    and a.business_attribute_name = c1.business_attribute_name -- where a.customer_code='124059'
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
  (confirm_date ) as confirm_date,
  (diff_confirm_days ) as diff_confirm_days,
  (statement_amount ) as statement_amount,
  a.last_sale_amt, -- 上一结算期销售金额 
  a.last_bill_amt,     -- 上一结算期对账金额 
  a.unbill_amt,    -- 上一结算期未对账金额
  a.bill_amt_all, -- 历史对账金额 
  a.unbill_amount_history,   -- 历史未对帐金额
  confirm_flag,               -- 是否完全对账
  invoice_statement_date,
  (max_invoice_date ) as max_invoice_date,
  (diff_invoice_days ) as diff_invoice_days,
  (kp_amount ) as kp_amount,
  invoice_amount    ,                           -- 上一期开票金额
  no_invoice_amt,                               -- 上一期未开票金额
  invoice_amount_all,                           -- 历史开票金额
  no_invoice_amt_history,                       -- 历史未开票金额
  kp_flag,                                      -- 是否完全开票 
  tc_bill_type,
  -- 对账缓发
  tc_history_bill_type,
  -- 对账扣发
  tc_invoice_type,
  tc_history_invoice_type,
  case
    when tc_bill_type = '是'
    or tc_invoice_type = '是' then '是'
    else '否'
  end final_outcome_type,
  if(
    tc_history_bill_type = '是'
    or tc_history_invoice_type = '是',
    '是',
    '否'
  ) final_result_withheld_type,
   c_statement_date,
    c1_statement_date,
    unbill_amt_all,
    unstatement_amount_all,
    no_invoice_amount_all,
    no_kp_amount_all
from
  tmp_ruslt_01 a

;

-- 结果导出
select performance_region_name
,performance_province_name
,performance_city_name
,customer_code
,customer_name
,business_attribute_name
,work_no_new
,sales_name_new
,service_user_work_no
,service_user_name
,reconciliation_period
,statement_date
,confirm_date
,if(diff_confirm_days=0,0,diff_confirm_days-7)  diff_confirm_days
,last_sale_amt
,unbill_amt
,unbill_amount_history
,confirm_flag
,invoice_statement_date
,max_invoice_date
,if(diff_invoice_days=0,0,diff_invoice_days-7) diff_invoice_days
,no_invoice_amt
,no_invoice_amt_history
,kp_flag
,tc_bill_type
,tc_history_bill_type
,tc_invoice_type
,tc_history_invoice_type
,final_outcome_type
,final_result_withheld_type
 from csx_analyse_tmp.csx_analyse_tmp_bill_settle_05 
 where
  (statement_date <> '' or invoice_statement_date<>'') and (unbill_amt_all>=1000 or no_invoice_amount_all>=1000)
  

  
;

279555 103997 279372 256736 277858 277437 125301 126387 125306 125394 

select performance_region_name,performance_province_name,
performance_city_name,
customer_code,	
customer_name,	
credit_code	,
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
diff_invoice_days	,
bill_total_amt,
kp_total_amt,
last_sale_amt,
last_bill_amt,
unbill_amt,
unbill_amount_history,
no_invoice_amt,
no_invoice_amt_history,
no_confirm_days,
no_invoice_days,
diff_confirm_days_ro	,	
diff_invoice_days_ro	,	
diff_no_confirm_days_ro	,	
diff_no_invoice_days_ro	
from    csx_analyse_tmp.csx_analyse_tmp_bill_settle_04_1  
where  diff_no_confirm_days_ro=1 or diff_no_invoice_days_ro=1
;



select performance_region_name,performance_province_name,
performance_city_name,
customer_code,	
customer_name,	
credit_code	,
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
diff_invoice_days	,
bill_total_amt,
kp_total_amt,
last_sale_amt,
last_bill_amt,
unbill_amt,
unbill_amount_history,
no_invoice_amt,
no_invoice_amt_history,
no_confirm_days,
no_invoice_days,
diff_confirm_days_ro	,	
diff_invoice_days_ro	,	
diff_no_confirm_days_ro	,	
diff_no_invoice_days_ro	
from    csx_analyse_tmp.csx_analyse_tmp_bill_settle_04_1  
where  ((diff_no_confirm_days_ro=1  and (unbill_amount_history>=1000 or unbill_amt>=1000))
    or (diff_no_invoice_days_ro=1 and (no_invoice_amt_history>=1000 or no_invoice_amt>=1000))
    )