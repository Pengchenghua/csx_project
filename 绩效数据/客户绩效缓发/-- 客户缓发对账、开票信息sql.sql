

--第二版 20250627
-- 按照开票、对账周期在6月1日开始的，按照开票、对账天数取最大的值
-- 开票日期，销售单号与开票单号关联
-- drop table  csx_analyse_tmp.csx_analyse_tmp_bill_settle;
create table csx_analyse_tmp.csx_analyse_tmp_bill_settle as 
with tmp_bill_settle_detail_di as 
(select source_bill_no,
    bill_code,
    customer_code,
    customer_name,
    company_code,
    happen_date,
    reconciliation_period,
    bill_start_date,
    bill_end_date,
    case when project_end_date='' then  date_add(bill_end_date,1)  
        else concat(substr(add_months(happen_date ,1) ,1,8),if(reconciliation_period <10,concat('0',reconciliation_period),reconciliation_period))
        end statement_date,
    project_end_date,
    project_begin_date,
    substr(regexp_replace(to_date(happen_date),'-',''),1,6) happen_month
 from   csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di 
  where sdt = '20250625'
  -- 上一周期对账
  --  and to_date(happen_date ) >='2025-04-01' and to_date(happen_date)<='2025-05-31'
--   and customer_code='254559'

  
 ),
 temp_company_credit as 
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
  -- and status=1
group by customer_code,
    credit_code,
    customer_name,
    business_attribute_code,
    business_attribute_name,
    company_code,
    status,
  is_history_compensate
) 
 -- 客户对账单
tmp_sss_customer_statement_account_di as 
(select bill_code,
        customer_code,
        company_code,
        invoice_time,
        to_date(sale_bill_date) sale_bill_date,
        customer_bill_date,
        to_date(confirm_time) confirm_date
 from  csx_dwd.csx_dwd_sss_customer_statement_account_di 
 where    sdt<='20250625'
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
      -- sdt >= '20250301'
       sdt<='20250625'
      --  and invoice_status_code=2
      --  and sync_status=1   -- 发票更新状态
      --  and cx_invoice_no_code is null 
    group by
      order_code,
      source_bill_no
  ) b on a.order_code = b.order_code

where
  -- sdt >= '20250301'
   sdt<='20250625'
--   and source_bill_no='OM25050100002822'
  group by company_code,
  customer_code,
  to_date(invoice_time),
  source_bill_no
 ),
 tmp_bill_invoice_detail as (
  select happen_month,
    a.source_bill_no,
    a.bill_code,
    a.customer_code,
    a.customer_name,
    a.company_code,
    a.happen_date,
    a.reconciliation_period,
    a.bill_start_date,
    a.bill_end_date,
    b.confirm_date,
    c.invoice_date,
    project_end_date,
    project_begin_date,
    sale_bill_date,
    customer_bill_date,
    statement_date
from tmp_bill_settle_detail_di a 
  left join tmp_sss_customer_statement_account_di b on a.bill_code=b.bill_code and a.company_code=b.company_code
  left join tmp_sss_invoice_detail c on a.source_bill_no=c.source_bill_no
--  where (b.confirm_date<='2025-05-25' or c.invoice_date<='2025-05-25')
--  and bill_end_date>='2025-01-01'
--  and a.bill_code='SDZ250210000482'
 )
select a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name ,
    a.customer_code,
    a.customer_name,
    a.work_no_new,
    a.sales_name_new,
    a.rp_service_user_work_no,
    a.rp_service_user_name,
    a.fl_service_user_work_no,
    a.fl_service_user_name,
    a.bbc_service_user_work_no,
    a.bbc_service_user_name,
 from tmp_bill_invoice_detail 
-- where statement_date>='2025-05-01'
where confirm_date>='2025-06-01' or invoice_date>='2025-06-01'
or confirm_date is null or invoice_date is null
;

-- 对账结果表
with tmp_csx_analyse_tmp_bill_settle as 
(
select *,row_number()over(partition by customer_code order by cast(diff_bill_days as int) desc ) diff_bill_days_ro,
row_number()over(partition by customer_code order by cast(diff_invoice_days as int) desc ) diff_invoice_days_ro
from (select  
    customer_code,
    reconciliation_period,
    statement_date,
    max(max_confirm_date) max_confirm_date,
    max(max_invoice_date) max_invoice_date,
    if(max(max_confirm_date)='' , 0,datediff(max(max_confirm_date),statement_date)) diff_bill_days,
    if(max(max_invoice_date)='' , 0,datediff(max(max_invoice_date),statement_date)) diff_invoice_days
from
(
 select bill_code,
    customer_code,
     reconciliation_period,
    statement_date,
    happen_month,
    concat_ws('-',bill_start_date,bill_end_date) as bill_date_section,
    max(coalesce(confirm_date,'')) max_confirm_date,
    max(coalesce(invoice_date,'')) max_invoice_date
from  csx_analyse_tmp.csx_analyse_tmp_bill_settle
 where  ((confirm_date<='2025-06-25' and confirm_date>='2025-06-01' ) 
 or  (invoice_date <='2025-06-25' and invoice_date>='2025-06-01' ) 
 or confirm_date ='' or invoice_date='') 
-- and happen_month>='202504'
group by bill_code,
    customer_code,
     reconciliation_period,
    statement_date,
    happen_month,
    concat_ws('-',bill_start_date,bill_end_date)
) a 

group by  customer_code,
     reconciliation_period,
    statement_date
)a 
)
,
  tmp_csx_dim_crm_customer_business_ownership as (
  select
    customer_no customer_code,
    customer_name,
    work_no_new,
    sales_name_new,
    a.region_code  as performance_region_code,
    a.region_name  as performance_region_name,
    a.province_code  as performance_province_code,
    a.province_name  as performance_province_name,
    a.city_group_code  as performance_city_code,
    a.city_group_name  as performance_city_name ,
    rp_service_user_work_no_new as rp_service_user_work_no,
    rp_service_user_name_new as rp_service_user_name,
    fl_service_user_work_no_new as fl_service_user_work_no,
    fl_service_user_name_new as fl_service_user_name,
    bbc_service_user_work_no_new as bbc_service_user_work_no,
    bbc_service_user_name_new as bbc_service_user_name
  from
       csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df a
  where
    sdt = regexp_replace(last_day(add_months('${yesterday_date}',-1)),'-','')
)
select  
    a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name ,
    a.customer_code,
    a.customer_name,
    a.work_no_new,
    a.sales_name_new,
    a.rp_service_user_work_no,
    a.rp_service_user_name,
    a.fl_service_user_work_no,
    a.fl_service_user_name,
    a.bbc_service_user_work_no,
    a.bbc_service_user_name,
    coalesce(b.reconciliation_period,c.reconciliation_period) reconciliation_period ,
    b.statement_date,
    b.max_confirm_date,
    b.diff_bill_days,
    c.statement_date as invoice_statement_date,
    c.max_invoice_date as max_invoice_date,
    c.diff_invoice_days ,
    b.diff_bill_days_ro,
    c.diff_invoice_days_ro,
    if(coalesce(b.diff_bill_days,0)>20  ,'是','否') as  tc_bill_type,  -- 对账缓发
    if(coalesce(b.diff_bill_days,0)>50  ,'是','否') as  tc_history_bill_type,  -- 对账扣发
    if(coalesce(c.diff_invoice_days,0)>25 ,'是','否') tc_invoice_type,
    if(coalesce(c.diff_invoice_days,0)>55   ,'是','否') tc_history_invoice_type
    -- if( coalesce(b.diff_bill_days,0)<=50  and c.tc_bill_type='是' ,'是','否') as  tc_reissue_bill_type,  -- 对账扣发
    -- if( coalesce(b.diff_invoice_days,0) <=55 and c.tc_invoice_type='是' ,'是','否') tc_reissue_invoice_type,
from  tmp_csx_dim_crm_customer_business_ownership a 
left  join  
(
select * from tmp_csx_analyse_tmp_bill_settle where diff_bill_days_ro=1) b on a.customer_code=b.customer_code
left join 
(
select * from tmp_csx_analyse_tmp_bill_settle where diff_invoice_days_ro=1) c on a.customer_code=c.customer_code
 
where coalesce(b.reconciliation_period,c.reconciliation_period) is not null 
;



--对账明细

with tmp_csx_analyse_tmp_bill_settle as 
(
 select bill_code,
    customer_code,
    reconciliation_period,
    statement_date,
    happen_month,
    concat_ws('-',bill_start_date,bill_end_date) as bill_date_section,
    max(coalesce(confirm_date,'')) max_confirm_date,
    max(coalesce(invoice_date,'')) max_invoice_date,
    if(max(confirm_date)='' , 0,datediff(max(confirm_date),statement_date)) diff_bill_days,
    if(max(invoice_date)='' , 0,datediff(max(invoice_date),statement_date)) diff_invoice_days
from  csx_analyse_tmp.csx_analyse_tmp_bill_settle
 where  ((confirm_date<='2025-06-25' and confirm_date>='2025-06-01' ) 
      or  (invoice_date <='2025-06-25' and invoice_date>='2025-06-01' ) 
      or confirm_date ='' 
      or invoice_date='') 
-- and happen_month>='202504'
group by bill_code,
    customer_code,
    reconciliation_period,
    statement_date,
    happen_month,
    concat_ws('-',bill_start_date,bill_end_date)
 
)
,
  tmp_csx_dim_crm_customer_business_ownership as (
  select
    customer_no customer_code,
    customer_name,
    work_no_new,
    sales_name_new,
    a.region_code  as performance_region_code,
    a.region_name  as performance_region_name,
    a.province_code  as performance_province_code,
    a.province_name  as performance_province_name,
    a.city_group_code  as performance_city_code,
    a.city_group_name  as performance_city_name ,
    rp_service_user_work_no_new as rp_service_user_work_no,
    rp_service_user_name_new as rp_service_user_name,
    fl_service_user_work_no_new as fl_service_user_work_no,
    fl_service_user_name_new as fl_service_user_name,
    bbc_service_user_work_no_new as bbc_service_user_work_no,
    bbc_service_user_name_new as bbc_service_user_name
  from
       csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df a
  where
    sdt = regexp_replace(last_day(add_months('${yesterday_date}',-1)),'-','')
)
select  
    a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name ,
    a.customer_code,
    a.customer_name,
    a.work_no_new,
    a.sales_name_new,
    a.rp_service_user_work_no,
    a.rp_service_user_name,
    a.fl_service_user_work_no,
    a.fl_service_user_name,
    a.bbc_service_user_work_no,
    a.bbc_service_user_name,
    b.bill_code,
    b.reconciliation_period,
    b.statement_date,
    b.happen_month,
    b.bill_date_section,
    b.max_confirm_date,
    b.max_invoice_date,
    b.diff_bill_days,
    b.diff_invoice_days
from  tmp_csx_dim_crm_customer_business_ownership a 
 join  
(
select * from tmp_csx_analyse_tmp_bill_settle  ) b on a.customer_code=b.customer_code

;

-- 客户缓发对账 20250814 增加业务类型
-- drop table  csx_analyse_tmp.csx_analyse_tmp_bill_settle;
create table csx_analyse_tmp.csx_analyse_tmp_bill_settle as 
with tmp_bill_settle_detail_di as 
(select source_bill_no,
    bill_code,
    credit_code,
    customer_code,
    customer_name,
    company_code,
    happen_date,
    reconciliation_period,
    bill_start_date,
    bill_end_date,
    case when project_end_date='' then  date_add(bill_end_date,1)  
        else concat(substr(add_months(happen_date ,1) ,1,8),if(reconciliation_period <10,concat('0',reconciliation_period),reconciliation_period))
        end statement_date,
    project_end_date,
    project_begin_date,
    substr(regexp_replace(to_date(happen_date),'-',''),1,6) happen_month
 from    csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di 
  where sdt = '20250725'
  -- 上一周期对账
  --  and to_date(happen_date ) >='2025-04-01' and to_date(happen_date)<='2025-05-31'
--   and customer_code='254559'

  
 ),
 temp_company_credit as 
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
 -- 客户对账单
tmp_sss_customer_statement_account_di as 
(select bill_code,
        customer_code,
        company_code,
        invoice_time,
        to_date(sale_bill_date) sale_bill_date,
        customer_bill_date,
        to_date(confirm_time) confirm_date
 from  csx_dwd.csx_dwd_sss_customer_statement_account_di 
 where    sdt<='20250725'
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
      -- sdt >= '20250301'
       sdt<='20250725'
    --   and invoice_status_code=2
    --   and sync_status=1   -- 发票更新状态
    --   and cx_invoice_no_code is null 
    group by
      order_code,
      source_bill_no
  ) b on a.order_code = b.order_code

where
  -- sdt >= '20250301'
   sdt<='20250725'
--   and source_bill_no='OM25050100002822'
  group by company_code,
  customer_code,
  to_date(invoice_time),
  source_bill_no
 ),
 tmp_bill_invoice_detail as (
  select happen_month,
    a.source_bill_no,
    a.bill_code,
    a.credit_code,
    a.customer_code,
    a.customer_name,
    a.company_code,
    d.business_attribute_name,
    a.happen_date,
    a.reconciliation_period,
    a.bill_start_date,
    a.bill_end_date,
    b.confirm_date,
    c.invoice_date,
    project_end_date,
    project_begin_date,
    sale_bill_date,
    customer_bill_date,
    statement_date
from tmp_bill_settle_detail_di a 
  left join tmp_sss_customer_statement_account_di b on a.bill_code=b.bill_code and a.company_code=b.company_code
  left join tmp_sss_invoice_detail c on a.source_bill_no=c.source_bill_no
  left join temp_company_credit d on a.customer_code = d.customer_code and a.credit_code=d.credit_code and a.company_code=d.company_code
 )
 ,
 tmp_csx_analyse_tmp_bill_settle as 
(
 select bill_code,
    customer_code,
    reconciliation_period,
    statement_date,
    business_attribute_name,
    happen_month,
    concat_ws('-',bill_start_date,bill_end_date) as bill_date_section,
    max(coalesce(confirm_date,'')) max_confirm_date,
    max(coalesce(invoice_date,'')) max_invoice_date,
    if(max(confirm_date)='' , 0,datediff(max(confirm_date),statement_date)) diff_bill_days,
    if(max(invoice_date)='' , 0,datediff(max(invoice_date),statement_date)) diff_invoice_days
from  tmp_bill_invoice_detail
 where  ((confirm_date<='2025-07-25' and confirm_date>='2025-07-01' ) 
            or  (invoice_date <='2025-07-25' and invoice_date>='2025-07-01' ) 
            -- or coalesce(confirm_date,'') ='' 
            -- or coalesce(invoice_date,'')=''
      ) 

group by bill_code,
    business_attribute_name,
    customer_code,
    reconciliation_period,
    statement_date,
    happen_month,
    concat_ws('-',bill_start_date,bill_end_date)
 
)
,
 
  tmp_csx_dim_crm_customer_business_ownership as (select
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
  sdt = regexp_replace(last_day(add_months('${yesterday_date}', -1)), '-', '')
    and rp_service_user_work_no_new<>''
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
  sdt = regexp_replace(last_day(add_months('${yesterday_date}', -1)), '-', '')
  and fl_service_user_work_no_new<>''
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
  sdt = regexp_replace(last_day(add_months('${yesterday_date}', -1)), '-', '')
  and bbc_service_user_work_no_new<>''
  )
select a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name ,
    a.customer_code,
    a.customer_name,
    a.business_attribute_name,
    a.work_no_new,
    a.sales_name_new,
    a.service_user_work_no,
    a.service_user_name,
    b.bill_code,
    b.reconciliation_period,
    b.statement_date,
    b.happen_month,
    b.bill_date_section,
    b.max_confirm_date,
    b.max_invoice_date,
    b.diff_bill_days,
    b.diff_invoice_days
 from tmp_csx_dim_crm_customer_business_ownership a 
 left join tmp_csx_analyse_tmp_bill_settle b  on a.customer_code=b.customer_code and a.business_attribute_name=b.business_attribute_name 
 where bill_code is not null 
 

;



-- 加业务 对账结果表
with tmp_csx_analyse_tmp_bill_settle as 
(
select *,
    row_number()over(partition by customer_code,business_attribute_name order by cast(diff_bill_days as int) desc ) diff_bill_days_ro,
    row_number()over(partition by customer_code,business_attribute_name order by cast(diff_invoice_days as int) desc ) diff_invoice_days_ro,
    row_number()over(partition by customer_code,business_attribute_name,service_user_work_no ) rn 
from (select 
    a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name ,
    a.customer_code,
    a.customer_name,
    a.business_attribute_name,
    a.work_no_new,
    a.sales_name_new,
    a.service_user_work_no,
    a.service_user_name,
    reconciliation_period,
    statement_date,
    max(max_confirm_date) max_confirm_date,
    max(max_invoice_date) max_invoice_date,
    if(max(max_confirm_date)='' , 0,datediff(max(max_confirm_date),statement_date)) diff_bill_days,
    if(max(max_invoice_date)='' , 0,datediff(max(max_invoice_date),statement_date)) diff_invoice_days
from
  csx_analyse_tmp.csx_analyse_tmp_bill_settle a 
group by  a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name ,
    a.customer_code,
    a.customer_name,
    a.business_attribute_name,
    a.work_no_new,
    a.sales_name_new,
    a.service_user_work_no,
    a.service_user_name,
    reconciliation_period,
    statement_date
)a 
)
select  
    a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name ,
    a.customer_code,
    a.customer_name,
    a.work_no_new,
    a.sales_name_new,
    a.service_user_work_no,
    a.service_user_name,
    a.business_attribute_name,
    coalesce(a.reconciliation_period,c.reconciliation_period) reconciliation_period ,
    a.statement_date,
    b.max_confirm_date,
    b.diff_bill_days,
    c.statement_date as invoice_statement_date,
    c.max_invoice_date as max_invoice_date,
    c.diff_invoice_days ,
    a.diff_bill_days_ro,
    c.diff_invoice_days_ro,
    if(coalesce(b.diff_bill_days,0)>20  ,'是','否') as  tc_bill_type,  -- 对账缓发
    if(coalesce(b.diff_bill_days,0)>50  ,'是','否') as  tc_history_bill_type,  -- 对账扣发
    if(coalesce(c.diff_invoice_days,0)>25 ,'是','否') tc_invoice_type,
    if(coalesce(c.diff_invoice_days,0)>55   ,'是','否') tc_history_invoice_type
    -- if( coalesce(b.diff_bill_days,0)<=50  and c.tc_bill_type='是' ,'是','否') as  tc_reissue_bill_type,  -- 对账扣发
    -- if( coalesce(b.diff_invoice_days,0) <=55 and c.tc_invoice_type='是' ,'是','否') tc_reissue_invoice_type,
from  
(select * from tmp_csx_analyse_tmp_bill_settle where diff_bill_days_ro=1) a
left join 
(select * from tmp_csx_analyse_tmp_bill_settle where diff_bill_days_ro=1) b on a.customer_code=b.customer_code
left join 
(
select * from tmp_csx_analyse_tmp_bill_settle where diff_invoice_days_ro=1) c on a.customer_code=c.customer_code
where coalesce(a.reconciliation_period,c.reconciliation_period) is not null 
;


createtab_stmt
CREATE EXTERNAL TABLE `csx_analyse`.`csx_analyse_hr_customer_bill_account_delay`(
  `performance_region_code` string COMMENT '大区编码', 
  `performance_region_name` string COMMENT '大区名称', 
  `performance_province_code` string COMMENT '省区编码', 
  `performance_province_name` string COMMENT '省区名称', 
  `performance_city_code` string COMMENT '城市编码', 
  `performance_city_name` string COMMENT '城市名称', 
  `customer_code` string COMMENT '客户编码', 
  `customer_name` string COMMENT '客户名称', 
  `statement_day` int COMMENT '账单日', 
  `statemen_date` string COMMENT '账单日期', 
  `customer_bill_date` date COMMENT '客户对账日期', 
  `diff_bill_days` int COMMENT '客户对账日期距离账单日天数', 
  `invoice_date` date COMMENT '开票日期', 
  `diff_invoice_days` int COMMENT '开票日期距离账单日天数', 
  `sales_user_number` string COMMENT '销售员工工号', 
  `sales_user_name` string COMMENT '销售员工姓名', 
  `rp_service_user_work_no` string COMMENT '日配管家工工号', 
  `rp_service_user_name` string COMMENT '日配管家名称', 
  `fl_service_user_work_no` string COMMENT '福利管家工号', 
  `fl_service_user_name` string COMMENT '福利管家名称', 
  `bbc_service_user_work_no` string COMMENT 'bbc管家工号', 
  `bbc_service_user_name` string COMMENT 'bbc管家名称', 
  `tc_bill_type` string COMMENT '提成对账缓发标识', 
  `tc_bill_type_no_send` string COMMENT '提成对账扣发标识', 
  `tc_invoice_type` string COMMENT '提成开票缓发标识', 
  `tc_history_invoice_type_no_send` string COMMENT '提成开票扣发标识', 
  `tc_history_bill_reissue` string COMMENT '提成历史对账补发标识', 
  `tc_history_invoice_reissue` string COMMENT '提成历史开票补发标识', 
  `last_tc_invoice_type` string COMMENT '上月开票缓发标识', 
  `last_tc_bill_type` string COMMENT '上月对账缓发标识', 
  `last_diff_bill_days` int COMMENT '上月对账天数', 
  `last_diff_invoice_days` int COMMENT '上月开票天数', 
  `update_time` string COMMENT '数据更新时间', 
  `s_month` string COMMENT '数据月份')
COMMENT '对账开票缓发补发表'
PARTITIONED BY ( 
  `smt` string COMMENT '分区，每月底执行{"FORMAT":"yyyymm"}')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://yhbigdata/yhcsx/warehouse/csx_analyse/csx_analyse_hr_customer_bill_account_delay'
TBLPROPERTIES (
  'TRANSLATED_TO_EXTERNAL'='TRUE', 
  'bucketing_version'='2', 
  'external.table.purge'='TRUE', 
  'transient_lastDdlTime'='1749798902')


-- 开票日期，销售单号与开票单号关联
-- drop table  csx_analyse_tmp.csx_analyse_tmp_bill_settle;
create table csx_analyse_tmp.csx_analyse_tmp_bill_settle as 
with tmp_bill_settle_detail_di as 
(select source_bill_no,
    bill_code,
    customer_code,
    customer_name,
    company_code,
    happen_date,
    reconciliation_period,
    bill_start_date,
    bill_end_date,
    case when project_end_date='' then  date_add(bill_end_date,1)  
        else concat(substr(add_months(happen_date ,1) ,1,8),if(reconciliation_period <10,concat('0',reconciliation_period),reconciliation_period))
        end statement_date,
    project_end_date,
    project_begin_date,
    substr(regexp_replace(to_date(happen_date),'-',''),1,6) happen_month
 from   csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di 
  where sdt = '20250625'
  -- 上一周期对账
  --  and to_date(happen_date ) >='2025-04-01' and to_date(happen_date)<='2025-05-31'
--   and customer_code='254559'

  
 ),
 -- 客户对账单
tmp_sss_customer_statement_account_di as 
(select bill_code,
        customer_code,
        company_code,
        invoice_time,
        to_date(sale_bill_date) sale_bill_date,
        customer_bill_date,
        to_date(confirm_time) confirm_date
 from  csx_dwd.csx_dwd_sss_customer_statement_account_di 
 where    sdt<='20250625'
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
      -- sdt >= '20250301'
      and sdt<='20250625'
    group by
      order_code,
      source_bill_no
  ) b on a.order_code = b.order_code
where
  -- sdt >= '20250301'
  and sdt<='20250625'
--   and source_bill_no='OM25050100002822'
  group by company_code,
  customer_code,
  to_date(invoice_time),
  source_bill_no
 ),
 tmp_bill_invoice_detail as (
  select happen_month,
    a.source_bill_no,
    a.bill_code,
    a.customer_code,
    a.customer_name,
    a.company_code,
    a.happen_date,
    a.reconciliation_period,
    a.bill_start_date,
    a.bill_end_date,
    b.confirm_date,
    c.invoice_date,
    project_end_date,
    project_begin_date,
    sale_bill_date,
    customer_bill_date,
    statement_date
from tmp_bill_settle_detail_di a 
  left join tmp_sss_customer_statement_account_di b on a.bill_code=b.bill_code and a.company_code=b.company_code
  left join tmp_sss_invoice_detail c on a.source_bill_no=c.source_bill_no
--  where (b.confirm_date<='2025-05-25' or c.invoice_date<='2025-05-25')
--  and bill_end_date>='2025-01-01'
--  and a.bill_code='SDZ250210000482'
 )
select * from tmp_bill_invoice_detail 
-- where statement_date>='2025-05-01'
where confirm_date>='2025-06-01' or invoice_date>='2025-06-01'
or confirm_date is null or invoice_date is null


;

select distinct statement_date from csx_analyse_tmp.csx_analyse_tmp_bill_settle a

-- 
-- drop table  csx_analyse_tmp.csx_analyse_tmp_customer_bill_result_01
create table csx_analyse_tmp.csx_analyse_tmp_customer_bill_result_01 as 
-- 结果表
with tmp_csx_analyse_tmp_bill_settle as 
(select  
    customer_code,
     reconciliation_period,
    statement_date,
    max(max_confirm_date) max_confirm_date,
    max(max_invoice_date) max_invoice_date,
    if(max(max_confirm_date)='' , '',datediff(max(max_confirm_date),statement_date)) diff_bill_days,
    if(max(max_invoice_date)='' , '',datediff(max(max_invoice_date),statement_date)) diff_invoice_days
from
(
 select bill_code,
    customer_code,
     reconciliation_period,
    statement_date,
    happen_month,
    concat_ws('-',bill_start_date,bill_end_date) as bill_date_section,
    max(coalesce(confirm_date,'')) max_confirm_date,
    max(coalesce(invoice_date,'')) max_invoice_date
from  csx_analyse_tmp.csx_analyse_tmp_bill_settle
 where  ((confirm_date<='2025-06-25' or  invoice_date <='2025-06-25' ) or confirm_date ='' or invoice_date='') 
and happen_month>='202504'
group by bill_code,
    customer_code,
     reconciliation_period,
    statement_date,
    happen_month,
    concat_ws('-',bill_start_date,bill_end_date)
) a 

group by  customer_code,
     reconciliation_period,
    statement_date
) ,
  tmp_csx_dim_crm_customer_business_ownership as (
  select
    customer_no customer_code,
    customer_name,
    work_no_new,
    sales_name_new,
    a.region_code  as performance_region_code,
    a.region_name  as performance_region_name,
    a.province_code  as performance_province_code,
    a.province_name  as performance_province_name,
    a.city_group_code  as performance_city_code,
    a.city_group_name  as performance_city_name ,
    rp_service_user_work_no_new as rp_service_user_work_no,
    rp_service_user_name_new as rp_service_user_name,
    fl_service_user_work_no_new as fl_service_user_work_no,
    fl_service_user_name_new as fl_service_user_name,
    bbc_service_user_work_no_new as bbc_service_user_work_no,
    bbc_service_user_name_new as bbc_service_user_name
  from
       csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df a
  where
    sdt = regexp_replace(last_day(add_months('${yesterday_date}',-1)),'-','')
)
select
    a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name ,
    a.customer_code,
    a.customer_name,
    a.work_no_new,
    a.sales_name_new,
    a.rp_service_user_work_no,
    a.rp_service_user_name,
    a.fl_service_user_work_no,
    a.fl_service_user_name,
    a.bbc_service_user_work_no,
    a.bbc_service_user_name,
    b.reconciliation_period,
    b.statement_date,
    b.max_confirm_date,
    b.max_invoice_date,
    b.diff_bill_days,
    b.diff_invoice_days,
    b.last_statement_date,
    if(tc_history_bill_type='是' or tc_reissue_bill_type='是',b.last_max_confirm_date,'') as last_max_confirm_date,
    if(tc_reissue_invoice_type='是' or tc_history_invoice_type='是',b.last_max_invoice_date,'') as last_max_invoice_date,
    if(tc_history_bill_type='是' or tc_reissue_bill_type='是',b.last_diff_bill_days,'') as last_diff_bill_days,
    if(tc_reissue_invoice_type='是' or tc_history_invoice_type='是' ,b.last_diff_invoice_days,'') as last_diff_invoice_days,
    tc_bill_type,  -- 对账缓发
    
    tc_history_bill_type,  -- 对账扣发
    tc_invoice_type,
    tc_history_invoice_type,
    tc_reissue_bill_type,  -- 对账扣发
    tc_reissue_invoice_type
from tmp_csx_dim_crm_customer_business_ownership a 
join 
(
select a.customer_code,
    -- a.customer_name,
    a.reconciliation_period,
    a.statement_date,
    a.max_confirm_date,
    a.max_invoice_date,
    a.diff_bill_days,
    a.diff_invoice_days,
    b.statement_date  last_statement_date,
    b.max_confirm_date last_max_confirm_date,
    b.max_invoice_date last_max_invoice_date,
    b.diff_bill_days last_diff_bill_days,
    b.diff_invoice_days last_diff_invoice_days,
    if(coalesce(a.diff_bill_days,0)>20 and a.max_confirm_date !='' ,'是','否') as  tc_bill_type,  -- 对账缓发
    if(coalesce(a.diff_invoice_days,0)>25 and  a.max_invoice_date!='','是','否') tc_invoice_type,
    if( coalesce(b.diff_bill_days,0)>50 and b.max_confirm_date!='' ,'是','否') as  tc_history_bill_type,  -- 对账扣发
    if( coalesce(b.diff_invoice_days,0)>55 and b.max_invoice_date!=''  ,'是','否') tc_history_invoice_type,
    if( coalesce(b.diff_bill_days,0)<=50  and c.tc_bill_type='是' ,'是','否') as  tc_reissue_bill_type,  -- 对账扣发
    if( coalesce(b.diff_invoice_days,0) <=55 and c.tc_invoice_type='是' ,'是','否') tc_reissue_invoice_type,
    c.tc_bill_type last_tc_bill_type,
    c.tc_invoice_type last_tc_invoice_type
    
from 
(select customer_code,
     reconciliation_period,
    statement_date,
    max_confirm_date,
    max_invoice_date,
    diff_bill_days,
    diff_invoice_days
from tmp_csx_analyse_tmp_bill_settle
where statement_date>='2025-05-26'
 group by customer_code,
    reconciliation_period,
    statement_date,
    max_confirm_date,
    max_invoice_date,
    diff_bill_days,
    diff_invoice_days
)a 
left join 
(select customer_code,
    -- customer_name,
    reconciliation_period,
    statement_date,
    max_confirm_date,
    max_invoice_date,
    diff_bill_days,
    diff_invoice_days
from tmp_csx_analyse_tmp_bill_settle
where statement_date<'2025-05-26'
group by customer_code,
    reconciliation_period,
    statement_date,
    max_confirm_date,
    max_invoice_date,
    diff_bill_days,
    diff_invoice_days
)b on a.customer_code=b.customer_code
left join 
(select customer_code,
     max_confirm_date,
    max_invoice_date,
    diff_bill_days,
    diff_invoice_days,
    tc_bill_type,
    tc_invoice_type
from csx_analyse_tmp.csx_analyse_tmp_customer_bill_result   
group by customer_code,
     max_confirm_date,
    max_invoice_date,
    diff_bill_days,
    diff_invoice_days,
    tc_bill_type,
    tc_invoice_type
-- where statement_date<'2025-05-26'
) c on a.customer_code=c.customer_code
)b on a.customer_code=b.customer_code
-- where a.customer_code='130078'
--  and customer_code='130078'

-- select * from  csx_analyse_tmp.csx_analyse_tmp_customer_bill_result

;


-- 
-- 开票日期，销售单号与开票单号关联
-- drop table  csx_analyse_tmp.csx_analyse_tmp_bill_settle_last;
create table csx_analyse_tmp.csx_analyse_tmp_bill_settle_last as 
with tmp_bill_settle_detail_di as 
(select source_bill_no,
    bill_code,
    customer_code,
    customer_name,
    company_code,
    happen_date,
    reconciliation_period,
    bill_start_date,
    bill_end_date,
    case when project_end_date='' then  date_add(bill_end_date,1)  
        else concat(substr(add_months(happen_date ,1) ,1,8),if(reconciliation_period <10,concat('0',reconciliation_period),reconciliation_period))
        end statement_date,
    project_end_date,
    project_begin_date,
    substr(regexp_replace(to_date(happen_date),'-',''),1,6) happen_month
 from   csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di 
  where sdt = '20250625'
  -- 上一周期对账
   and to_date(happen_date ) >='2025-04-01' and to_date(happen_date)<='2025-04-30'
--   and customer_code='254559'

  
 ),
 -- 客户对账单
tmp_sss_customer_statement_account_di as 
(select bill_code,
        customer_code,
        company_code,
        invoice_time,
        to_date(sale_bill_date) sale_bill_date,
        customer_bill_date,
        to_date(confirm_time) confirm_date
 from  csx_dwd.csx_dwd_sss_customer_statement_account_di 
 where  sdt>='20250301'
    and sdt<='20250525'
    -- and to_date(happen_date_start)>='2025-04-01' and 	to_date(happen_date_end)<='2025-04-30'
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
      sdt >= '20250301'
      and sdt<='20250525'
    group by
      order_code,
      source_bill_no
  ) b on a.order_code = b.order_code
where
  sdt >= '20250301'
  and sdt<='20250525'
--   and source_bill_no='OM25050100002822'
  group by company_code,
  customer_code,
  to_date(invoice_time),
  source_bill_no
 ),
 tmp_bill_invoice_detail as (
  select happen_month,
    a.source_bill_no,
    a.bill_code,
    a.customer_code,
    a.customer_name,
    a.company_code,
    a.happen_date,
    a.reconciliation_period,
    a.bill_start_date,
    a.bill_end_date,
    b.confirm_date,
    c.invoice_date,
    project_end_date,
    project_begin_date,
    sale_bill_date,
    customer_bill_date,
    statement_date
from tmp_bill_settle_detail_di a 
  left join tmp_sss_customer_statement_account_di b on a.bill_code=b.bill_code and a.company_code=b.company_code
  left join tmp_sss_invoice_detail c on a.source_bill_no=c.source_bill_no
--  where (b.confirm_date<='2025-05-25' or c.invoice_date<='2025-05-25')
--  and bill_end_date>='2025-01-01'
--  and a.bill_code='SDZ250210000482'
 )
select * from tmp_bill_invoice_detail 
where statement_date>='2025-05-01'

;

-- drop table  csx_analyse_tmp.csx_analyse_tmp_customer_bill_result
create table csx_analyse_tmp.csx_analyse_tmp_customer_bill_result as 
-- 结果表
with tmp_csx_analyse_tmp_bill_settle as 
(select  
    customer_code,
    customer_name,
    reconciliation_period,
    statement_date,
    max(max_confirm_date) max_confirm_date,
    max(max_invoice_date) max_invoice_date,
    if(max(max_confirm_date)='' , '',datediff(max(max_confirm_date),statement_date)) diff_bill_days,
    if(max(max_invoice_date)='' , '',datediff(max(max_invoice_date),statement_date)) diff_invoice_days
from
(
 select bill_code,
    customer_code,
    regexp_replace(customer_name,'\t|\n|\s','') customer_name,
    reconciliation_period,
    statement_date,
    happen_month,
    concat_ws('-',bill_start_date,bill_end_date) as bill_date_section,
    max(coalesce(confirm_date,'')) max_confirm_date,
    max(coalesce(invoice_date,'')) max_invoice_date
from  csx_analyse_tmp.csx_analyse_tmp_bill_settle_last
where happen_month='202504'
    and ((confirm_date<='2025-05-25' or  invoice_date <='2025-05-25' ) or confirm_date ='' or invoice_date='')
-- where customer_code='101543'
group by bill_code,
    customer_code,
    regexp_replace(customer_name,'\t|\n|\s',''),
    reconciliation_period,
    statement_date,
    happen_month,
    concat_ws('-',bill_start_date,bill_end_date)
) a 

group by  customer_code,
    customer_name,
    reconciliation_period,
    statement_date
) ,
  tmp_csx_dim_crm_customer_business_ownership as (
  select
    customer_no customer_code,
    customer_name,
    work_no_new,
    sales_name_new,
    a.region_code  as performance_region_code,
    a.region_name  as performance_region_name,
    a.province_code  as performance_province_code,
    a.province_name  as performance_province_name,
    a.city_group_code  as performance_city_code,
    a.city_group_name  as performance_city_name ,
    rp_service_user_work_no_new as rp_service_user_work_no,
    rp_service_user_name_new as rp_service_user_name,
    fl_service_user_work_no_new as fl_service_user_work_no,
    fl_service_user_name_new as fl_service_user_name,
    bbc_service_user_work_no_new as bbc_service_user_work_no,
    bbc_service_user_name_new as bbc_service_user_name
  from
       csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df a
  where
    sdt = regexp_replace(last_day(add_months('${yesterday_date}',-1)),'-','')
)
select
    a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name ,
    a.customer_code,
    a.customer_name,
    a.work_no_new,
    a.sales_name_new,
    a.rp_service_user_work_no,
    a.rp_service_user_name,
    a.fl_service_user_work_no,
    a.fl_service_user_name,
    a.bbc_service_user_work_no,
    a.bbc_service_user_name,
    b.reconciliation_period,
    b.statement_date,
    b.max_confirm_date,
    b.max_invoice_date,
    b.diff_bill_days,
    b.diff_invoice_days,
    -- b.last_statement_date,
    -- b.last_max_confirm_date,
    -- b.last_max_invoice_date,
    -- b.last_diff_bill_days,
    -- b.last_diff_invoice_days,
    tc_bill_type,  -- 对账缓发
    tc_invoice_type
    -- tc_history_bill_type,  -- 对账扣发
    -- tc_history_invoice_type,
    -- tc_reissue_bill_type,  -- 对账扣发
    -- tc_reissue_invoice_type
from tmp_csx_dim_crm_customer_business_ownership a 
join 
(
select a.customer_code,
    a.customer_name,
    a.reconciliation_period,
    a.statement_date,
    a.max_confirm_date,
    a.max_invoice_date,
    a.diff_bill_days,
    a.diff_invoice_days,
    -- b.statement_date  last_statement_date,
    -- b.max_confirm_date last_max_confirm_date,
    -- b.max_invoice_date last_max_invoice_date,
    -- b.diff_bill_days last_diff_bill_days,
    -- b.diff_invoice_days last_diff_invoice_days,
    if(a.diff_bill_days>20 and a.max_confirm_date !='' ,'是','否') as  tc_bill_type,  -- 对账缓发
    if(a.diff_invoice_days>25 and  a.max_invoice_date!='','是','否') tc_invoice_type
    -- if(b.diff_bill_days>50  ,'是','否') as  tc_history_bill_type,  -- 对账扣发
    -- if(b.diff_invoice_days>55  ,'是','否') tc_history_invoice_type,
    -- if(b.diff_bill_days<=50  ,'是','否') as  tc_reissue_bill_type,  -- 对账扣发
    -- if(b.diff_invoice_days<=55  ,'是','否') tc_reissue_invoice_type
from 
(select customer_code,
    customer_name,
    reconciliation_period,
    statement_date,
    max_confirm_date,
    max_invoice_date,
    diff_bill_days,
    diff_invoice_days
from tmp_csx_analyse_tmp_bill_settle
-- where statement_date>='2025-06-01'
)a 
)b
-- left join 
-- (select customer_code,
--     customer_name,
--     reconciliation_period,
--     statement_date,
--     max_confirm_date,
--     max_invoice_date,
--     diff_bill_days,
--     diff_invoice_days
-- from tmp_csx_analyse_tmp_bill_settle
-- where statement_date<'2025-06-01'
-- )b
on a.customer_code=b.customer_code
-- )b on a.customer_code=b.customer_code


select * from  csx_analyse_tmp.csx_analyse_tmp_customer_bill_result   
;



-- 开票日期，销售单号与开票单号关联
-- drop table  csx_analyse_tmp.csx_analyse_tmp_bill_settle;
create table csx_analyse_tmp.csx_analyse_tmp_bill_settle as 
with tmp_bill_settle_detail_di as 
(select source_bill_no,
    bill_code,
    customer_code,
    customer_name,
    company_code,
    happen_date,
    reconciliation_period,
    bill_start_date,
    bill_end_date,
    case when project_end_date='' then  date_add(bill_end_date,1)  
        else concat(substr(add_months(happen_date ,1) ,1,8),if(reconciliation_period <10,concat('0',reconciliation_period),reconciliation_period))
        end statement_date,
    project_end_date,
    project_begin_date,
    substr(regexp_replace(to_date(happen_date),'-',''),1,6) happen_month
 from   csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di 
  where sdt = '20250625'
  -- 上一周期对账
  --  and to_date(happen_date ) >='2025-04-01' and to_date(happen_date)<='2025-05-31'
--   and customer_code='254559'

  
 ),
 -- 客户对账单
tmp_sss_customer_statement_account_di as 
(select bill_code,
        customer_code,
        company_code,
        invoice_time,
        to_date(sale_bill_date) sale_bill_date,
        customer_bill_date,
        to_date(confirm_time) confirm_date
 from  csx_dwd.csx_dwd_sss_customer_statement_account_di 
 where    sdt<='20250625'
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
      -- sdt >= '20250301'
       sdt<='20250625'
    group by
      order_code,
      source_bill_no
  ) b on a.order_code = b.order_code
where
  -- sdt >= '20250301'
   sdt<='20250625'
--   and source_bill_no='OM25050100002822'
  group by company_code,
  customer_code,
  to_date(invoice_time),
  source_bill_no
 ),
 tmp_bill_invoice_detail as (
  select happen_month,
    a.source_bill_no,
    a.bill_code,
    a.customer_code,
    a.customer_name,
    a.company_code,
    a.happen_date,
    a.reconciliation_period,
    a.bill_start_date,
    a.bill_end_date,
    b.confirm_date,
    c.invoice_date,
    project_end_date,
    project_begin_date,
    sale_bill_date,
    customer_bill_date,
    statement_date
from tmp_bill_settle_detail_di a 
  left join tmp_sss_customer_statement_account_di b on a.bill_code=b.bill_code and a.company_code=b.company_code
  left join tmp_sss_invoice_detail c on a.source_bill_no=c.source_bill_no
--  where (b.confirm_date<='2025-05-25' or c.invoice_date<='2025-05-25')
--  and bill_end_date>='2025-01-01'
--  and a.bill_code='SDZ250210000482'
 )
select * from tmp_bill_invoice_detail 
-- where statement_date>='2025-05-01'
where confirm_date>='2025-06-01' or invoice_date>='2025-06-01'
or confirm_date is null or invoice_date is null
;

with tmp_csx_analyse_tmp_bill_settle as 
(
select *,row_number()over(partition by customer_code order by cast(diff_bill_days as int) desc ) diff_bill_days_ro,
row_number()over(partition by customer_code order by cast(diff_invoice_days as int) desc ) diff_invoice_days_ro
from (select  
    customer_code,
    reconciliation_period,
    statement_date,
    max(max_confirm_date) max_confirm_date,
    max(max_invoice_date) max_invoice_date,
    if(max(max_confirm_date)='' , 0,datediff(max(max_confirm_date),statement_date)) diff_bill_days,
    if(max(max_invoice_date)='' , 0,datediff(max(max_invoice_date),statement_date)) diff_invoice_days
from
(
 select bill_code,
    customer_code,
     reconciliation_period,
    statement_date,
    happen_month,
    concat_ws('-',bill_start_date,bill_end_date) as bill_date_section,
    max(coalesce(confirm_date,'')) max_confirm_date,
    max(coalesce(invoice_date,'')) max_invoice_date
from  csx_analyse_tmp.csx_analyse_tmp_bill_settle
 where  ((confirm_date<='2025-06-25' and confirm_date>='2025-06-01' ) or  (invoice_date <='2025-06-25' and invoice_date>='2025-06-01' ) or confirm_date ='' or invoice_date='') 
-- and happen_month>='202504'
group by bill_code,
    customer_code,
     reconciliation_period,
    statement_date,
    happen_month,
    concat_ws('-',bill_start_date,bill_end_date)
) a 

group by  customer_code,
     reconciliation_period,
    statement_date
)a 
)
,
  tmp_csx_dim_crm_customer_business_ownership as (
  select
    customer_no customer_code,
    customer_name,
    work_no_new,
    sales_name_new,
    a.region_code  as performance_region_code,
    a.region_name  as performance_region_name,
    a.province_code  as performance_province_code,
    a.province_name  as performance_province_name,
    a.city_group_code  as performance_city_code,
    a.city_group_name  as performance_city_name ,
    rp_service_user_work_no_new as rp_service_user_work_no,
    rp_service_user_name_new as rp_service_user_name,
    fl_service_user_work_no_new as fl_service_user_work_no,
    fl_service_user_name_new as fl_service_user_name,
    bbc_service_user_work_no_new as bbc_service_user_work_no,
    bbc_service_user_name_new as bbc_service_user_name
  from
       csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df a
  where
    sdt = regexp_replace(last_day(add_months('2025-06-25',-1)),'-','')
)
select  
    a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name ,
    a.customer_code,
    a.customer_name,
    a.work_no_new,
    a.sales_name_new,
    a.rp_service_user_work_no,
    a.rp_service_user_name,
    a.fl_service_user_work_no,
    a.fl_service_user_name,
    a.bbc_service_user_work_no,
    a.bbc_service_user_name,
    coalesce(b.reconciliation_period,c.reconciliation_period) reconciliation_period ,
    b.statement_date,
    b.max_confirm_date,
    b.diff_bill_days,
    c.statement_date as invoice_statement_date,
    c.max_invoice_date as max_invoice_date,
    c.diff_invoice_days ,
    b.diff_bill_days_ro,
    c.diff_invoice_days_ro,
    if(coalesce(b.diff_bill_days,0)>20  ,'是','否') as  tc_bill_type,  -- 对账缓发
    if(coalesce(b.diff_bill_days,0)>50  ,'是','否') as  tc_history_bill_type,  -- 对账扣发
    if(coalesce(c.diff_invoice_days,0)>25 ,'是','否') tc_invoice_type,
    if(coalesce(c.diff_invoice_days,0)>55   ,'是','否') tc_history_invoice_type
    -- if( coalesce(b.diff_bill_days,0)<=50  and c.tc_bill_type='是' ,'是','否') as  tc_reissue_bill_type,  -- 对账扣发
    -- if( coalesce(b.diff_invoice_days,0) <=55 and c.tc_invoice_type='是' ,'是','否') tc_reissue_invoice_type,
from  tmp_csx_dim_crm_customer_business_ownership a 
left  join  
(
select * from tmp_csx_analyse_tmp_bill_settle where diff_bill_days_ro=1) b on a.customer_code=b.customer_code
left join 
(
select * from tmp_csx_analyse_tmp_bill_settle where diff_invoice_days_ro=1) c on a.customer_code=c.customer_code
 
where coalesce(b.reconciliation_period,c.reconciliation_period) is not null 
;

  
with tmp_csx_analyse_tmp_bill_settle as 
(
 select bill_code,
    customer_code,
    reconciliation_period,
    statement_date,
    happen_month,
    concat_ws('-',bill_start_date,bill_end_date) as bill_date_section,
    max(coalesce(confirm_date,'')) max_confirm_date,
    max(coalesce(invoice_date,'')) max_invoice_date,
    if(max(confirm_date)='' , 0,datediff(max(confirm_date),statement_date)) diff_bill_days,
    if(max(invoice_date)='' , 0,datediff(max(invoice_date),statement_date)) diff_invoice_days
from  csx_analyse_tmp.csx_analyse_tmp_bill_settle
 where  ((confirm_date<='2025-06-25' and confirm_date>='2025-06-01' ) or  (invoice_date <='2025-06-25' and invoice_date>='2025-06-01' ) or confirm_date ='' or invoice_date='') 
-- and happen_month>='202504'
group by bill_code,
    customer_code,
     reconciliation_period,
    statement_date,
    happen_month,
    concat_ws('-',bill_start_date,bill_end_date)
 
)
,
  tmp_csx_dim_crm_customer_business_ownership as (
  select
    customer_no customer_code,
    customer_name,
    work_no_new,
    sales_name_new,
    a.region_code  as performance_region_code,
    a.region_name  as performance_region_name,
    a.province_code  as performance_province_code,
    a.province_name  as performance_province_name,
    a.city_group_code  as performance_city_code,
    a.city_group_name  as performance_city_name ,
    rp_service_user_work_no_new as rp_service_user_work_no,
    rp_service_user_name_new as rp_service_user_name,
    fl_service_user_work_no_new as fl_service_user_work_no,
    fl_service_user_name_new as fl_service_user_name,
    bbc_service_user_work_no_new as bbc_service_user_work_no,
    bbc_service_user_name_new as bbc_service_user_name
  from
       csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df a
  where
    sdt = regexp_replace(last_day(add_months('2025-06-25',-1)),'-','')
)
select  
    a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name ,
    a.customer_code,
    a.customer_name,
    a.work_no_new,
    a.sales_name_new,
    a.rp_service_user_work_no,
    a.rp_service_user_name,
    a.fl_service_user_work_no,
    a.fl_service_user_name,
    a.bbc_service_user_work_no,
    a.bbc_service_user_name,
    b.bill_code,
    b.reconciliation_period,
    b.statement_date,
    b.happen_month,
    b.bill_date_section,
    b.max_confirm_date,
    b.max_invoice_date,
    b.diff_bill_days,
    b.diff_invoice_days
from  tmp_csx_dim_crm_customer_business_ownership a 
 join  
(
select * from tmp_csx_analyse_tmp_bill_settle  ) b on a.customer_code=b.customer_code
