-- 客户对账开票信息缓发表-20250910
-- drop table  csx_analyse_tmp.csx_analyse_tmp_bill_settle_01;
create table csx_analyse_tmp.csx_analyse_tmp_bill_settle_01 as 
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
    sum(order_amt)over(partition by bill_code ) as order_total_amt,
    sum(invoice_amount)over(partition by bill_code) as invoice_total_amt,
    overdue_date
  from       csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di a
  LEFT JOIN temp_company_credit b on a.customer_code=b.customer_code and a.company_code=b.company_code and a.credit_code=b.credit_code
  where sdt = '20250922' 
  and happen_date>='2025-01-01'
  and bill_end_date<='2025-09-22'
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
      where ((confirm_date between '2025-08-26' and '2025-09-22' or confirm_date is null ) 
        or (invoice_date between '2025-08-26' and '2025-09-22' or invoice_date is null ))
    --  AND (bill_amt>=0 OR bill_amt IS NULL )
    --  and (diff_confirm_days>=20 or diff_invoice_days>=25)

;

-- desc  csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di 
 -- 客户对账开票信息缓发表明细
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
  -- and fl_service_user_work_no_new<>''
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
  -- and bbc_service_user_work_no_new<>''
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
 tmp_csx_analyse_tmp_bill_settle as  (
select source_bill_no	,
bill_code	,
customer_code	,
credit_code	,
company_code	,
business_attribute_code	,
business_attribute_name	,
source_sys	,
happen_date	,
account_period_name	,
account_period_value	,
reconciliation_period	,
project_begin_date	,
project_end_date	,
bill_start_date	,
bill_end_date	,
statement_date	,
sub_customer_code	,
order_amt	,
statement_amount	,
kp_amount	,
order_total_amt	,
invoice_total_amt	,
overdue_date	,
bill_amt	,
confirm_date	,
confirm_status	,
credit_pay_amt	,
diff_confirm_days	,
invoice_date	,
diff_invoice_days	,
-- row_confirm	,
-- row_invoice	,
concat_ws('-',bill_start_date,bill_end_date) as bill_date_section,
row_number()over(partition by customer_code ,credit_code,company_code,business_attribute_name order by diff_confirm_days desc ) row_confirm,
row_number()over(partition by customer_code ,credit_code,company_code,business_attribute_name order by diff_invoice_days desc ) row_invoice
from csx_analyse_tmp.csx_analyse_tmp_bill_settle_01
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
    b.source_bill_no,
    b.reconciliation_period,
    b.statement_date,
    b.happen_date,
    b.bill_date_section,
    b.order_amt	,
    b.statement_amount	,
    b.kp_amount	,
    b.confirm_date,
    b.diff_confirm_days,
    b.invoice_date,
    b.diff_invoice_days,
    row_confirm,
    row_invoice
 from tmp_sale_info a 
 left join tmp_csx_analyse_tmp_bill_settle b  
 on a.customer_code=b.customer_code 
 and a.business_attribute_name=b.business_attribute_name 
 where bill_date_section is not null
  and (diff_confirm_days>=20 or diff_invoice_days>=25)

--  and (row_confirm=1 or row_invoice=1)

;
-- desc  csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di 
 -- 结果表
-- desc  csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di 
 
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
  -- and fl_service_user_work_no_new<>''
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
  -- and bbc_service_user_work_no_new<>''
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
  sdt =regexp_replace('2025-09-22', '-', '')
  )a 
  left join tmp_csx_dim_crm_customer_business_ownership b on a.customer_code=b.customer_code
 )
 ,
 tmp_csx_analyse_tmp_bill_settle as  (
select source_bill_no	,
bill_code	,
customer_code	,
credit_code	,
company_code	,
business_attribute_code	,
business_attribute_name	,
source_sys	,
happen_date	,
account_period_name	,
account_period_value	,
reconciliation_period	,
project_begin_date	,
project_end_date	,
bill_start_date	,
bill_end_date	,
statement_date	,
sub_customer_code	,
order_amt	,
statement_amount	,
kp_amount	,
order_total_amt	,
invoice_total_amt	,
overdue_date	,
bill_amt	,
confirm_date	,
confirm_status	,
credit_pay_amt	,
diff_confirm_days	,
invoice_date	,
diff_invoice_days	,
-- row_confirm	,
-- row_invoice	,
concat_ws('-',bill_start_date,bill_end_date) as bill_date_section,
row_number()over(partition by customer_code ,credit_code,company_code,business_attribute_name order by diff_confirm_days desc ) row_confirm,
row_number()over(partition by customer_code ,credit_code,company_code,business_attribute_name order by diff_invoice_days desc ) row_invoice
from csx_analyse_tmp.csx_analyse_tmp_bill_settle_01
)
,
tmp_csx_analyse_tmp_bill_settle_ruslt as 
(
select *,
    row_number()over(partition by customer_code,business_attribute_name order by cast(diff_confirm_days as int) desc ) diff_confirm_days_ro,
    row_number()over(partition by customer_code,business_attribute_name order by cast(diff_invoice_days as int) desc ) diff_invoice_days_ro,
    row_number()over(partition by customer_code,business_attribute_name ) rn 
from (
select 
    b.performance_region_code,
    b.performance_region_name,
    b.performance_province_code,
    b.performance_province_name,
    b.performance_city_code,
    b.performance_city_name ,
    b.customer_code,
    b.customer_name,
    b.business_attribute_name,
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
    row_invoice
 from tmp_csx_analyse_tmp_bill_settle a 
 left join tmp_sale_info b  on a.customer_code=b.customer_code and a.business_attribute_name=b.business_attribute_name 
 where bill_date_section is not null
--   and (diff_confirm_days>=20 or diff_invoice_days>=25)
 )a 
 )
 ,
 tmp_ruslt_01 as (
select  
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
    coalesce(a.reconciliation_period,c.reconciliation_period) reconciliation_period ,
    a.statement_date,
    -- a.bill_amt,
    b.confirm_date,
    b.diff_confirm_days,
    d.bill_amt_all ,
    d.bill_amt,
    d.unbill_amt,
    if(d.unbill_amt=0,'是','否') as confirm_flag,
    c.statement_date as invoice_statement_date,
    c.invoice_date as max_invoice_date,
    c.diff_invoice_days ,
    d.invoice_amount_all ,
    d.invoice_amount as kp_amount,
    d.no_invoice_amt_all,
    if(no_invoice_amt_all=0,'是','否') kp_flag,
    case when d.unbill_amt=0 and no_invoice_amt_all=0 then '否' else   if(coalesce(b.diff_confirm_days,0)>20 and coalesce(b.diff_confirm_days,0)<=50 ,'是','否') end  as  tc_bill_type,  -- 对账缓发
    case when d.unbill_amt=0 and no_invoice_amt_all=0 then '否' else  if(coalesce(b.diff_confirm_days,0)>50  ,'是','否') end as  tc_history_bill_type,  -- 对账扣发
    case when d.unbill_amt=0 and no_invoice_amt_all=0 then '否' else  if(coalesce(c.diff_invoice_days,0)>25 and coalesce(c.diff_invoice_days,0)<=55 ,'是','否') end  tc_invoice_type,
    case when d.unbill_amt=0 and no_invoice_amt_all=0 then '否' else  if(coalesce(c.diff_invoice_days,0)>55   ,'是','否') end  tc_history_invoice_type
    -- if( coalesce(b.diff_confirm_days,0)<=50  and c.tc_bill_type='是' ,'是','否') as  tc_reissue_bill_type,  -- 对账扣发
    -- if( coalesce(b.diff_invoice_days,0) <=55 and c.tc_invoice_type='是' ,'是','否') tc_reissue_invoice_type,
from  
(select * from tmp_csx_analyse_tmp_bill_settle_ruslt where rn=1) a
left join 
(select * from tmp_csx_analyse_tmp_bill_settle_ruslt where diff_confirm_days_ro=1) b on a.customer_code=b.customer_code and a.business_attribute_name=b.business_attribute_name
left join 
(
select * from tmp_csx_analyse_tmp_bill_settle_ruslt where diff_invoice_days_ro=1) c on a.customer_code=c.customer_code and a.business_attribute_name =c.business_attribute_name
-- where a.customer_code='124059'
left join 
(select customer_code,
    credit_business_attribute_name,
    sum(bill_amt+unbill_amt+unbill_amount_history) as bill_amt_all,
    sum(bill_amt) bill_amt,
    sum(unbill_amt+unbill_amount_history) unbill_amt, 
    sum(invoice_amount+no_invoice_amt_history+no_invoice_amt ) invoice_amount_all,
    sum(invoice_amount) invoice_amount,
    sum(no_invoice_amt_history+no_invoice_amt ) no_invoice_amt_all
from  
         csx_dws.csx_dws_sss_customer_credit_invoice_bill_settle_stat_di
     where sdt=regexp_replace('2025-09-22', '-', '')
     group by customer_code,
    credit_business_attribute_name
) d on a.customer_code=d.customer_code and a.business_attribute_name=d.credit_business_attribute_name
where  coalesce(a.reconciliation_period,c.reconciliation_period) is not null 
   and    (unbill_amt<1000 or no_invoice_amt_all<1000)  -- 过滤账金额小于1000
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
    reconciliation_period ,
    statement_date,
    -- a.bill_amt,
    confirm_date,
    diff_confirm_days,
    bill_amt_all ,
    bill_amt,
    unbill_amt,
    confirm_flag,
    invoice_statement_date,
    max_invoice_date,
    diff_invoice_days ,
    invoice_amount_all ,
    kp_amount,
    no_invoice_amt_all,
    kp_flag,
    tc_bill_type,  -- 对账缓发
    tc_history_bill_type,  -- 对账扣发
    tc_invoice_type,
    tc_history_invoice_type,
    case when tc_history_bill_type='是' or tc_history_invoice_type='是' then '否' when tc_bill_type='是' or tc_invoice_type='是' then '是' else '否' end final_outcome_type,
    if(tc_history_bill_type='是' or tc_history_invoice_type='是','是','否') final_result_withheld_type
 from tmp_ruslt_01 a ;



243348 233646 252028 252038 265077 249799 258144 259930 

蛋占比3.7%，毛利率-29.5%；省区品类毛利率-4.17%; sku数6个，核心影响sku数1个
水产占比14.9%，毛利率-0.6%；省区品类毛利率-3.94%; sku数58个，核心影响sku数11个


鱿鱼须（冰鲜）(销售（0.22W、毛利率-18.71%、影响-10.6%)
鱿鱼须（冰鲜）(销售（0.22W、毛利率-18.71%、影响-10.6%)
鮰鱼(杀净)(杀前2-3斤/条)(销售（0.16W、毛利率-19.9%、影响-8.64%)
鲜鸡蛋粉壳(销售（0.36W、毛利率-35%、影响-38.01%)
家禽占比11.6%，毛利率1.8%；省区品类毛利率21.2%; sku数39个，核心影响sku数6个

冷鲜光鸭(销售（0.09W、毛利率-2.69%、影响-1.93%)
冷鲜白条鸡(净膛)(销售（0.48W、毛利率1%、影响-1.71%)

家禽占比19.4%，毛利率38.8%；sku数11个，核心影响sku数2个
熟食烘焙占比3.7%，毛利率25.9%；sku数4个，核心影响sku数1个


广式烧鸭(销售0.08W、毛利率8.29%、影响-6.48%)
有叶酸菜（散）(销售0.05W、毛利率-44.74%、影响-1.83%)
老豆腐(销售0.07W、毛利率-11.32%、影响-0.94%)

鲜鸡蛋粉壳(销售0.56W、毛利率-27.38%、影响-5.72%)

蔬菜占比13.3%，毛利率-1.2%；省区品类毛利率3.74%; sku数160个，核心影响sku数7个
家禽占比7.7%，毛利率23.1%；省区品类毛利率18.07%; sku数27个，核心影响sku数4个
牛羊占比8.8%，毛利率4.2%；省区品类毛利率-1.54%; sku数16个，核心影响sku数2个

六角丝瓜B(销售0.27W、毛利率-68.62%、影响-1.81%)
菠菜B(销售0.15W、毛利率-102.18%、影响-1.27%)
菜心(销售0.22W、毛利率-42.36%、影响-0.71%)
上海青(销售0.23W、毛利率-42.2%、影响-0.7%)
奶白菜B(销售0.26W、毛利率-28.13%、影响-0.64%)
西红柿(销售0.38W、毛利率-20.38%、影响-0.63%)
西兰花 B(销售0.13W、毛利率-58.24%、影响-0.58%)

冻鸡翅中(40-50g)(销售1.09W、毛利率-9.38%、影响-2.48%)
冻琵琶腿(150g+)(销售0.55W、毛利率18.48%、影响-0.78%)
冻凤爪(50g+)(销售0.5W、毛利率27.67%、影响-0.48%)

热鲜牛后腿肉(销售5.13W、毛利率7.47%、影响-2.84%)
热鲜牛展(销售0.36W、毛利率0.51%、影响-1.65%)
热鲜牛排腩条(销售0.35W、毛利率-21.99%、影响-0.49%)

蔬菜占比20.9%，毛利率7.2%；省区品类毛利率3.74%; sku数123个，核心影响sku数10个
水产占比13.9%，毛利率4.5%；省区品类毛利率6.44%; sku数49个，核心影响sku数5个
猪肉占比5%，毛利率17.6%；省区品类毛利率8.13%; sku数22个，核心影响sku数4个


蒜米手工(销售0.35W、毛利率9.26%、影响-2.9%)
八角丝瓜(销售0.1W、毛利率-72.93%、影响-2.04%)
红椒B(销售0.34W、毛利率-2.66%、影响-1.87%)
金银甜玉米(约350g)/根(销售0.25W、毛利率9.64%、影响-0.97%)
[彩食鲜]菜心(销售0.14W、毛利率-15.98%、影响-0.75%)
土豆(销售0.23W、毛利率-2.86%、影响-0.74%)
铁棍山药B(销售0.13W、毛利率-17.22%、影响-0.67%)


淡水鲈鱼(活)(350-450g/条)(销售0.19W、毛利率9.08%、影响-2.95%)
鱿鱼须(冰鲜)(销售0.58W、毛利率-21.85%、影响-1.97%)
海白虾(活)(22-28头)(销售1.49W、毛利率1.96%、影响-1.86%)
小膏蟹(活)(80-125g/只)(销售0.09W、毛利率-35.75%、影响-0.97%)
章鱼(冰鲜)(100-150g/条)(销售0.12W、毛利率-12.73%、影响-0.55%)


鱿鱼(冰鲜)(定制)(0.5-1斤/条)(销售0.23W、毛利率-33.63%、影响-8.02%)
整条带鱼(冰鲜)(1-1.3斤/条)(销售0.13W、毛利率-22.64%、影响-3.31%)
淡水鲈鱼(活)(350-450g/条)(销售0.26W、毛利率-6.51%、影响-2.54%)


热鲜猪耳（带根）(销售0.15W、毛利率-76.72%、影响-8.34%)
热鲜通排(精品)(销售2.59W、毛利率-6.35%、影响-6.11%)
热鲜猪舌(销售0.09W、毛利率-29.73%、影响-1.97%)


热鲜牛后腿肉(销售3.18W、毛利率-23.31%、影响-8.27%)
鲜牛腩(销售1.51W、毛利率-16.87%、影响-0.06%)
热鲜羊肉(销售0.11W、毛利率-3.95%、影响-1.14%)

预制菜占比3%，毛利率-27.2%；省区品类毛利率-27.23%; sku数63个，核心影响sku数4个
调味品类占比4.2%，毛利率-4.3%；省区品类毛利率-4.33%; sku数199个，核心影响sku数2个
水产占比26.4%，毛利率-3.2%；省区品类毛利率-3.16%; sku数187个，核心影响sku数6个
干货占比4.8%，毛利率-9.5%；省区品类毛利率-9.5%; sku数190个，核心影响sku数5个


彩食鲜大鱿鱼（冰）(销售0.47W、毛利率-27.85%、影响-2.08%)
花蛤(活)(40-50个/斤)(销售0.12W、毛利率-6.24%、影响-1.95%)
马面鱼(鲜杀)(杀前1.5-1.8斤/条)(销售0.13W、毛利率-6.94%、影响-0.1%)

冻猪蹄切块(1切10)(销售0.17W、毛利率-12.54%、影响-1.53%)
热鲜精肋排切块(长约2cm)(销售0.1W、毛利率-21.2%、影响-1.23%)
热鲜猪肉馅(前腿)(销售0.09W、毛利率-14.43%、影响-0.97%)
热鲜五花肉(二级)(销售0.35W、毛利率7.14%、影响-0.74%)

细云耳（一级）(销售0.06W、毛利率-75.02%、影响-4.34%)
皇上皇五花腊肉400g(销售0.07W、毛利率-64.16%、影响-2.38%)

