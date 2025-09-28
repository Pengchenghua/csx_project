取值规则：csx_dws_sss_order_credit_invoice_bill_settle_detail_di 按照bill_code 汇总计算账单金额order_amt  ，过滤账单金额小于等于0的

-- csx_analyse.csx_analyse_hr_customer_bill_account_delay 明细表
-- 客户缓发对账 20250826 增加业务类型
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
    -- 无项目制的，取账单结束日期+1天，否则按照业务日期+账单日
    case when project_end_date='' then  date_add(bill_end_date,1)  
        else concat(substr(add_months(happen_date ,1) ,1,8),if(reconciliation_period <10,concat('0',reconciliation_period),reconciliation_period))
        end statement_date,
    project_end_date,
    project_begin_date,
    substr(regexp_replace(to_date(happen_date),'-',''),1,6) happen_month
 from    csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di 
  where sdt = '${yesterday}'
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
 where    sdt<='${yesterday}'
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
       sdt<='${yesterday}'
    --   and invoice_status_code=2
    --   and sync_status=1   -- 发票更新状态
    --   and cx_invoice_no_code is null 
    group by
      order_code,
      source_bill_no
  ) b on a.order_code = b.order_code

where
  -- sdt >= '20250301'
   sdt<='${yesterday}'
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
 where  ((confirm_date<='${yesterday_date}' and confirm_date>=trunc('${yesterday_date}','MM') ) 
            or  (invoice_date <='${yesterday_date}' and invoice_date>=trunc('${yesterday_date}','MM')  ) 
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
 
  tmp_csx_dim_crm_customer_business_ownership as 
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
    -- a.diff_bill_days_ro,
    -- c.diff_invoice_days_ro,
    if(coalesce(b.diff_bill_days,0)>20  ,'是','否') as  tc_bill_type,  -- 对账缓发
    if(coalesce(b.diff_bill_days,0)>50  ,'是','否') as  tc_history_bill_type,  -- 对账扣发
    if(coalesce(c.diff_invoice_days,0)>25 ,'是','否') tc_invoice_type,
    if(coalesce(c.diff_invoice_days,0)>55   ,'是','否') tc_history_invoice_type
    -- if( coalesce(b.diff_bill_days,0)<=50  and c.tc_bill_type='是' ,'是','否') as  tc_reissue_bill_type,  -- 对账扣发
    -- if( coalesce(b.diff_invoice_days,0) <=55 and c.tc_invoice_type='是' ,'是','否') tc_reissue_invoice_type,
from  
(select * from tmp_csx_analyse_tmp_bill_settle where diff_bill_days_ro=1) a
left join 
(select * from tmp_csx_analyse_tmp_bill_settle where diff_bill_days_ro=1) b on a.customer_code=b.customer_code and a.business_attribute_name=b.business_attribute_name
left join 
(
select * from tmp_csx_analyse_tmp_bill_settle where diff_invoice_days_ro=1) c on a.customer_code=c.customer_code and a.business_attribute_name =c.business_attribute_name
-- where a.customer_code='124059'
where  coalesce(a.reconciliation_period,c.reconciliation_period) is not null 
;


CREATE  TABLE IF NOT EXISTS csx_analyse.csx_analyse_hr_customer_bill_account_detail_mi( 

`performance_region_code` STRING  COMMENT '大区编码',
`performance_region_name` STRING  COMMENT '大区名称',
`performance_province_code` STRING  COMMENT '省区编码',
`performance_province_name` STRING  COMMENT '省区名称',
`performance_city_code` STRING  COMMENT '城市编码',
`performance_city_name` STRING  COMMENT '城市名称',
`customer_code` STRING  COMMENT '客户编码',
`customer_name` STRING  COMMENT '客户名称',
business_attribute_name string COMMENT '业务类型属性',
`sales_user_number` STRING  COMMENT '销售员工工号',
`sales_user_name` STRING  COMMENT '销售员工姓名',
`service_user_work_no` STRING  COMMENT '管家工工号',
`service_user_name` STRING  COMMENT '管家名称',
bill_code STRING  COMMENT '对账单号',
`reconciliation_period` INT  COMMENT '对账周期',
`statemen_date` STRING  COMMENT '账单日期',
happen_month  STRING  COMMENT '发生月份',
bill_date_section  STRING  COMMENT '账单区间',
`customer_confirm_date` DATE  COMMENT '客户对账日期',
`diff_bill_days` INT  COMMENT '客户对账日期距离账单日天数',
`invoice_date` DATE  COMMENT '开票日期',
`diff_invoice_days` INT  COMMENT '开票日期距离账单日天数',
`update_time` STRING  COMMENT '数据更新时间',
`s_month` STRING  COMMENT '数据月份' ) 
 COMMENT '销售/管家对账缓发明细表' 
 PARTITIONED BY
 (
`smt` STRING  COMMENT '分区，每月底执行{"FORMAT":"yyyymm"}' )
 STORED AS PARQUET



 CREATE  TABLE IF NOT EXISTS csx_analyse.csx_analyse_tmp_hr_customer_bill_account_result_mi( 

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
business_attribute_name string COMMENT '业务类型属性',
`reconciliation_period` INT  COMMENT '对账周期',
`statemen_date` STRING  COMMENT '账单日期',
`customer_confirm_date` DATE  COMMENT '客户对账日期',
`diff_bill_days` INT  COMMENT '客户对账日期距离账单日天数',
`invoice_date` DATE  COMMENT '开票日期',
`diff_invoice_days` INT  COMMENT '开票日期距离账单日天数',
`tc_bill_type` STRING  COMMENT '提成对账缓发标识',
`tc_bill_type_no_send` STRING  COMMENT '提成对账扣发标识',
`tc_invoice_type` STRING  COMMENT '提成开票缓发标识',
`tc_history_invoice_type_no_send` STRING  COMMENT '提成开票扣发标识',
`delayed_release_result` STRING  COMMENT '缓发结果',
`suspended_result` STRING  COMMENT '扣发结果',
`update_time` STRING  COMMENT '数据更新时间',
`s_month` STRING  COMMENT '数据月份' ) 
 COMMENT '客户对账缓发结果表' 
 PARTITIONED BY
 (
`smt` STRING  COMMENT '分区，每月底执行{"FORMAT":"yyyymm"}' )
 STORED AS PARQUET



CREATE  TABLE IF NOT EXISTS data_analysis_prd.report_csx_analyse_hr_customer_bill_account_detail_mi
( 
id bigint(20) auto_increment COMMENT '主键',
`performance_region_code` varchar(128)  COMMENT '大区编码',
`performance_region_name` varchar(128)  COMMENT '大区名称',
`performance_province_code` varchar(128)  COMMENT '省区编码',
`performance_province_name` varchar(128)  COMMENT '省区名称',
`performance_city_code` varchar(128)  COMMENT '城市编码',
`performance_city_name` varchar(128)  COMMENT '城市名称',
`customer_code` varchar(128)  COMMENT '客户编码',
`customer_name` varchar(128)  COMMENT '客户名称',
`business_attribute_name` varchar(128)  COMMENT '业务类型属性',
`sales_user_number` varchar(128)  COMMENT '销售员工工号',
`sales_user_name` varchar(128)  COMMENT '销售员工姓名',
`service_user_work_no` varchar(128)  COMMENT '管家工工号',
`service_user_name` varchar(128)  COMMENT '管家名称',
`bill_code` varchar(128)  COMMENT '对账单号',
`reconciliation_period` varchar(128)  COMMENT '对账周期',
`statemen_date` varchar(128)  COMMENT '账单日期',
`happen_month` varchar(128)  COMMENT '发生月份',
`bill_date_section` varchar(128)  COMMENT '账单区间',
`customer_confirm_date` varchar(128)  COMMENT '客户对账日期',
`diff_bill_days` varchar(128)  COMMENT '客户对账日期距离账单日天数',
`invoice_date` varchar(128)  COMMENT '开票日期',
`diff_invoice_days` varchar(128)  COMMENT '开票日期距离账单日天数',
`update_time` varchar(128)  COMMENT '数据更新时间',
`s_month` varchar(128)  COMMENT '数据月份',
primary key (id) ,
index index_name(s_month,performance_region_name,performance_province_name)
) 
 COMMENT ='HR客户对账缓发明细表' 
 
