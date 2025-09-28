-- 绩效提成补发缓发

取数逻辑：
对账：单个客户超出账单日20天-50（含）天未对账，当月总提成缓发50%，超50天以上总提成扣发50%；
开票：单个客户超出账单日25天-55（含）天未开票，当月提成缓发50%，55天以上提成扣发50%
补发条件：
对账在50天内完成，次月补发提成；
开票在55天内完成，次月补发提成；
考核对象：有挂客户的服务管家，若该客户无服务管家，考核对应销售

表结构 
create table csx_analyse.csx_analyse_hr_customer_bill_account_delay (
performance_region_code	string	comment '	大区编码',
performance_region_name	string	comment '	大区名称',
performance_province_code	string	comment '	省区编码',
performance_province_name	string	comment '	省区名称',
performance_city_code	string	comment '	城市编码',
performance_city_name	string	comment '	城市名称',
customer_code	string	comment '	客户编码',
customer_name	string	comment '	客户名称',
statement_day	int	comment '	账单日',
statemen_date	string	comment '	账单日期',
customer_bill_date	date	comment '	客户对账日期',
diff_bill_days	int	comment '	客户对账日期距离账单日天数',
invoice_date	date	comment '	开票日期',
diff_invoice_days	int	comment '	开票日期距离账单日天数',
sales_user_number	string	comment '销售员工工号',
sales_user_name	string	comment '销售员工姓名',
rp_service_user_work_no	string	comment '日配管家工工号',
rp_service_user_name	string	comment '	日配管家名称',
fl_service_user_work_no	string	comment '福利管家工号',
fl_service_user_name	string	comment '福利管家名称',
bbc_service_user_work_no	string	comment 'BBC管家工号',
bbc_service_user_name	string	comment 'bbc管家名称',
tc_bill_type	string	comment '提成对账缓发标识',
tc_bill_type_no_send	string	comment '提成对账扣发标识',
tc_invoice_type	string	comment '提成开票缓发标识',
tc_history_invoice_type_no_send	string	comment '提成开票扣发标识',
tc_history_bill_reissue	string	comment '提成历史对账补发标识',
tc_history_invoice_reissue	string	comment '提成历史开票补发标识',
last_tc_invoice_type	string	comment '上月开票缓发标识',
last_tc_bill_type	string	comment '上月对账缓发标识',
last_diff_bill_days	int	comment '上月对账天数',
last_diff_invoice_days	int	comment '上月开票天数',
update_time	string	comment '数据更新时间',
s_month  string	comment '数据月份'
)  comment '对账开票缓发补发表'
partitioned by (smt string comment '分区，每月底执行')
stored as parquet;


create table data_analysis_prd.report_csx_analyse_hr_customer_bill_account_delay (
id BIGINT AUTO_INCREMENT PRIMARY KEY,
performance_region_code	varchar(128)	comment '	大区编码',
performance_region_name	varchar(128)	comment '	大区名称',
performance_province_code	varchar(128)	comment '	省区编码',
performance_province_name	varchar(128)	comment '	省区名称',
performance_city_code	varchar(128)	comment '	城市编码',
performance_city_name	varchar(128)	comment '	城市名称',
customer_code	varchar(128)	comment '	客户编码',
customer_name	varchar(128)	comment '	客户名称',
statement_day	int	comment '	账单日',
statemen_date	varchar(128)	comment '	账单日期',
customer_bill_date	date	comment '	客户对账日期',
diff_bill_days	int	comment '	客户对账日期距离账单日天数',
invoice_date	date	comment '	开票日期',
diff_invoice_days	int	comment '	开票日期距离账单日天数',
sales_user_number	varchar(128)	comment '销售员工工号',
sales_user_name	varchar(128)	comment '销售员工姓名',
rp_service_user_work_no	varchar(128)	comment '日配管家工工号',
rp_service_user_name	varchar(128)	comment '	日配管家名称',
fl_service_user_work_no	varchar(128)	comment '福利管家工号',
fl_service_user_name	varchar(128)	comment '福利管家名称',
bbc_service_user_work_no	varchar(128)	comment 'BBC管家工号',
bbc_service_user_name	varchar(128)	comment 'bbc管家名称',
tc_bill_type	varchar(128)	comment '提成对账缓发标识',
tc_bill_type_no_send	varchar(128)	comment '提成对账扣发标识',
tc_invoice_type	varchar(128)	comment '提成开票缓发标识',
tc_history_invoice_type_no_send	varchar(128)	comment '提成开票扣发标识',
tc_history_bill_reissue	varchar(128)	comment '提成历史对账补发标识',
tc_history_invoice_reissue	varchar(128)	comment '提成历史开票补发标识',
last_tc_invoice_type	varchar(128)	comment '上月开票缓发标识',
last_tc_bill_type	varchar(128)	comment '上月对账缓发标识',
last_diff_bill_days	int	comment '上月对账天数',
last_diff_invoice_days	int	comment '上月开票天数',
update_time	varchar(128)	comment '数据更新时间',
s_month  varchar(128)	comment '数据月份',
KEY idx_smt (s_month),
KEY idx_performance_province (s_month,performance_province_name)
)  comment='对账开票缓发补发表'

-- 第三版版 20250613
 
-- drop table  csx_analyse_tmp.csx_analyse_tmp_bill_account_delay;
create table csx_analyse_tmp.csx_analyse_tmp_bill_account_delay_01 as 
 with tmp_sss_invoice as 
(select 
        customer_code,
        to_date(max(invoice_time))  max_invoice_date
from  csx_dwd.csx_dwd_sss_invoice_di  
where sdt<='${yesterday}'
    and delete_flag='0'
group by 
        customer_code
)
,
tmp_csx_dwd_sss_customer_statement_account_di as 
(select
  customer_code,
  to_date(max(customer_bill_date)) customer_bill_date
from
  csx_dwd.csx_dwd_sss_customer_statement_account_di
where
  sdt <= '${yesterday}'
  and to_date(customer_bill_date) <=to_date('${yesterday_date}')
--   and customer_code = '167954'
  and delete_flag = 0
  group by customer_code
  )
  ,
 tmp_csx_dim_sss_customer_statement_config as 
(
select a.customer_code,
    customer_name,
	dev_source_code,
	dev_source_name,
    performance_region_code   ,
    performance_region_name   ,
    performance_province_code ,
    performance_province_name ,
    performance_city_code     ,
    performance_city_name     ,
    sales_user_number         ,
    sales_user_name           ,
    statement_day,
    if(cast (statement_day as string )<=day(to_date('${yesterday_date}')),  
        if(statement_day<10,CONCAT_WS('0',substr(to_date('${yesterday_date}'),1,8),cast(statement_day as string )),CONCAT(substr(to_date('${yesterday_date}'),1,8),cast (statement_day as string ))),
        CONCAT(substr(add_months(to_date('${yesterday_date}'),-1),1,8),cast (statement_day as string ))) as statemen_date
 from (
    select 
        cast(customer_code as string) as customer_code,
    	max(statement_day)statement_day 
      from    csx_dim.csx_dim_sss_customer_statement_config
      where sdt = 'current' 
        -- and table_type like 'DETAIL_%'
        and shipper_code='YHCSX'
      group by cast(customer_code as string) 
      
     )a 
     left join 
     (
  select 
    customer_code,
    customer_name,
	dev_source_code,
	dev_source_name,
    performance_region_code   ,
    performance_region_name   ,
    performance_province_code ,
    performance_province_name ,
    performance_city_code     ,
    performance_city_name     ,
    sales_user_number         ,
    sales_user_name           
  from csx_dim.csx_dim_crm_customer_info
  where sdt = 'current'
  and shipper_code='YHCSX'
  ) b on a.customer_code=b.customer_code
  ),
tmp_last_bill_invoice_delay as 
(select
  customer_code,
  statement_day,
  statemen_date,
  customer_bill_date,
  diff_days,
  max_invoice_date,
  diff_invoice_days,
  tc_bill_type,
  tc_invoice_type
from
  csx_analyse_tmp.csx_analyse_tmp_bill_account_delay
 ),
tmp_csx_dim_crm_customer_business_ownership as (
  select
    customer_no customer_code,
    customer_name,
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
    sdt = '20250612'
)
select 
    coalesce(a.performance_region_code,b.performance_region_code) performance_region_code,
    coalesce(a.performance_region_name,b.performance_region_name) performance_region_name,
    coalesce(a.performance_province_code,b.performance_province_code) performance_province_code,
    coalesce(a.performance_province_name,b.performance_province_name) performance_province_name,
    coalesce(a.performance_city_code,b.performance_city_code) performance_city_code,
    coalesce(a.performance_city_name,b.performance_city_name)performance_city_name,
    a.customer_code,
    coalesce(a.customer_name,b.customer_name) customer_name,
    b.statement_day,
    b.statemen_date,
    d.customer_bill_date,
    datediff(d.customer_bill_date,b.statemen_date)+1 as diff_days,
    c.max_invoice_date,           -- 开票日期
    datediff(c.max_invoice_date,b.statemen_date)+1 as diff_invoice_days,
    b.sales_user_number,
    b.sales_user_name,
    a.rp_service_user_work_no,
    a.rp_service_user_name,
    a.fl_service_user_work_no,
    a.fl_service_user_name,
    a.bbc_service_user_work_no,
    a.bbc_service_user_name,
   if( datediff(d.customer_bill_date,b.statemen_date)+1 >'20' and datediff(d.customer_bill_date,b.statemen_date)+1 <='50'  ,'是','否') tc_bill_type,  -- 对账缓发
   if( h.tc_bill_type='是' and  datediff(d.customer_bill_date,h.statemen_date)+1 >'50' ,"是","否") tc_bill_type_no_send,    -- 对账扣发
   case
        -- 当开票日期为空时
        when c.max_invoice_date ='' and datediff(d.customer_bill_date,b.statemen_date)+1 >'25' and datediff(d.customer_bill_date,b.statemen_date)+1 <='55' then '是'
        when  datediff(c.max_invoice_date,b.statemen_date)+1 >'25' and   datediff(c.max_invoice_date,b.statemen_date)+1  <='55'   then '是'
        else "否" end  tc_invoice_type,
   if(h.tc_invoice_type='是' and  datediff(to_date(c.max_invoice_date),h.statemen_date)+1 >'55' ,"是","否") tc_history_invoice_type_no_send,
   if( h.tc_bill_type='是' and datediff(d.customer_bill_date,  h.statemen_date )+1  <'50'  ,"是","否") tc_history_bill_reissue,
   if( h.tc_invoice_type='是' and  datediff(c.max_invoice_date,h.statemen_date )+1  <'55'  ,"是","否") tc_history_invoice_reissue,
   h.tc_invoice_type as last_tc_invoice_type,
   h.tc_bill_type as last_tc_bill_type,
   h.diff_days as last_diff_days,
   h.diff_invoice_days last_diff_invoice_days,
   substr('${yesterday}',1,6) smt 
from tmp_csx_dim_crm_customer_business_ownership  a 
left join 
tmp_csx_dim_sss_customer_statement_config b on a.customer_code=b.customer_code 
left  join tmp_sss_invoice c on a.customer_code=c.customer_code 
left  join tmp_csx_dwd_sss_customer_statement_account_di d  on a.customer_code=d.customer_code 
left  join tmp_last_bill_invoice_delay h on a.customer_code=h.customer_code
 where  b.statement_day is not null 
 ;



-- 第二版 20250612

with tmp_sss_invoice as 
(select 
        customer_code,
        to_date(max(invoice_time))  max_invoice_date,
        to_date(max(if(sdt<='20250425',invoice_time,''))) last_max_invoice_date
from  csx_dwd.csx_dwd_sss_invoice_di  
where sdt<='20250525'
    and delete_flag='0'
group by 
        customer_code
)
,
tmp_csx_dwd_sss_customer_statement_account_di as 
(select
  customer_code,
  to_date(max(customer_bill_date)) customer_bill_date,
  to_date(max(if(sdt<='20250425',customer_bill_date,''))) last_max_bill_date
from
  csx_dwd.csx_dwd_sss_customer_statement_account_di
where
  sdt <= '20250525'
--   and customer_code = '167954'
  and delete_flag = 0
  group by customer_code
  )
  ,
 tmp_csx_dim_sss_customer_statement_config as 
(
select a.customer_code,
    customer_name,
	dev_source_code,
	dev_source_name,
    performance_region_code   ,
    performance_region_name   ,
    performance_province_code ,
    performance_province_name ,
    performance_city_code     ,
    performance_city_name     ,
    sales_user_number         ,
    sales_user_name           ,
    statement_day,
    if(cast (statement_day as string )<=day(to_date('2025-05-25')),  
        if(statement_day<10,CONCAT_WS('0',substr(to_date('2025-05-25'),1,8),cast(statement_day as string )),CONCAT(substr(to_date('2025-05-25'),1,8),cast (statement_day as string ))),
        CONCAT(substr(add_months(to_date('2025-05-25'),-1),1,8),cast (statement_day as string ))) as statemen_date
 from (
    select 
        cast(customer_code as string) as customer_code,
    	max(statement_day)statement_day 
      from    csx_dim.csx_dim_sss_customer_statement_config
      where sdt = 'current' 
        -- and table_type like 'DETAIL_%'
        and shipper_code='YHCSX'
      group by cast(customer_code as string) 
      
     )a 
     left join 
     (
  select 
    customer_code,
    customer_name,
	dev_source_code,
	dev_source_name,
    performance_region_code   ,
    performance_region_name   ,
    performance_province_code ,
    performance_province_name ,
    performance_city_code     ,
    performance_city_name     ,
    sales_user_number         ,
    sales_user_name           
  from csx_dim.csx_dim_crm_customer_info
  where sdt = 'current'
  and shipper_code='YHCSX'
  ) b on a.customer_code=b.customer_code
  ),
tmp_csx_dim_crm_customer_business_ownership as (
  select
    customer_no customer_code,
    customer_name,
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
    sdt = '20250609'
)
select 
    coalesce(a.performance_region_code,b.performance_region_code) performance_region_code,
    coalesce(a.performance_region_name,b.performance_region_name) performance_region_name,
    coalesce(a.performance_province_code,b.performance_province_code) performance_province_code,
    coalesce(a.performance_province_name,b.performance_province_name) performance_province_name,
    coalesce(a.performance_city_code,b.performance_city_code) performance_city_code,
    coalesce(a.performance_city_name,b.performance_city_name)performance_city_name,
    a.customer_code,
    coalesce(a.customer_name,b.customer_name) customer_name,
    b.statement_day,
    b.statemen_date,
    customer_bill_date,
    datediff(d.customer_bill_date,b.statemen_date)+1 as diff_days,
    c.max_invoice_date,           -- 开票日期
    datediff(c.max_invoice_date,b.statemen_date)+1 as diff_invoice_days,
    b.sales_user_number,
    b.sales_user_name,
    a.rp_service_user_work_no,
    a.rp_service_user_name,
    a.fl_service_user_work_no,
    a.fl_service_user_name,
    a.bbc_service_user_work_no,
    a.bbc_service_user_name,
   if( datediff(customer_bill_date,statemen_date)+1 >'20' and datediff(customer_bill_date,statemen_date)+1 <='50'  ,'是','否') tc_bill_type,
   if( datediff(customer_bill_date,statemen_date)+1 >'50' ,"是","否") tc_bill_type_no_send,
   case
        -- 当开票日期为空时
        when max_invoice_date ='' and datediff(customer_bill_date,statemen_date)+1 >'25' and datediff(customer_bill_date,statemen_date)+1 <='55' then '是'
         when  datediff(max_invoice_date,statemen_date)+1 >'25' and   datediff(max_invoice_date,statemen_date)+1  <='55'   then '是'
        else "否" end  tc_invoice_type,
   if( datediff(to_date('2025-05-25'),add_months(statemen_date,-1))+1 >'55' ,"是","否") tc_history_invoice_type_no_send,
   
   if( datediff(last_max_bill_date,  add_months(statemen_date ,-1))+1  <'50'  ,"是","否") tc_history_bill_reissue,
   if( datediff(last_max_invoice_date,add_months(statemen_date ,-1))+1  <'55'  ,"是","否") tc_history_invoice_reissue
from tmp_csx_dim_crm_customer_business_ownership  a 
left join 
tmp_csx_dim_sss_customer_statement_config b on a.customer_code=b.customer_code 
left  join tmp_sss_invoice c on a.customer_code=c.customer_code 
left join  tmp_csx_dwd_sss_customer_statement_account_di d  on a.customer_code=d.customer_code 
-- and a.customer_attribute_name=b.business_attribute_name
 where  b.statement_day is not null 
 ;



-- 第一版

with tmp_bill_settle_stat as (
  select
    region_code,
    region_name,
    province_code,
    province_name,
    city_code city_group_code,
    city_name city_group_name,
    sale_code as sales_employee_code,
    sale_name as sales_employee_name,
    customer_no customer_code,
    customer_name,
    statement_day,
    if(cast (statement_day as string )<=day(to_date('2025-05-25')),  
        if(statement_day<10,CONCAT_WS('0',substr(to_date('2025-05-25'),1,8),cast(statement_day as string )),CONCAT(substr(to_date('2025-05-25'),1,8),cast (statement_day as string ))),
        CONCAT(substr(add_months(to_date('2025-05-25'),-1),1,8),cast (statement_day as string ))) as statemen_date,
    business_attribute_desc customer_attribute_name,
    -- company_code,
    sum(unstatement_amount) unbill_amt_all,
    sum(unstatement_amount_history) bill_amt_history,
    sum(no_invoice_amt) no_invoice_amt_all,
    sum(no_invoice_amt_history) no_invoice_amt_history
  from
    csx_report.csx_report_sss_statement_kp_process_detail_credit_df
  where
    sdt = '20250609'
    group by region_code,
    region_name,
    province_code,
    province_name,
    city_code,
    city_name,
    sale_code,
    sale_name,
    customer_no,
    customer_name,
    business_attribute_desc,
    statement_day,
    if(cast (statement_day as string )<=day(to_date('2025-05-25')),  
        if(statement_day<10,CONCAT_WS('0',substr(to_date('2025-05-25'),1,8),cast(statement_day as string )),CONCAT(substr(to_date('2025-05-25'),1,8),cast (statement_day as string ))),
        CONCAT(substr(add_months(to_date('2025-05-25'),-1),1,8),cast (statement_day as string ))) 
),
tmp_csx_dim_crm_customer_business_ownership as (
  select
    customer_no customer_code,
    customer_name,
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
    sdt = '20250609'
)
select 
    coalesce(b.performance_region_code,a.region_code) performance_region_code,
    coalesce(b.performance_region_name,a.region_name) performance_region_name,
    coalesce(b.performance_province_code,a.province_code) performance_province_code,
    coalesce(b.performance_province_name,a.province_name) performance_province_name,
    coalesce(b.performance_city_code,a.city_group_code) performance_city_code,
    coalesce(b.performance_city_name,a.city_group_name)performance_city_name,
    a.customer_attribute_name,
    a.customer_code,
    coalesce(b.customer_name,a.customer_name) customer_name,
    statement_day,
    statemen_date,
    datediff(to_date('2025-05-25'),statemen_date)+1 as diff_date,
    a.sales_employee_code,
    a.sales_employee_name,
    rp_service_user_work_no,
    rp_service_user_name,
    fl_service_user_work_no,
    fl_service_user_name,
    bbc_service_user_work_no,
    bbc_service_user_name,
    a.unbill_amt_all,
    a.bill_amt_history,
    a.no_invoice_amt_all,
    a.no_invoice_amt_history,
   if( datediff(to_date('2025-05-25'),statemen_date)+1 >='20' and  datediff(to_date('2025-05-25'),statemen_date)+1 <='50' and unbill_amt_all>0,"是","否") tc_bill_type,
   if( datediff(to_date('2025-05-25'),statemen_date)+1 >='25' and  datediff(to_date('2025-05-25'),statemen_date)+1 <='55' and no_invoice_amt_all>0,"是","否") tc_invoice_type,
   if( datediff(to_date('2025-05-25'),add_months(statemen_date,-1))+1 >'50'  and bill_amt_history>0,"是","否") tc_history_bill_type,
   if( datediff(to_date('2025-05-25'),add_months(statemen_date,-1))+1 >'55'  and no_invoice_amt_history>0,"是","否") tc_history_invoice_type
   
from tmp_bill_settle_stat a 
  join 
tmp_csx_dim_crm_customer_business_ownership b on a.customer_code=b.customer_code 
-- and a.customer_attribute_name=b.business_attribute_name
where (a.unbill_amt_all>0 
        or a.bill_amt_history>0 
        or a.no_invoice_amt_all>0 
        or a.no_invoice_amt_history>0)
--  where  b.customer_code is not null 