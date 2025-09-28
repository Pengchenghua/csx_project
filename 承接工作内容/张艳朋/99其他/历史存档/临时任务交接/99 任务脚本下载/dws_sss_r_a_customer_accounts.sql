SET hive.execution.engine=tez;
SET tez.queue.name=caishixian;

-- 动态分区
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- 中间结果压缩
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.type=BLOCK;
SET parquet.compression=SNAPPY;

-- 启用引号识别
set hive.support.quoted.identifiers=none;

-- 最新日期
set last_day=regexp_replace(date_sub(current_date, 1), '-', '');

-- 昨天标准格式
set yesterday = date_sub(current_date, 1);

-- 当前年第一天
set start_year=regexp_replace(trunc(date_sub(current_date, 1), 'YYYY'), '-', '');

-- 新系统上线日期
set new_system='20200901';

-- 更新客户应收账款
set target_customer_account=csx_dw.dws_sss_r_a_customer_accounts;

-- 期初应收账款
set source_begin_receive_account=csx_dw.dwd_sss_r_a_beginning_receivable_20201116;

-- 当前应收账款
set source_current_receive_account=csx_dw.dwd_sss_r_d_sale_order_statement_detail_20201116;

-- 回款表
set source_money_back=csx_dw.dwd_sss_r_d_money_back;

with accounts_union as
(
  select 
    trim(customer_code) as customer_no,
    trim(company_code) as company_code,
    if((datediff(${hiveconf:yesterday}, overdue_date) + 1) >= 1, datediff(${hiveconf:yesterday}, overdue_date) + 1, 0) as overdue_days,
    if((datediff(${hiveconf:yesterday}, overdue_date) + 1) >= 1, unpaid_amount, 0) as overdue_amount,
    unpaid_amount,
    bad_debt_amount,
    account_period_code,
    account_period_name,
    account_period_val,
    -- 账期天数“月结”类型计算处理 计算因子，用于计算逾期系数
    if(account_period_code like 'Y%', if(account_period_val = 31, 45, account_period_val + 15), account_period_val) as acc_val_calculation_factor
  from ${hiveconf:source_begin_receive_account} 
  where sdt = ${hiveconf:last_day} and to_date(create_time) < current_date()
  union all 
  select 
    trim(customer_code) as customer_no,
    trim(company_code) as company_code,
    if((datediff(${hiveconf:yesterday}, overdue_date) + 1) >= 1, datediff(${hiveconf:yesterday}, overdue_date) + 1, 0) as overdue_days,
    if((datediff(${hiveconf:yesterday}, overdue_date) + 1) >= 1, unpaid_amount, 0) as overdue_amount,
    unpaid_amount,
    bad_debt_amount,
    account_period_code,
    account_period_name,
    account_period_val,
    -- 账期天数“月结”类型计算处理 计算因子，用于计算逾期系数 财务默认
    if(account_period_code like 'Y%', if(account_period_val = 31, 45, account_period_val + 15), account_period_val) as acc_val_calculation_factor
  from ${hiveconf:source_current_receive_account} 
  where sdt = ${hiveconf:last_day} and to_date(happen_date) < current_date()
),
customer_company as 
(
   select 
      *
   from csx_dw.dws_crm_w_a_customer_company
   where sdt = 'current'
),
customer_accounts as 
(
  select
    customer_no,
    company_code,
    sum(overdue_amount) as overdue_amount,
    sum(if(overdue_days >= 1 and overdue_days <= 15, overdue_amount, 0)) as overdue_amount1,  
    sum(if(overdue_days > 15 and overdue_days <= 30, overdue_amount, 0)) as overdue_amount15,
    sum(if(overdue_days > 30 and overdue_days <= 60, overdue_amount, 0)) as overdue_amount30,
    sum(if(overdue_days > 60 and overdue_days <= 90, overdue_amount, 0)) as overdue_amount60,
    sum(if(overdue_days > 90 and overdue_days <= 120, overdue_amount, 0)) as overdue_amount90,
    sum(if(overdue_days > 120 and overdue_days <= 180, overdue_amount, 0)) as overdue_amount120,
    sum(if(overdue_days > 180 and overdue_days <= 365, overdue_amount, 0)) as overdue_amount180,
    sum(if(overdue_days > 365 and overdue_days <= 730, overdue_amount, 0)) as overdue_amount365,
    sum(if(overdue_days > 730 and overdue_days <= 1095, overdue_amount, 0)) as overdue_amount730,
    sum(if(overdue_days > 1095, overdue_amount, 0)) as overdue_amount1095,
    sum(if(overdue_days = 0, unpaid_amount, 0)) as non_overdue_amount,
    sum(unpaid_amount) as receivable_amount,
    sum(bad_debt_amount) as bad_debt_amount,
    max(overdue_days) as  max_overdue_day,
     -- 逾期金额*逾期天数 计算因子，用于计算逾期系数分子
    sum( if(overdue_days >= 1, unpaid_amount * overdue_days, 0) ) as overdue_coefficient_numerator,
    -- 应收金额*账期天数 计算因子，用于计算逾期系数分母
    sum( if(unpaid_amount >= 0, unpaid_amount, 0) * if(account_period_val = 0, 1, acc_val_calculation_factor) ) as overdue_coefficient_denominator
  from accounts_union 
  group by customer_no, company_code
),
year_paid as 
(
   -- 获取年至今客户回款金额
  select
    customer_code, -- 客户编码
    company_code, -- 公司代码
    sum(paid_amount) as paid_amount
  from  ${hiveconf:source_money_back} -- sdt以过账日期分区，只看20200601及以后的，该表计算回款只计算已核销金额
  where case when ${hiveconf:new_system} >= ${hiveconf:start_year} then sdt >= ${hiveconf:new_system} else sdt >= ${hiveconf:start_year} end -- 获取今年第一天
    or (sdt = '19990101' and posting_time >= '2020-09-01')
  group by customer_code, company_code
) 
insert overwrite table ${hiveconf:target_customer_account} partition(sdt)
select 
  concat_ws('&', t1.customer_no, t1.company_code) as id,
  t1.customer_no,
  t2.customer_name,
  t2.channel_code,
  t2.channel_name,
  t2.attribute_code,
  t2.attribute_name,
  t2.sales_id,
  t2.work_no,
  t2.sales_name,
  t4.province_code,
  t4.province_name,
  t4.city_group_code as city_code,
  t4.city_group_name as city_name,
  t1.company_code,
  t2.company_name,
  t2.payment_terms,
  t2.payment_name,
  t2.payment_days,
  t2.customer_level,
  t2.credit_limit,
  t2.temp_credit_limit,
  t2.temp_begin_time,
  t2.temp_end_time,
  t1.overdue_amount,
  t1.overdue_amount1,
  t1.overdue_amount15,
  t1.overdue_amount30,
  t1.overdue_amount60,
  t1.overdue_amount90,
  t1.overdue_amount120,
  t1.overdue_amount180,
  t1.overdue_amount365,
  t1.overdue_amount730,
  t1.overdue_amount1095,
  t1.non_overdue_amount,
  t1.receivable_amount,
  t1.bad_debt_amount,
  t1.max_overdue_day,
  t3.paid_amount,
  t1.overdue_coefficient_numerator,
  t1.overdue_coefficient_denominator,
  case when t1.receivable_amount <= 1 then 0.00
    else coalesce(round( if( t1.overdue_coefficient_numerator < 0, 0, t1.overdue_coefficient_numerator ) / t1.overdue_coefficient_denominator, 2), 0.00)
    end as overdue_coefficient, -- 逾期系数
  ${hiveconf:last_day} as sdt
from customer_accounts t1 left outer join customer_company t2 
  on t1.customer_no = t2.customer_no and t1.company_code = t2.company_code
  left outer join year_paid t3 on t1.customer_no = t3.customer_code and t1.company_code = t3.company_code 
 left outer join 
(
  select distinct
    city_code,
    city_name,
    city_group_code,
    city_group_name,
    province_code,
    province_name,
    region_code,
    region_name,
    area_province_code
  from csx_dw.dws_sale_w_a_area_belong 
)t4 on t2.city_code=t4.city_code and t2.province_code=t4.area_province_code;