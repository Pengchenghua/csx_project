
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
set target_customer_account=csx_dw.dws_sss_r_a_customer_company_accounts;

-- 期初应收账款
set source_begin_receive_account=csx_dw.dwd_sss_r_a_beginning_receivable_20201116;

-- 当前应收账款
set source_current_receive_account=csx_dw.dwd_sss_r_d_sale_order_statement_detail_20201116;

-- 回款表
set source_money_back=csx_dw.dwd_sss_r_d_money_back;


drop table csx_tmp.tmp_dws_sss_r_a_customer_company_accounts_0;
create table csx_tmp.tmp_dws_sss_r_a_customer_company_accounts_0
   as 
select 
    source_bill_no,
    source_statement_amount,
    customer_code,
    company_code,
    case when  account_period_code = 'Z007'  and unpaid_amount>residual_amount then unpaid_amount-residual_amount
	when  account_period_code = 'Z007'  and unpaid_amount<residual_amount   then 0
	else unpaid_amount end unpaid_amount,
   unpaid_amount unpaid_amount_old,
    bad_debt_amount,
    account_period_code,
    account_period_name,
    account_period_val,
    happen_date,
    settle_cycle,
    project_end,
    project_begin_old,
	residual_amount,
   case when  account_period_code in ('Z001','Z002','Z003','Z004','Z005','Z006') and ( project_end is null or  project_end='')
             THEN date_add(happen_date, COALESCE( account_period_val, 0))
         when account_period_code like 'Y%' and (project_end is null or project_end='') and (settle_cycle is  null or settle_cycle='')
         --sap没有对账日 取下凭证日期下月一号+预期账期
             then date_add(add_months(trunc(happen_date,'MM'),1) ,int(account_period_val-1)) 
         --如果有对账日期（凭证过账日期所在月+记账周期数）+预期账期
         when  account_period_code like 'Y%' and (project_end is null or project_end='') and (settle_cycle is not null and  settle_cycle<>'')
              then 
                 case when substr(date_format( happen_date,'yyyy-MM-dd'),9,10)<int(settle_cycle) 
                      then  
                           case when last_day(happen_date)<date_add(trunc(happen_date,'MM'),int(settle_cycle-1))
                               then date_add(last_day(happen_date),int(account_period_val-1))
                               else  date_add(date_add(trunc(happen_date,'MM'),int(settle_cycle-1)),int(account_period_val-1)) end 
                      else 
                          date_add(date_add(add_months(trunc(happen_date,'MM'),1),int(settle_cycle-1)),int(account_period_val-1)) end
         when project_end is not null and project_end<>'' then date_add(substr(project_end,1,10),int(account_period_val-1)) 
     ---预付款客户 逾期 按照大于客户的信控额度的上期逾期日期+1天
        when  account_period_code = 'Z007' and unpaid_amount>residual_amount  then date_add(happen_date,1)
		when (project_end is null or project_end='')  then date_format(current_date,'yyyy-MM-dd')
        when (account_period_code is null or account_period_code ='')  
              and  (settle_cycle is  null or settle_cycle='') 
              and ( project_end is null or  project_end='') 
             then add_months(trunc(happen_date,'MM'),1)  end overdue_date_new
from 
(
     select 
          t1.id as source_bill_no,
          t1.beginning_amount as source_statement_amount,
          t1.customer_code,
          t1.company_code,
          t1.unpaid_amount,
          t1.bad_debt_amount,
          t1.account_period_code,
          t1.account_period_name,
          t1.account_period_val,
          t1.happen_date,
          t2.settle_cycle,
          case when date_format(t1.happen_date,'yyyy-MM-dd')>=date_format(t2.project_begin,'yyyy-MM-dd') 
                        and date_format(happen_date,'yyyy-MM-dd')<=date_format(t2.project_end,'yyyy-MM-dd')
                then t2.project_end 
                else null end project_end,
          t2.project_begin as project_begin_old    ,
t3.residual_amount        
      
     from 
    (
      select 
        *,
        date_sub(from_unixtime(unix_timestamp(overdue_date,'yyyy-MM-dd hh:mm:ss')),coalesce(account_period_val,0)) as happen_date	-- 发生时间
      from ${hiveconf:source_begin_receive_account} 
      where sdt = ${hiveconf:last_day} and to_date(create_time) < current_date() 
    )t1 
    left join 
    (
      select 
         customer_number,
         company_code,
         max(settle_cycle) as settle_cycle,
         max(project_end) as project_end,
         max(project_begin) as project_begin,
		 max(credit_limit)+max(case when temp_end_time <=${hiveconf:yesterday}   then  temp_credit_limit end) as credit_limit
      from csx_ods.source_r_a_crm_customer_company  
      where is_deleted=0 and company_status=1 and sdt=regexp_replace(${hiveconf:yesterday}, '-', '')
      group by customer_number,company_code
    )t2 on t1.customer_code=t2.customer_number and t1.company_code=t2.company_code
	 left join (
	 select
    customer_code, -- 客户编码
    company_code, -- 公司代码
    sum(residual_amount) as residual_amount
  from  ${hiveconf:source_money_back} -- sdt以过账日期分区，只看20200601及以后的，该表计算回款只计算已核销金额
  group by customer_code, company_code
  ) t3 on t1.customer_code=t3.customer_code and t1.company_code=t3.company_code 
)h1
union all 
select 
    source_bill_no,
    source_statement_amount,
    customer_code,
    company_code,
   case when  account_period_code = 'Z007'  and unpaid_amounts>residual_amount and  unpaid_amounts-residual_amount<unpaid_amount then unpaid_amounts-residual_amount
	when  account_period_code = 'Z007'  and unpaid_amounts<residual_amount   then 0
   else unpaid_amount end  unpaid_amount,
   unpaid_amount unpaid_amount_old,
    bad_debt_amount,
    account_period_code,
    account_period_name,
    account_period_val,
    happen_date,
    settle_cycle,
    project_end,
    project_begin_old,
	residual_amount,
   case when  account_period_code in ('Z001','Z002','Z003','Z004','Z005','Z006') and ( project_end is null or  project_end='')
             THEN date_add(happen_date, COALESCE( account_period_val, 0))
        when account_period_code like 'Y%' and (project_end is null or project_end='') and (settle_cycle is  null or settle_cycle='')
        --sap没有对账日 取下凭证日期下月一号+预期账期
             then date_add(add_months(trunc(happen_date,'MM'),1) ,int(account_period_val-1))
        --如果有对账日期（凭证过账日期所在月+记账周期数）+预期账期
        when  account_period_code like 'Y%' and (project_end is null or project_end='') and (settle_cycle is not null and  settle_cycle<>'')
             then 
                case when substr(date_format( happen_date,'yyyy-MM-dd'),9,10)<int(settle_cycle) 
                     then  
                          case when last_day(happen_date)<date_add(trunc(happen_date,'MM'),int(settle_cycle-1))
                               then date_add(last_day(happen_date),int(account_period_val-1))
                               else  date_add(date_add(trunc(happen_date,'MM'),int(settle_cycle-1)),int(account_period_val-1)) end 
                     else 
                         date_add(date_add(add_months(trunc(happen_date,'MM'),1),int(settle_cycle-1)),int(account_period_val-1)) end
        when project_end is not null and project_end<>'' then date_add(substr(project_end,1,10),int(account_period_val-1)) 
     ---预付款客户 不计算预期
        when  account_period_code = 'Z007' and unpaid_amounts>residual_amount  then min(date_add(case when unpaid_amounts>residual_amount then happen_date else date_format(current_date,'yyyy-MM-dd')  end,1))over (partition by customer_code,company_code)
when		(project_end is null or project_end='')  then date_format(current_date,'yyyy-MM-dd')  
        when (account_period_code is null or account_period_code ='') and  (settle_cycle is  null or settle_cycle='') 
             and ( project_end is null or  project_end='') 
             then add_months(trunc(happen_date,'MM'),1)  end overdue_date_new
from     
( 
  select 
    t1.source_bill_no,
    t1.source_statement_amount,
    t1.customer_code,
    t1.company_code,
    t1.unpaid_amount,
    t1.bad_debt_amount,
    t1.account_period_code,
    t1.account_period_name,
    t1.account_period_val,
    t1.happen_date,
    t2.settle_cycle,
    case when date_format(t1.happen_date,'yyyy-MM-dd')>=date_format(t2.project_begin,'yyyy-MM-dd') 
                  and date_format(happen_date,'yyyy-MM-dd')<=date_format(t2.project_end,'yyyy-MM-dd')
              then t2.project_end 
              else null end project_end,
    t2.project_begin as project_begin_old    ,
t3.residual_amount  ,
sum(unpaid_amount) over (partition by  t1.customer_code, t1.company_code order by happen_date) unpaid_amounts
  from 
  (
    select 
       *
    from ${hiveconf:source_current_receive_account} 
    where sdt = ${hiveconf:last_day} and to_date(happen_date) < current_date() 
  )t1 
  left join 
  (
    select 
      customer_number,company_code,
      settle_cycle,
      sync_status,
      company_status,
      reconciliation_mode,
      project_begin,--项目开始时间
      project_end,-- 项目结束时间
      case when cus_type=0 then 'MALL' 
           when cus_type=1 then 'BBC' end as cus_type,--0非BBC，1BBC 
		 credit_limit+case when temp_end_time <=${hiveconf:yesterday}   then  temp_credit_limit end as credit_limit
    from csx_ods.source_r_a_crm_customer_company   
    where sdt=regexp_replace(date_sub(current_date,1),'-','')  and is_deleted='0'
  )t2 on t1.customer_code=t2.customer_number and t1.company_code=t2.company_code and t2.cus_type=t1.source_sys
  left join (
	 select
    customer_code, -- 客户编码
    company_code, -- 公司代码
    sum(residual_amount) as residual_amount
  from  ${hiveconf:source_money_back} -- sdt以过账日期分区，只看20200601及以后的，该表计算回款只计算已核销金额
  group by customer_code, company_code
  ) t3 on t1.customer_code=t3.customer_code and t1.company_code=t3.company_code 
)h;


drop table csx_tmp.tmp_dws_sss_r_a_customer_company_accounts_1;
create table csx_tmp.tmp_dws_sss_r_a_customer_company_accounts_1
   as 
select 
  trim(customer_code) as customer_no,
  trim(company_code) as company_code,
  if((datediff(${hiveconf:yesterday}, overdue_date_new) + 1) >= 1, datediff(${hiveconf:yesterday}, overdue_date_new) + 1, 0) as overdue_days,
  if((datediff(${hiveconf:yesterday}, overdue_date_new) + 1) >= 1, unpaid_amount , 0) as overdue_amount,
  unpaid_amount,
  bad_debt_amount,
  account_period_code,
  account_period_name,
  account_period_val,
  -- 账期天数“月结”类型计算处理 计算因子，用于计算逾期系数
  if(account_period_code like 'Y%', if(account_period_val = 31, 45, account_period_val + 15), account_period_val) as acc_val_calculation_factor 
from  csx_tmp.tmp_dws_sss_r_a_customer_company_accounts_0;

drop table csx_tmp.tmp_dws_sss_r_a_customer_company_accounts_2;
create table csx_tmp.tmp_dws_sss_r_a_customer_company_accounts_2
   as 
   select 
      *
   from csx_dw.dws_crm_w_a_customer_company
   where sdt = 'current';
   
drop table csx_tmp.tmp_dws_sss_r_a_customer_company_accounts_3; 
create table csx_tmp.tmp_dws_sss_r_a_customer_company_accounts_3
   as 
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
  from csx_tmp.tmp_dws_sss_r_a_customer_company_accounts_1 
  group by customer_no, company_code;
  
  
drop table csx_tmp.tmp_dws_sss_r_a_customer_company_accounts_4;
create table csx_tmp.tmp_dws_sss_r_a_customer_company_accounts_4
   as 
   -- 获取年至今客户回款金额
  select
    customer_code, -- 客户编码
    company_code, -- 公司代码
    sum(paid_amount) as paid_amount
  from  ${hiveconf:source_money_back} -- sdt以过账日期分区，只看20200601及以后的，该表计算回款只计算已核销金额
  where case when ${hiveconf:new_system} >= ${hiveconf:start_year} then sdt >= ${hiveconf:new_system} else sdt >= ${hiveconf:start_year} end -- 获取今年第一天
    or (sdt = '19990101' and posting_time >= '2020-09-01')
  group by customer_code, company_code;

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
from csx_tmp.tmp_dws_sss_r_a_customer_company_accounts_3 t1 
left outer join 
csx_tmp.tmp_dws_sss_r_a_customer_company_accounts_2 t2 
  on t1.customer_no = t2.customer_no and t1.company_code = t2.company_code
  left outer join 
csx_tmp.tmp_dws_sss_r_a_customer_company_accounts_4 t3 on t1.customer_no = t3.customer_code and t1.company_code = t3.company_code 
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


