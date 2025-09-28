--kpi 财务指标
---1、当前月指标：
---应收金额、逾期金额、坏账金额 按截止日期计算，
----认领金额、认领核销、认领未核销 按统计当前月销售金额
---2、上月指标：
----销售额上月按业务发生时间所有销售额（大客户、bbc）,对账金额、kp金额、核销金额 按业务发生时间计算，

SET hive.execution.engine=mr;
-- 动态分区
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.exec.max.dynamic.partitions=10000;
-- 中间结果压缩
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.type=BLOCK;
SET parquet.compression=SNAPPY;
-- 启用引号识别
set hive.support.quoted.identifiers=none;
--map 端聚合
set hive.map.aggr=true;
--支持正则
--当天
set current_day=regexp_replace(date_sub(current_date,1),'-','');

set current_date2=date_sub(current_date,1);
drop table csx_tmp.tmp_dws_sss_r_d_receive_kp_statement_0;
create table csx_tmp.tmp_dws_sss_r_d_receive_kp_statement_0
   as
select
       t1.source_bill_no,
       t1.relation_order_no,
       t1.merchant_type,
       t1.order_status,
       t1.customer_code,
       t1.customer_name,
       t1.sub_customer_code,
       t1.sub_customer_name,
t1.happen_date,
       t1.source_statement_amount,
       t1.company_code,
       t1.company_name,
       t1.statement_no,
       t1.statement_date,
       t1.customer_statement_person,
       t1.customer_statement_date,
       t1.statement_state,
       t1.kp_state,
       t1.kp_amount,
       t1.paid_amount,
       t1.money_back_status,
       t1.overdue_date,
       t1.overdue_days,
       t1.unpaid_amount,
       t1.overdue_amount,
       t1.tax_amount,
       t1.source_sys,
       t1.bad_debt_amount,
       t1.residue_total_amount,
       if(date_format(t1.happen_date,'yyyy-MM-dd')>=date_format(t2.project_begin,'yyyy-MM-dd') 
                   and  date_format(happen_date,'yyyy-MM-dd')<=date_format(t2.project_end,'yyyy-MM-dd'),t2.project_end,null) as project_end,
       t2.settle_cycle as settle_2,
coalesce(
        trunc(if(date_format(t1.happen_date,'yyyy-MM-dd')>=date_format(t2.project_begin,'yyyy-MM-dd') 
                   and  date_format(happen_date,'yyyy-MM-dd')<=date_format(t2.project_end,'yyyy-MM-dd'),t2.project_end,null) ,'MM'),
        case
            when dayofmonth(regexp_replace(t1.happen_date, '\\.0', '')) >=  t2.settle_cycle then
                case
                    when
                            date_format(concat_ws('-', cast(year(regexp_replace(t1.happen_date, '\\.0', '')) as string),
                                                  cast(month(regexp_replace(t1.happen_date, '\\.0', '')) as string),
                                                  cast(t2.settle_cycle  as string)),
                                        'yyyy-MM-dd') >
                            last_day(regexp_replace(t1.happen_date, '\\.0', ''))
                        then
                        last_day(regexp_replace(t1.happen_date, '\\.0', ''))
                    else
                        date_format(concat_ws('-', cast(year(regexp_replace(t1.happen_date, '\\.0', '')) as string),
                                              cast(month(regexp_replace(t1.happen_date, '\\.0', '')) as string),
                                              cast(t2.settle_cycle as string)), 'yyyy-MM-dd')
                    end
            else
                case
                    when
                            date_format(concat_ws('-', cast(
                                    year(add_months(regexp_replace(t1.happen_date, '\\.0', ''), -1)) as string), cast(
                                                          month(
                                                                  add_months(regexp_replace(t1.happen_date, '\\.0', ''), -1)) as string),
                                                  cast( t2.settle_cycle as string)),
                                        'yyyy-MM-dd') >
                            last_day(add_months(regexp_replace(t1.happen_date, '\\.0', ''), -1))
                        then
                        last_day(add_months(regexp_replace(t1.happen_date, '\\.0', ''), -1))
                    else
                        date_format(concat_ws('-', cast(
                                year(add_months(regexp_replace(t1.happen_date, '\\.0', ''), -1)) as string), cast(month(
                                add_months(regexp_replace(t1.happen_date, '\\.0', ''), -1)) as string),
                                              cast( t2.settle_cycle  as string)), 'yyyy-MM-dd')
                    end
            end,
        trunc(t1.happen_date,'MM') 
    )           as statement_start_date,
coalesce(
        last_day(substr(if(date_format(t1.happen_date,'yyyy-MM-dd')>=date_format(t2.project_begin,'yyyy-MM-dd') 
                   and  date_format(happen_date,'yyyy-MM-dd')<=date_format(t2.project_end,'yyyy-MM-dd'),t2.project_end,null),1,10)),
        date_add(
        case
            when dayofmonth(regexp_replace(t1.happen_date, '\\.0', '')) >= t2.settle_cycle  then
                case
                    when
                            date_format(concat_ws('-', cast(
                                    year(add_months(regexp_replace(t1.happen_date, '\\.0', ''), 1)) as string),
                                                  cast(
                                                          month(add_months(regexp_replace(t1.happen_date, '\\.0', ''), 1)) as string),
                                                  cast( t2.settle_cycle as string)),
                                        'yyyy-MM-dd') >
                            last_day(add_months(regexp_replace(t1.happen_date, '\\.0', ''), 1))
                        then
                        last_day(add_months(regexp_replace(t1.happen_date, '\\.0', ''), 1))
                    else
                        date_format(concat_ws('-', cast(
                                year(add_months(regexp_replace(t1.happen_date, '\\.0', ''), 1)) as string),
                                              cast(
                                                      month(add_months(regexp_replace(t1.happen_date, '\\.0', ''), 1)) as string),
                                              cast(t2.settle_cycle  as string)), 'yyyy-MM-dd')
                    end
            else
                case
                    when
                            date_format(concat_ws('-', cast(
                                    year(regexp_replace(t1.happen_date, '\\.0', '')) as string), cast(
                                                          month(regexp_replace(t1.happen_date, '\\.0', '')) as string),
                                                  cast( t2.settle_cycle as string)),
                                        'yyyy-MM-dd') >
                            last_day(regexp_replace(t1.happen_date, '\\.0', ''))
                        then
                        last_day(regexp_replace(t1.happen_date, '\\.0', ''))
                    else
                        date_format(concat_ws('-', cast(
                                year(regexp_replace(t1.happen_date, '\\.0', '')) as string), cast(
                                                      month(regexp_replace(t1.happen_date, '\\.0', '')) as string),
                                              cast( t2.settle_cycle  as string)), 'yyyy-MM-dd')
                    end
            end, -1),
        last_day(t1.happen_date)
    )           as statement_end_date,
t1.sdt
from (
         select *
         from csx_dw.dwd_sss_r_d_source_bill
         where sdt >= '20200601'
     ) t1
         left join -- 主客户
    (
        select customer_number,
               company_code,
               reconciliation_period,                                        --对账周期
               case when cus_type = 1 then 'BBC' else 'MALL' end as cus_type,--：0非BBC，1BBC'
               case when payment_terms in ('Y001','Y002','Y003','Y004','Y005','Y006') and settle_cycle is not null and settle_cycle!='' and settle_cycle>0 then settle_cycle else 1 end settle_cycle,
               project_begin,
               project_end
        from csx_ods.source_r_a_crm_customer_company
        where is_deleted = 0
          and company_status = 1
          and sdt = ${hiveconf:current_day}
    ) t2 on t1.customer_code = t2.customer_number and t1.company_code = t2.company_code and t1.source_sys = t2.cus_type
;

---对账、回款、
drop table csx_tmp.tmp_dws_sss_r_d_receive_kp_statement_1;
create table csx_tmp.tmp_dws_sss_r_d_receive_kp_statement_1
   as
select
  customer_code as customer_no,
  company_code,
  (sum(case when statement_state='20' then residue_total_amount else 0 end)
  +sum( source_statement_amount - residue_total_amount - kp_amount )+sum(kp_amount)) as statement_amount,
  sum(case when statement_state<>'20' then source_statement_amount end) as unstatement_amount,
  sum(kp_amount) as kp_amount,
  sum(source_statement_amount) as tax_sale_amount,---财务业务确认以财务对账来源单为销售金额计算(张正孝)
  sum(tax_amount) as tax_amount,
  '' as claim_amount,
  '' as claim_hx_amount,
  '' as claim_unhx_amount,
  sum(paid_amount) as back_money_amount,
  '' as unhx_receive_amount,
  '' as unhx_overdue_amount,
  '' as unhx_unoverdue_amount,
  ${hiveconf:current_day} as sdt
from csx_tmp.tmp_dws_sss_r_d_receive_kp_statement_0
where statement_start_date<=add_months(${hiveconf:current_date2},-1) and statement_end_date>=add_months(${hiveconf:current_date2},-1)
group by customer_code,company_code;


----求每月应收金额 时间码表dws_basic_w_a_date
----应收是时间点，其他是时间段
---- 逾期、未逾期、坏账、应收 过去每月最后一天+当月当前天
drop table csx_tmp.tmp_dws_sss_r_d_receive_kp_statement_2;
create table csx_tmp.tmp_dws_sss_r_d_receive_kp_statement_2 
   as 
select 
  customer_no,
  company_code,
  '' as statement_amount,
  '' as unstatement_amount,
  '' as kp_amount,
  '' as tax_sale_amount,
  '' as tax_amount,
  '' as claim_amount,
  '' as claim_hx_amount,
  '' as claim_unhx_amount,
  '' as back_money_amount,
  receivable_amount as unhx_receive_amount,
  overdue_amount as unhx_overdue_amount,
  non_overdue_amount as unhx_unoverdue_amount,
  sdt
from csx_dw.dws_sss_r_a_customer_company_accounts 
where sdt=${hiveconf:current_day};



----聚合
drop table csx_tmp.tmp_dws_sss_r_d_receive_kp_statement_3;
create table csx_tmp.tmp_dws_sss_r_d_receive_kp_statement_3
   as 
 select
    customer_no,
    company_code,
    sum(statement_amount) as statement_amount,
    sum(unstatement_amount) as unstatement_amount,
    sum(kp_amount) as kp_amount,
    sum(tax_sale_amount) as tax_sale_amount,---财务业务确认以财务对账来源单为销售金额计算(张正孝)
    sum(tax_amount) as tax_amount,---财务业务确认以财务对账来源单为销售金额计算(张正孝)
    sum(claim_amount) as claim_amount,
    sum(claim_hx_amount) as claim_hx_amount,
    sum(claim_unhx_amount) as claim_unhx_amount,
    sum(back_money_amount) as back_money_amount,
    sum(unhx_receive_amount) as unhx_receive_amount,
    sum(if(unhx_overdue_amount>=0,unhx_overdue_amount,0)) as unhx_overdue_amount,
    sum(if(unhx_unoverdue_amount>=0,unhx_unoverdue_amount,0)) as unhx_unoverdue_amount,
    sdt
from 
(
---逾期、未逾期、应收
select * from csx_tmp.tmp_dws_sss_r_d_receive_kp_statement_2  
union all 
select * from csx_tmp.tmp_dws_sss_r_d_receive_kp_statement_1 
union all 
--月度回款计划金额
--本期已认领金额
select 
 customer_code as customer_no,
 company_code,
 '' as statement_amount,
 '' as unstatement_amount,
 '' as kp_amount,
 '' as tax_sale_amount,---财务业务确认以财务对账来源单为销售金额计算(张正孝)
 '' as tax_amount,
 sum(claim_amount) as claim_amount,
 sum(paid_amount) as claim_hx_amount,
 sum(residual_amount) as claim_unhx_amount,
 '' as back_money_amount,
 '' as unhx_receive_amount,
 '' as unhx_overdue_amount,
 '' as unhx_unoverdue_amount,
 ${hiveconf:current_day} as sdt
from csx_dw.dwd_sss_r_d_money_back 
where  substr(regexp_replace(claim_time,'-',''),1,6)=substr(${hiveconf:current_day},1,6)
 and regexp_replace(substr(claim_time,1,10),'-','')>='20200601'
 ---剔除补救单
 and (paid_amount<>'0' or residual_amount<>'0')
group by customer_code,company_code,regexp_replace(substr(claim_time,1,7),'-','')
)h group by customer_no,company_code,sdt;
-----
----添加数据基本信息
drop table csx_tmp.tmp_dws_sss_r_d_receive_kp_statement_4; 
create table csx_tmp.tmp_dws_sss_r_d_receive_kp_statement_4
   as 
select 
  t4.region_code,
  t4.region_name,
  t4.province_code,
  t4.province_name,
  t4.city_group_code as city_code,
  t4.city_group_name as city_name,
  t2.sales_id as sale_code,
  t2.work_no as sale_work_no,
  t2.sales_name as sale_name,
  t2.first_supervisor_code as sale_supervisor_code,
  t2.first_supervisor_work_no as sale_supervisor_work_no,
  t2.first_supervisor_name as sale_supervisor_name,
  coalesce(t2.customer_no,t1.customer_no) as customer_no,
  t2.customer_name,
  coalesce(t3.code,t1.company_code) as company_code,
  t3.name as company_mame,
  t1.statement_amount,
  t1.unstatement_amount,
  t1.kp_amount,
  t1.tax_sale_amount, ---财务含税销售额
  t1.tax_amount,
  t1.claim_amount,
  t1.claim_hx_amount,
  t1.claim_unhx_amount,
  t1.back_money_amount,
  t1.unhx_receive_amount,
  if(t1.unhx_overdue_amount>=0,t1.unhx_overdue_amount,0) as unhx_overdue_amount,
  if(t1.unhx_unoverdue_amount>=0,t1.unhx_unoverdue_amount,0) as unhx_unoverdue_amount,
  sdt
from 
csx_tmp.tmp_dws_sss_r_d_receive_kp_statement_3 t1
left join
(--客户 销售员 主管 城市 省区
  select 
    customer_no,
    customer_name,
    sales_region_code,
    sales_region_name,
    sales_province_code,
    sales_province_name,
    city_group_code,
    city_group_name,
    sales_city_code,
    sales_city_name,
    sales_id,
    work_no,
    sales_name,
    first_supervisor_code,
    first_supervisor_work_no,
    first_supervisor_name
  from csx_dw.dws_crm_w_a_customer 
  where sdt='current'
)t2 on t1.customer_no=t2.customer_no
left join 
(
  select 
    code,name
  from csx_dw.dws_basic_w_a_company_code 
  where sdt='current'
)t3 on t1.company_code=t3.code 
left join 
(
  select 
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
)t4 on t2.sales_city_code=t4.city_code and t2.sales_province_code=t4.area_province_code;



----刷新数据
----ads_sss_r_m_kpi_province_account_indicator
insert overwrite table csx_dw.dws_sss_r_d_receive_kp_statement partition(sdt) 
select 
  concat_ws('&',sdt,cast(sale_code as string),cast(customer_no as string),cast(company_code as string)) as biz_id,
  region_code,
  region_name,
  province_code,
  province_name,
  city_code,
  city_name,
  sale_code,
  sale_work_no,
  sale_name,
  sale_supervisor_code,
  sale_supervisor_work_no,
  sale_supervisor_name,
  customer_no,
  customer_name,
  company_code,
  company_mame,
  statement_amount,
  unstatement_amount,
  kp_amount,
  tax_sale_amount, ---财务含税销售额
  tax_amount,
  claim_amount,
  claim_hx_amount,
  claim_unhx_amount,
  back_money_amount,
  unhx_receive_amount,
  unhx_overdue_amount,
  unhx_unoverdue_amount,
  (statement_amount/tax_sale_amount) as statement_ratio,
  kp_amount/tax_sale_amount as kp_ratio,
  if((unhx_overdue_amount/unhx_receive_amount)>1,1,unhx_overdue_amount/unhx_receive_amount) as overdue_ratio,
  sdt
from csx_tmp.tmp_dws_sss_r_d_receive_kp_statement_4; 
