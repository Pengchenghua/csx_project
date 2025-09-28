-- 新客毛利趋势与留存
-- 核心逻辑： 统计留存新客毛利率趋势

-- 任务名
set mapred.job.name=report_sale_r_m_new_customer_business_detail;
-- 动态分区
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
-- 中间结果压缩
set mapred.output.compression.codec=org.apache.hadoop.io.compress.snappycodec;
set mapred.output.compression.type=block;
set parquet.compression=snappy;
-- 启用引号识别
set hive.support.quoted.identifiers=none;

-- 昨天月份
set current_month=substr(regexp_replace(date_sub(current_date, 1), '-', ''), 1, 6);
-- 昨天季度
set current_qua=concat(date_format(date_sub(current_date, 1),'y'),floor(cast(date_format(date_sub(current_date, 1),'M') as int)/3.1)+1);
-- 12个月前
set before_12mon=substr(regexp_replace(add_months(date_sub(current_date,1),-11),'-',''), 1, 6);

-- b端客户月统计销售表
set source_order_sale=csx_dw.ads_sale_r_m_customer_business;
set crm_customer=csx_dw.dws_crm_w_a_customer;
-- 目标表
set target_table=csx_dw.report_sale_r_m_new_customer_business_detail;

insert overwrite table ${hiveconf:target_table} partition(month)
select
  concat_ws('&', b.first_order_month, a.province_code, a.business_type_code,substr(a.city_group_name, 1, 3), a.customer_no,a.month, ${hiveconf:current_month}) as biz_id,
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  substr(a.city_group_name, 1, 3) as city_name,
  a.business_type_code,
  a.business_type_name,
  a.customer_no,
  d.customer_name,
  b.first_order_month,
  concat(substr(b.first_order_month, 1, 4), floor(substr(b.first_order_month, 5, 2)/3.1) + 1) as first_order_quarter,    -- 第一次下单季度
  c.first_order_month_all,
  concat(substr(c.first_order_month_all, 1, 4), floor(substr(c.first_order_month_all, 5, 2)/3.1) + 1) as first_order_quarter_all,    -- 第一次下单季度
  a.month sale_month,
  concat(substr(a.month, 1, 4), floor(substr(a.month, 5, 2)/3.1) + 1) as sale_quarter,    -- 销售季度
  a.sales_value,
  a.profit,
  a.front_profit,
  ${hiveconf:current_qua} as quarter,
  ${hiveconf:current_month} as month
from
(
  select *
  from ${hiveconf:source_order_sale}
  where month <= ${hiveconf:current_month} and month >= ${hiveconf:before_12mon} and channel_code in('1','7','9')
)a
-- 客户各业务类型业绩最小、最大成交日期
left join
(
  select
    business_type_code,customer_no,
    min(month) as first_order_month
  from ${hiveconf:source_order_sale}
  where channel_code in('1','7','9')
  group by business_type_code,customer_no
)b on a.customer_no=b.customer_no and a.business_type_code=b.business_type_code
-- 客户全业务业绩最小、最大成交日期
left join
(
  select
    customer_no,
    min(month) as first_order_month_all
  from ${hiveconf:source_order_sale}
  where channel_code in('1','7','9')
  group by customer_no
)c on a.customer_no=c.customer_no
left join
(
  select
  customer_no,
  customer_name,
  coalesce(attribute_desc,'其他') as attribute_name,
  cast(coalesce(attribute,'-1') as string) as attribute_code
  from ${hiveconf:crm_customer}
  where sdt='current'
)d on d.customer_no=a.customer_no ;
