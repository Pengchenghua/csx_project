-- 新客毛利趋势与留存(季度)
-- 核心逻辑： 统计(季度)留存新客毛利率趋势

-- 任务名
set mapred.job.name=report_sale_r_q_new_customer_business;
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
-- 4季度前首月
set before_4qua=substr(regexp_replace(to_date(concat(date_format(date_sub(current_date, 1),'y'),'-',
                      floor(cast(date_format(date_sub(current_date, 1),'M') as int)/3.1)*3+1-3*3,'-',
                      date_format(trunc(date_sub(current_date, 1),'MM'),'dd'))
                  ),'-',''), 1, 6);

-- b端客户月统计销售表
set source_order_sale=csx_dw.ads_sale_r_m_customer_business;
-- 目标表
set target_table=csx_dw.report_sale_r_q_new_customer_business;


-- 客户各业务类型业绩最小、最大成交日期
drop table csx_tmp.tmp_cust_sale_max_min;
create temporary table csx_tmp.tmp_cust_sale_max_min
as
select
  a.region_code,                                                               -- 大区编码
  a.region_name,                                 -- 大区名称
  a.province_code,                                 -- 省区编码
  a.province_name,
  substr(a.city_group_name, 1, 3) as city_name,
  a.customer_no,
  a.business_type_code,
  a.business_type_name,
  a.sales_value,
  a.profit,
  a.front_profit,
  a.month,
  concat(substr(a.month, 1, 4), floor(substr(a.month, 5, 2)/3.1) + 1) as sale_quarter,    -- 销售季度
  b.first_order_month,
  concat(substr(b.first_order_month, 1, 4), floor(substr(b.first_order_month, 5, 2)/3.1) + 1) as first_order_quarter,    -- 第一次下单季度
  c.first_order_month_all,
  concat(substr(c.first_order_month_all, 1, 4), floor(substr(c.first_order_month_all, 5, 2)/3.1) + 1) as first_order_quarter_all    -- 第一次下单季度
from
(
  select *
  from ${hiveconf:source_order_sale}
  where month <= ${hiveconf:current_month} and month >= ${hiveconf:before_4qua}
  and channel_code in ('1','7','9')
)a
-- 客户各业务类型业绩最小、最大成交日期
left join
(
  select
    business_type_code,customer_no,
    min(month) as first_order_month
  from ${hiveconf:source_order_sale}
  where channel_code in ('1','7','9')
  group by business_type_code,customer_no
)b on a.customer_no=b.customer_no and a.business_type_code=b.business_type_code
-- 客户全业务业绩最小、最大成交日期
left join
(
  select
    customer_no,
    min(month) as first_order_month_all
  from ${hiveconf:source_order_sale}
  where channel_code in ('1','7','9')
  group by customer_no
)c on a.customer_no=c.customer_no;



with
-- 查询近4季度客户的销售情况:业务类型+全业务
last_sale as
(
  select
    region_code,                                                               -- 大区编码
    region_name,                                 -- 大区名称
    province_code,                                 -- 省区编码
    province_name,                                 -- 省区名称
    city_name,                   -- 归属城市名称
    first_order_quarter,
    sale_quarter,
    business_type_code,
    business_type_name,
    count(distinct customer_no) as all_order_customer_cnt,               -- 下单客户统计
    count(distinct case when first_order_month >= ${hiveconf:before_4qua} then customer_no end) as order_customer_cnt,               -- 下单客户统计-新客
    sum(sales_value) as sales_value,                       -- 销售金额
    sum(profit) as  profit,                             -- 销售毛利
    sum(front_profit) as front_profit                       -- 前端毛利
  from csx_tmp.tmp_cust_sale_max_min
  group by region_code,region_name,province_code,province_name,business_type_code,business_type_name,city_name,first_order_quarter,sale_quarter
  union all
  select
    region_code,                                                               -- 大区编码
    region_name,                                 -- 大区名称
    province_code,                                 -- 省区编码
    province_name,                                 -- 省区名称
    city_name,                   -- 归属城市名称
    first_order_quarter_all first_order_quarter,
    sale_quarter,
    '99' business_type_code,
    '全业务' business_type_name,
    count(distinct customer_no) as all_order_customer_cnt,               -- 下单客户统计
    count(distinct case when first_order_month_all >= ${hiveconf:before_4qua} then customer_no end) as order_customer_cnt,               -- 下单客户统计-新客
    sum(sales_value) as sales_value,                       -- 销售金额
    sum(profit) as  profit,                             -- 销售毛利
    sum(front_profit) as front_profit                       -- 前端毛利
  from csx_tmp.tmp_cust_sale_max_min
  group by region_code,region_name,province_code,province_name,business_type_code,business_type_name,city_name,first_order_quarter_all,sale_quarter
),
-- 每季度新下单客户指标统计
new_customer_statistics as
(
  select
    coalesce(region_code,'全国') as region_code,
    coalesce(region_name,'全国') as region_name,
    coalesce(province_code,'全国') as province_code,
    coalesce(province_name,'全国') as province_name,
    coalesce(city_name,'合计') as city_name,
    business_type_code,
    business_type_name,
    first_order_quarter,
    sum(order_customer_cnt) as new_order_customer_cnt
  from last_sale
  where first_order_quarter >= ${hiveconf:before_4qua}
  and first_order_quarter = sale_quarter
  group by region_code,region_name,province_code,province_name,city_name,business_type_code,business_type_name,first_order_quarter
    grouping sets
    (
      (business_type_code,business_type_name,first_order_quarter),
      (region_code,region_name,province_code,province_name,business_type_code,business_type_name,first_order_quarter),
      (region_code,region_name,province_code,province_name,city_name,business_type_code,business_type_name,first_order_quarter)
    )
),
-- 新客在近4季度的销售统计
classify_statistics as
(
  select
    coalesce(region_code,'全国') as region_code,
    coalesce(region_name,'全国') as region_name,
    coalesce(province_code,'全国') as province_code,
    coalesce(province_name,'全国') as province_name,
    coalesce(city_name,'合计') as city_name,
    business_type_code,
    business_type_name,
    first_order_quarter,
    sale_quarter,
    sum(order_customer_cnt) as order_customer_cnt,
    sum(sales_value) as sales_value,
    sum(profit) as profit,
    sum(front_profit) as front_profit,
    sum(profit)/abs(sum(sales_value)) as profit_prorate
  from last_sale
  where first_order_quarter >= ${hiveconf:before_4qua}
  group by region_code,region_name,province_code,province_name,city_name,business_type_code,business_type_name,first_order_quarter,sale_quarter
    grouping sets
    (
      (business_type_code,business_type_name,first_order_quarter,sale_quarter),
      (region_code,region_name,province_code,province_name,business_type_code,business_type_name,first_order_quarter,sale_quarter),
      (region_code,region_name,province_code,province_name,city_name,business_type_code,business_type_name,first_order_quarter,sale_quarter)
    )
),
-- 查询所有客户在近4季度的销售情况
all_last_sale as
(
  select
    coalesce(region_code,'全国') as region_code,
    coalesce(region_name,'全国') as region_name,
    coalesce(province_code,'全国') as province_code,
    coalesce(province_name,'全国') as province_name,
    coalesce(city_name,'合计') as city_name,
    business_type_code,
    business_type_name,
    sale_quarter,
    sum(all_order_customer_cnt) as all_order_customer_cnt,
    sum(sales_value) as all_sales_value,
    sum(profit) as all_profit,
    sum(front_profit) as all_front_profit,
    sum(profit)/abs(sum(sales_value)) as all_profit_prorate
  from last_sale
  group by region_code,region_name,province_code,province_name,city_name,business_type_code,business_type_name,sale_quarter
    grouping sets
    (
      (business_type_code,business_type_name,sale_quarter),
      (region_code,region_name,province_code,province_name,business_type_code,business_type_name,sale_quarter),
      (region_code,region_name,province_code,province_name,city_name,business_type_code,business_type_name,sale_quarter)
    )
),
--近4季度新客在各季度的销售与留存
new_customer_sale_retention as
(
  select
    t1.region_code,
    t1.region_name,
    t1.province_code,
    t1.province_name,
    t1.city_name,
    t1.business_type_code,
    t1.business_type_name,
    -- 第一次下单季度
    t1.first_order_quarter,
    -- 对应下单月份新下单客户数
    t1.new_order_customer_cnt,
    -- 销售季度
    t2.sale_quarter,
    -- 新下单客户在当月下单客户数
    t2.order_customer_cnt,
    t2.sales_value,
    t2.profit,
    t2.front_profit,
    cast(t2.profit_prorate as decimal(10, 6)) as profit_prorate,
    -- 新客户留存率
    cast(t2.order_customer_cnt/t1.new_order_customer_cnt as decimal(10, 6)) as retention_prorate
  from new_customer_statistics t1 left outer join classify_statistics t2
    on t1.province_code = t2.province_code and t1.business_type_code = t2.business_type_code and t1.first_order_quarter = t2.first_order_quarter and t1.city_name = t2.city_name
)
insert overwrite table ${hiveconf:target_table} partition(quarter)
select
  concat_ws('&', t2.first_order_quarter, t1.province_code, t1.business_type_code, t1.city_name, t1.sale_quarter, ${hiveconf:current_qua}) as biz_id,
  t1.region_code,
  t1.region_name,
  t1.province_code,
  t1.province_name,
  t1.city_name,
  t1.business_type_code,
  t1.business_type_name,
  t2.first_order_quarter,
  t2.new_order_customer_cnt,
  t1.sale_quarter,
  t2.order_customer_cnt,
  t2.sales_value,
  t2.profit,
  t2.front_profit,
  cast(t2.profit_prorate as decimal(10, 6)) as profit_prorate,
  cast(t2.retention_prorate as decimal(10, 6)) as retention_prorate,
  t1.all_order_customer_cnt,
  t1.all_sales_value,
  t1.all_profit,
  t1.all_front_profit,
  cast(t1.all_profit_prorate as decimal(10, 6)) as all_profit_prorate,
  ${hiveconf:current_qua} as `quarter`
from all_last_sale t1
left outer join new_customer_sale_retention t2
  on t1.province_code = t2.province_code and t1.business_type_code = t2.business_type_code and t1.sale_quarter = t2.sale_quarter and t1.city_name = t2.city_name;
