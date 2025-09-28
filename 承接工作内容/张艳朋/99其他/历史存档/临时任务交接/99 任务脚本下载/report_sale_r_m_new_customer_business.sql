-- 新客毛利趋势与留存
-- 核心逻辑： 统计留存新客毛利率趋势

-- 切换tez计算引擎
set mapred.job.name=report_sale_r_m_new_customer_business;
set tez.queue.name=caishixian;

-- 动态分区
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions =1000;
set hive.exec.max.dynamic.partitions.pernode =1000;

-- 中间结果压缩
set mapred.output.compression.codec=org.apache.hadoop.io.compress.snappycodec;
set mapred.output.compression.type=block;
set parquet.compression=snappy;

-- 启用引号识别
set hive.support.quoted.identifiers=none;

-- 昨天月份
set current_month=substr(regexp_replace(date_sub(current_date, 1), '-', ''), 1, 6);

-- 12个月前
set before_12mon=substr(regexp_replace(add_months(date_sub(current_date,1),-11),'-',''), 1, 6);

-- b端客户月统计销售表
set source_order_sale=csx_dw.ads_sale_r_m_customer_business;


-- 目标表
set target_table=csx_dw.report_sale_r_m_new_customer_business;


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
  a.month as sale_month,
  b.first_order_month,
  c.first_order_month_all
from 
(
  select *
  from ${hiveconf:source_order_sale} 
  where month <= ${hiveconf:current_month} and month >= ${hiveconf:before_12mon}
  and channel_code in('1','7','9')
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
)c on a.customer_no=c.customer_no;

 

with 
-- 查询近一年客户的销售情况:业务类型+全业务
last_sale as 
(
  select 
    region_code,                                                               -- 大区编码
    region_name,                  -- 大区名称
    province_code,                  -- 省区编码
    province_name,                  -- 省区名称
    city_name,           -- 归属城市名称
    first_order_month,        -- 第一次下单月份
    sale_month, -- 销售月份
    business_type_code,
    business_type_name,
    count(distinct customer_no) as all_order_customer_cnt,         -- 下单客户统计
    count(distinct case when first_order_month >= ${hiveconf:before_12mon} then customer_no end) as order_customer_cnt,         -- 下单客户统计-新客
    sum(sales_value) as sales_value,             -- 销售金额
    sum(profit) as  profit,                -- 销售毛利
    sum(front_profit) as front_profit             -- 前端毛利
  from csx_tmp.tmp_cust_sale_max_min
  group by 
    region_code,region_name,province_code,province_name,business_type_code,business_type_name,city_name,first_order_month,sale_month  
  union all  
  select 
    region_code,                                                               -- 大区编码
    region_name,                  -- 大区名称
    province_code,                  -- 省区编码
    province_name,                  -- 省区名称
    city_name,           -- 归属城市名称
    first_order_month_all first_order_month,        -- 第一次下单月份
    sale_month, -- 销售月份
    '99' business_type_code,
    '全业务' business_type_name,
    count(distinct customer_no) as all_order_customer_cnt,         -- 下单客户统计
    count(distinct case when first_order_month_all >= ${hiveconf:before_12mon} then customer_no end) as order_customer_cnt,         -- 下单客户统计-新客
    sum(sales_value) as sales_value,             -- 销售金额
    sum(profit) as  profit,                -- 销售毛利
    sum(front_profit) as front_profit             -- 前端毛利
  from csx_tmp.tmp_cust_sale_max_min
  group by 
    region_code,region_name,province_code,province_name,city_name,first_order_month_all,sale_month    
),
-- 每月新下单客户指标统计
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
    first_order_month,
    sum(order_customer_cnt) as new_order_customer_cnt
  from last_sale
  where first_order_month >= ${hiveconf:before_12mon}
  and first_order_month = sale_month
  group by region_code,region_name,province_code,province_name,city_name,business_type_code,business_type_name,first_order_month
    grouping sets 
    (
      (business_type_code,business_type_name,first_order_month),
      (region_code,region_name,province_code,province_name,business_type_code,business_type_name,first_order_month),
      (region_code,region_name,province_code,province_name,city_name,business_type_code,business_type_name,first_order_month)
    )
),
-- 新客在近一年的月销售统计
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
    first_order_month,
    sale_month,
    sum(order_customer_cnt) as order_customer_cnt,
    sum(sales_value) as sales_value,
    sum(profit) as profit,
    sum(front_profit) as front_profit,
    sum(profit)/abs(sum(sales_value)) as profit_prorate
  from last_sale
  where first_order_month >= ${hiveconf:before_12mon}
  group by region_code,region_name,province_code,province_name,city_name,business_type_code,business_type_name,first_order_month,sale_month
    grouping sets 
    (
      (business_type_code,business_type_name,first_order_month,sale_month),
      (region_code,region_name,province_code,province_name,business_type_code,business_type_name,first_order_month,sale_month),
      (region_code,region_name,province_code,province_name,city_name,business_type_code,business_type_name,first_order_month,sale_month)
    )
),
-- 查询所有客户在近12个月的销售情况
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
    sale_month,
    concat(substr(sale_month, 1, 4), floor(substr(sale_month, 5, 2)/3.1) + 1) as sale_quarter, 
    sum(all_order_customer_cnt) as all_order_customer_cnt,
    sum(sales_value) as all_sales_value,
    sum(profit) as all_profit,
    sum(front_profit) as all_front_profit,
    sum(profit)/abs(sum(sales_value)) as all_profit_prorate
  from last_sale  
  group by region_code,region_name,province_code,province_name,city_name,business_type_code,business_type_name,sale_month
    grouping sets 
    (
      (business_type_code,business_type_name,sale_month),
      (region_code,region_name,province_code,province_name,business_type_code,business_type_name,sale_month),
      (region_code,region_name,province_code,province_name,city_name,business_type_code,business_type_name,sale_month)
    )
),
--近12月新客在各月的销售与留存
new_customer_sale_retention as 
(
  select 
    --concat_ws('&', t1.first_order_month, t1.province_code, t1.business_type_code, t1.city_name, t2.sale_month, ${hiveconf:current_month}) as id,
    t1.region_code,
    t1.region_name,
    t1.province_code,
    t1.province_name,
    t1.city_name,
    t1.business_type_code,
    t1.business_type_name,
    -- 第一次下单月份
    t1.first_order_month, 
    -- 第一次下单季度                                                                                                    
    concat(substr(t1.first_order_month, 1, 4), floor(substr(t1.first_order_month, 5, 2)/3.1) + 1) as first_order_quarter,     
    -- 对应下单月份新下单客户数
    t1.new_order_customer_cnt,  
     -- 销售月份                                                                                               
    t2.sale_month,     
    -- 销售季度                                                                                                      
    concat(substr(t2.sale_month, 1, 4), floor(substr(t2.sale_month, 5, 2)/3.1) + 1) as sale_quarter, 
    -- 新下单客户在当月下单客户数                         
    t2.order_customer_cnt,                                                                                                  
    t2.sales_value,
    t2.profit,
    t2.front_profit,
    cast(t2.profit_prorate as decimal(10, 6)) as profit_prorate,
    -- 新客户留存率
    cast(t2.order_customer_cnt/t1.new_order_customer_cnt as decimal(10, 6)) as retention_prorate,                                           
    ${hiveconf:current_month} as month
  from new_customer_statistics t1 left outer join classify_statistics t2 
    on t1.province_code = t2.province_code and t1.business_type_code = t2.business_type_code and t1.first_order_month = t2.first_order_month and t1.city_name = t2.city_name
)   
insert overwrite table ${hiveconf:target_table} partition(month)  
select 
  concat_ws('&', t2.first_order_month, t1.province_code, t1.business_type_code, t1.city_name, t1.sale_month, ${hiveconf:current_month}) as biz_id,
  t1.region_code,
  t1.region_name,
  t1.province_code,
  t1.province_name,
  t1.city_name,
  t1.business_type_code,
  t1.business_type_name,
  -- 第一次下单月份
  t2.first_order_month, 
  -- 第一次下单季度                                                                                                    
  t2.first_order_quarter,     
  -- 对应下单月份新下单客户数
  t2.new_order_customer_cnt,  
   -- 销售月份                                                                                               
  t1.sale_month,     
  -- 销售季度                                                                                                      
  t1.sale_quarter, 
  -- 新下单客户在当月下单客户数                         
  t2.order_customer_cnt,                                                                                                  
  t2.sales_value,
  t2.profit,
  t2.front_profit,
  cast(t2.profit_prorate as decimal(10, 6)) as profit_prorate,
  -- 新客户留存率
  cast(t2.retention_prorate as decimal(10, 6)) as retention_prorate, 
  t1.all_order_customer_cnt,
  t1.all_sales_value,
  t1.all_profit,
  t1.all_front_profit,
  cast(t1.all_profit_prorate as decimal(10, 6)) as all_profit_prorate,
  ${hiveconf:current_month} as month
from all_last_sale t1
left outer join new_customer_sale_retention t2 
  on t1.province_code = t2.province_code and t1.business_type_code = t2.business_type_code and t1.sale_month = t2.sale_month and t1.city_name = t2.city_name
;  