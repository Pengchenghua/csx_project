-- 毛利率监控仪表盘
-- 核心逻辑： 统计近一周业务类型省区销售情况

-- 切换tez计算引擎
set mapred.job.name=report_sale_r_w_area;
set hive.execution.engine=tez;
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

-- 昨天日期
set current_day=regexp_replace(date_sub(current_date, 1), '-', '');

-- 本周起始日期(本周六)
set current_week_start=regexp_replace(date_sub(date_sub(current_date,1),pmod(datediff(date_sub(current_date,1),'2020-01-04'),7)),'-','');

-- 上周起始日期(上周周六)
set last_week_start=regexp_replace(date_sub(date_sub(current_date,1),pmod(datediff(date_sub(current_date,1),'2020-01-04'),7)+7),'-','');

-- 相对昨天上周同比日期
set last_week_current_day=regexp_replace(date_sub(current_date,1+7),'-','');	

-- 当前是第几周
set current_weeks=weekofyear(date_sub(current_date(),-1));

-- 销售订单表
set source_order_sale=csx_dw.ads_sale_r_d_2b_order;

-- 目标表
set target_table=csx_dw.report_sale_r_w_area; 

-- 各地区业务业务类型销售统计
with business_area_sale as 
(
  select  	
    -- 大区编码																			                
    region_code,
    -- 大区名称																						                    
    region_name,																						                    
    province_code,  
    province_name, 
    -- 本期销售额 
    sum(case when sdt >= ${hiveconf:current_week_start} then sales_value else 0 end ) as current_period_sales_value,
    -- 本期毛利       
    sum(case when sdt >= ${hiveconf:current_week_start} then profit else 0 end ) as current_period_profit,
    -- 上期销售额             	
    sum(case when sdt <= ${hiveconf:last_week_current_day} then sales_value else 0 end ) as last_period_sales_value,	
    -- 上期毛利    
    sum(case when sdt <= ${hiveconf:last_week_current_day} then profit else 0 end ) as last_period_profit                 
  from ${hiveconf:source_order_sale} 
  where sdt >= ${hiveconf:last_week_start} and sdt <= ${hiveconf:current_day} 
  group by region_code, region_name, province_code, province_name
  having current_period_sales_value is not null and last_period_sales_value is not null
  --having current_period_sales_value is not null and current_period_sales_value <> 0
)
insert overwrite table ${hiveconf:target_table} partition(week) 
select 
  -- 唯一id
  concat_ws('&', province_code, concat(substr(${hiveconf:current_day},1,4), lpad(${hiveconf:current_weeks},2,'0'))) as biz_id,
  region_code,																						     -- 大区编码
  region_name,																						     -- 大区名称
  province_code,  
  province_name,  
  current_period_sales_value,
  current_period_profit,
  -- 本期毛利率
  round(current_period_profit / abs(current_period_sales_value), 6) as current_period_profit_prorate,
  last_period_sales_value,
  last_period_profit,
  -- 上期毛利率
  round(coalesce(last_period_profit / abs(last_period_sales_value), 0), 6) as last_period_profit_prorate,
  -- 销售额环比增长率
  round(current_period_sales_value / abs(last_period_sales_value) - 1, 6) as sales_hb_increment,
  -- 毛利率环比增长率
  round(current_period_profit / abs(current_period_sales_value) - coalesce(last_period_profit / abs(last_period_sales_value), 0), 6) as profit_prorate_hb_diff,
  -- 销售额环比增长率排名
  rank() over(order by round(current_period_sales_value / abs(last_period_sales_value) - 1, 6) desc) as sale_hb_increment_rank,
  -- 本期毛利率排名
  rank() over(order by round(current_period_profit / abs(current_period_sales_value), 6) desc) as profit_prorate_rank, 
  -- 毛利率环比增长排名                       
  rank() over(order by round(current_period_profit / abs(current_period_sales_value) - coalesce(last_period_profit / abs(last_period_sales_value), 0), 6) desc) as profit_prorate_hb_diff_rank,             
  concat(substr(${hiveconf:current_day},1,4), lpad(${hiveconf:current_weeks},2,'0')) as week
from business_area_sale;