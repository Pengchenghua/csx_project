-- 商品简报  B端客户日负毛利客户统计
-- 核心逻辑：统计

-- 切换tez计算引擎
set mapred.job.name=report_sale_r_d_negative_customer;
-- set hive.execution.engine=tez;
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

-- 统计日期
set current_day = regexp_replace(date_sub(current_date, 1), '-', '');

-- 销售明细表
set source_sale_detail=csx_dw.dws_sale_r_d_detail;

-- 目标表
set target_table=csx_dw.report_sale_r_d_negative_customer;

with current_2b_sale as 
(
  select 
    region_code,
    region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,	
    customer_no,
    customer_name,
    goods_code,
    goods_name,
    sum(sales_value) as sales_value,
    sum(profit) as profit,
    sum(case when profit < 0 then profit else 0 end) as negative_profit,
    sum(sum(sales_value)) over(partition by customer_no) as customer_sales_value,
    sum(sum(profit)) over(partition by customer_no) as customer_profit,
    row_number() over(partition by customer_no order by sum(profit) asc) as customer_goods_profit_no
  from ${hiveconf:source_sale_detail} 
  where sdt = ${hiveconf:current_day} and channel_code in ('1', '7', '9') 
  group by region_code,
    region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,	
    customer_no,
    customer_name,
    goods_code,
    goods_name
  union all 
  select 
    '0' as region_code,
    '全国' as region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,	
    customer_no,
    customer_name,
    goods_code,
    goods_name,
    sum(sales_value) as sales_value,
    sum(profit) as profit,
    sum(case when profit < 0 then profit else 0 end) as negative_profit,
    sum(sum(sales_value)) over(partition by customer_no) as customer_sales_value,
    sum(sum(profit)) over(partition by customer_no) as customer_profit,
    row_number() over(partition by customer_no order by sum(profit) asc) as customer_goods_profit_no
  from ${hiveconf:source_sale_detail} 
  where sdt = ${hiveconf:current_day} and channel_code in ('1', '7', '9') 
  group by region_code,
    region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,	
    customer_no,
    customer_name,
    goods_code,
    goods_name
),
negative_profit_customer_sale as
(
  
  select 
    t1.region_code,
    t1.region_name,
    t1.province_code,
    t1.province_name,
    t1.city_group_code,
    t1.city_group_name,	
    t1.customer_no,
    t1.customer_name,
    t1.sales_value,
    t1.profit,
    t1.negative_profit,
    t1.region_customer_ranking,
    t1.province_customer_ranking,
	t1.city_customer_ranking,
    t2.top1_goods,
    t2.top1_goods_profit
  from 
  (
    select 
      region_code,
      region_name,
      province_code,
      province_name,
      city_group_code,
      city_group_name,	  
      customer_no,
      customer_name,
      sum(sales_value) as sales_value,
      sum(profit) as profit,
      sum(negative_profit) as negative_profit,
      -- 大区负毛利客户排名
      row_number() over(partition by region_code order by sum(profit) asc) as region_customer_ranking, 
      -- 省区负毛利客户排名  
      row_number() over(partition by region_code, province_code order by sum(profit) asc) as province_customer_ranking,
      -- 省区负毛利客户排名  
      row_number() over(partition by region_code, province_code,city_group_code order by sum(profit) asc) as city_customer_ranking  
    from current_2b_sale 
    where customer_sales_value > 0 and customer_profit < 0
    group by region_code,
      region_name,
      province_code,
      province_name,
      city_group_code,
      city_group_name,	  
      customer_no,
      customer_name
  )t1 join 
  (
    select distinct
      customer_no,
      -- 负毛利top1商品
      concat_ws('-', goods_code, goods_name) as  top1_goods,  
      -- 负毛利top1商品毛利   
      profit as top1_goods_profit 
    from current_2b_sale 
    where customer_sales_value > 0 and customer_profit < 0 and customer_goods_profit_no = 1
  )t2 on t1.customer_no = t2.customer_no
)
insert overwrite table ${hiveconf:target_table} partition(sdt) 
-- 全国top20客户
select
  concat_ws('&', '0', customer_no, ${hiveconf:current_day}) as id,
  '0' as statistic_area,
  region_code,
  region_name,
  province_code,
  province_name,
  city_group_code,
  city_group_name,  
  customer_no,
  customer_name,
  sales_value,
  profit,
  negative_profit,
  region_customer_ranking as customer_ranking,  -- 全国负毛利客户排名
  concat(top1_goods, ':',round(top1_goods_profit / negative_profit * 100), '%') as top1_goods_prorate,  -- top1商品在负毛利商品中销售占比
  ${hiveconf:current_day} as sdt
from negative_profit_customer_sale
where region_code = '0' and region_customer_ranking <= 20
union all 
-- 大区top20客户
select
  concat_ws('&', '1', customer_no, ${hiveconf:current_day}) as id,
  '1' as statistic_area,
  region_code,
  region_name,
  province_code,
  province_name,
  city_group_code,
  city_group_name,  
  customer_no,
  customer_name,
  sales_value,
  profit,
  negative_profit,
  region_customer_ranking as customer_ranking,  -- 大区负毛利客户排名
  concat(top1_goods, ':',round(top1_goods_profit / negative_profit * 100), '%') as top1_goods_prorate,
  ${hiveconf:current_day} as sdt
from negative_profit_customer_sale
where region_code <> '0' and region_customer_ranking <= 20
union all 
-- 省区top20客户
select
  concat_ws('&', '2', customer_no, ${hiveconf:current_day}) as id,
  '2' as statistic_area,
  region_code,
  region_name,
  province_code,
  province_name,
  city_group_code,
  city_group_name,  
  customer_no,
  customer_name,
  sales_value,
  profit,
  negative_profit,
  province_customer_ranking as customer_ranking,  -- 省区负毛利客户排名
  concat(top1_goods, ':',round(top1_goods_profit / negative_profit * 100), '%') as top1_goods_prorate,
  ${hiveconf:current_day} as sdt
from negative_profit_customer_sale
where region_code <> '0' and province_customer_ranking <= 20
union all 
-- 城市top20客户
select
  concat_ws('&', '3', customer_no, ${hiveconf:current_day}) as id,
  '3' as statistic_area,
  region_code,
  region_name,
  province_code,
  province_name,
  city_group_code,
  city_group_name,  
  customer_no,
  customer_name,
  sales_value,
  profit,
  negative_profit,
  city_customer_ranking as customer_ranking,  -- 城市负毛利客户排名
  concat(top1_goods, ':',round(top1_goods_profit / negative_profit * 100), '%') as top1_goods_prorate,
  ${hiveconf:current_day} as sdt
from negative_profit_customer_sale
where region_code <> '0' and city_customer_ranking <= 20;