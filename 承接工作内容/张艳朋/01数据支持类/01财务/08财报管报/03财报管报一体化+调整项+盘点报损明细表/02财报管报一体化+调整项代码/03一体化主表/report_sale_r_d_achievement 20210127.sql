-- 销售主题-业绩报表/财务主题-财务管报一体化表
-- 核心逻辑： 统计所有客户销售业绩
-- 更新范围： 更新近两个月数据

-- 切换tez计算引擎
set mapred.job.name=report_sale_r_d_achievement;
SET hive.execution.engine=tez;
SET tez.queue.name=caishixian;

-- 动态分区
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions =1000;
SET hive.exec.max.dynamic.partitions.pernode =1000;

-- 中间结果压缩
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.type=BLOCK;
SET parquet.compression=SNAPPY;

-- 启用引号识别
set hive.support.quoted.identifiers=none;

-- 最新日期
set end_day=regexp_replace(date_sub(current_date, 1), '-', '');

-- 起始日期
set start_day=regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');

-- 销售订单表
set source_order_sale=csx_dw.dws_sale_r_d_detail;

-- 客户销售属性表
set source_customer_active=csx_dw.dws_crm_w_a_customer_active;

-- 目标表
set target_table=csx_dw.report_sale_r_d_achievement; 

with last_sale_order as 
(
  select 
    region_code,   -- 大区编码
    region_name,   -- 大区名称
    province_code,
    province_name,
    city_group_code,  -- 城市组编码
    city_group_name,  -- 城市组名称
    channel_code,     -- 客户渠道编码
    channel_name,     -- 客户渠道名称
    customer_no,
    sdt,
    sum(sales_value) as sales_value,  
    sum(sales_cost) as sales_cost,
    sum(profit) as profit,
    sum(excluding_tax_sales) as no_tax_sales,
    sum(excluding_tax_cost) as no_tax_cost,
    sum(excluding_tax_profit) as no_tax_profit    
  from ${hiveconf:source_order_sale}
  where sdt>=${hiveconf:start_day} and sdt<=${hiveconf:end_day}
    -- 排除特殊订单, 个性化需求
    and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
          'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') 
    	or order_no is null)
  group by region_code, region_name, province_code, province_name, city_group_code, city_group_name, channel_code, channel_name, customer_no, sdt 
),
last_active_customer as 
(
  select 
  	customer_no,
  	customer_name,  
  	attribute_code, -- 属性编码
  	sign_date,    -- 签约日期
  	first_order_date    -- 第一次销售日期
  from ${hiveconf:source_customer_active} 
  where sdt=${hiveconf:end_day}
)
insert overwrite table ${hiveconf:target_table} partition(sdt)
select 
  concat_ws('&', t1.customer_no, t1.sdt) as id,
  t1.region_code,
  t1.region_name,
  t1.province_code,
  t1.province_name,
  t1.city_group_code,
  t1.city_group_name,
  t1.channel_code,
  t1.channel_name,
  t1.customer_no,
  t2.customer_name,
  t2.attribute_code,
  if(substr(t2.sign_date,1,6)=substr(t1.sdt,1,6),'是', '否') is_new_sign,
  if(substr(t2.first_order_date,1,6)=substr(t1.sdt,1,6),'是', '否') is_new_sale,  
  t1.sales_value,
  t1.sales_cost,
  t1.profit,
  t1.no_tax_sales,
  t1.no_tax_cost,
  t1.no_tax_profit,
  t1.sdt
from last_sale_order t1 left outer join last_active_customer t2 on t1.customer_no = t2.customer_no;