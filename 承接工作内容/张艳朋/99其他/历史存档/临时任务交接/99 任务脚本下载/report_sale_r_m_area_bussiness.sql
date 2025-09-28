-- 毛利率监控仪表盘
-- 核心逻辑： 统计近一个月业务类型省区销售情况

-- 切换tez计算引擎
set mapred.job.name=report_sale_r_m_area_bussiness;
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

-- 本月起始日期
set current_month_start=regexp_replace(trunc(date_sub(current_date, 1),'MM'),'-','');

-- 上月起始日期
set last_month_start=regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'), -1),'-','');

-- 相对昨天上月同比日期
set last_month_current_day=concat(substr(regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-',''),1,6),
					if(date_sub(current_date,1)=last_day(date_sub(current_date,1))
					,substr(regexp_replace(last_day(add_months(trunc(date_sub(current_date,1),'MM'),-1)),'-',''),7,2)
					,substr(regexp_replace(date_sub(current_date,1),'-',''),7,2)));	

-- 销售订单表
set source_order_sale=csx_dw.ads_sale_r_d_2b_order;

-- 目标表
set target_table=csx_dw.report_sale_r_m_area_bussiness; 

-- 各地区业务业务类型销售统计
with business_area_sale as 
(
  select 
    -- 业务类型
    business_type_code,
    business_type_name,  	
     -- 大区编码																			                    
    region_code,
    -- 大区名称																						                   
    region_name,																						                    
    province_code,  
    province_name,  
     -- 本期销售额
    sum(case when sdt >= ${hiveconf:current_month_start} then sales_value else 0 end ) as current_period_sales_value,  
    -- 本期毛利    
    sum(case when sdt >= ${hiveconf:current_month_start} then profit else 0 end ) as current_period_profit, 
    -- 上期销售额            	
    sum(case when sdt <= ${hiveconf:last_month_current_day} then sales_value else 0 end ) as last_period_sales_value,	 
     -- 上期毛利   
    sum(case when sdt <= ${hiveconf:last_month_current_day} then profit else 0 end ) as last_period_profit,    
    -- 本期业务销售额                
    sum(current_period_sales_value) over(partition by business_type_code) as current_period_business_sales_value,	
    -- 本期业务毛利					 
    sum(current_period_profit) over(partition by business_type_code) as current_period_business_profit,     
    -- 本期地区销售额                          
    sum(current_period_sales_value) over(partition by province_code) as current_period_area_sales_value,	
    -- 上期地区销售额                            
    sum(last_period_sales_value) over(partition by province_code) as last_period_area_sales_value                                  
  from ${hiveconf:source_order_sale} 
  where sdt >= ${hiveconf:last_month_start} and sdt <= ${hiveconf:current_day} 
  group by  business_type_code, business_type_name, region_code, region_name, province_code, province_name 
  having current_period_sales_value is not null and last_period_sales_value is not null
  --having current_period_sales_value is not null and current_period_sales_value <> 0  
),
-- 销售毛利率排名
sale_rank as 
(
  select 
    business_type_code,
    -- 业务类型
    business_type_name,  	
    -- 大区编码																			                              
    region_code,	
     -- 大区名称																					                              
    region_name,																						                             
    province_code,                           
    province_name,                           
    current_period_sales_value,
    current_period_profit,
     -- 本期毛利率
    current_period_profit / abs(current_period_sales_value) as current_period_profit_prorate,															                         
    last_period_sales_value,                          
    last_period_profit,  
     -- 上期毛利率                        
    coalesce(last_period_profit / abs(last_period_sales_value), 0) as last_period_profit_prorate,                                                 
    current_period_business_sales_value,
    current_period_business_profit,
     -- 本期业务毛利率
    current_period_business_profit / abs(current_period_business_sales_value) as current_period_business_profit_prorate,			 				 
    current_period_area_sales_value,                         
    last_period_area_sales_value,
    -- 本期毛利率Rank排名						         
    rank() over(partition by business_type_code order by round(current_period_profit/abs(current_period_sales_value),6) desc) as profit_prorate_rank,
    -- 本期毛利排名行号                
    row_number() over(partition by business_type_code order by round(current_period_profit/abs(current_period_sales_value),6) desc) as profit_prorate_rank_no       
  from business_area_sale
),
-- 毛利率top1省区业务
top_sale as 
(
  select 
    business_type_code,
    province_code,  
    province_name, 
    current_period_sales_value,
    current_period_profit,
    current_period_profit_prorate
  from sale_rank 
  where profit_prorate_rank = 1 and profit_prorate_rank_no = 1
)
insert overwrite table ${hiveconf:target_table} partition(month) 
select 
  concat_ws('&', substr(${hiveconf:current_day}, 1, 6), t1.business_type_code, t1.province_code) as biz_id,
   -- 业务类型
  t1.business_type_code,
  t1.business_type_name,  
  -- 大区编码																				    
  t1.region_code,	
  -- 大区名称																					     
  t1.region_name,																						     
  t1.province_code,  
  t1.province_name,  
  t1.current_period_sales_value,
  t1.current_period_profit,
  round(t1.current_period_profit_prorate, 6) as current_period_profit_prorate,
  t1.last_period_sales_value,
  t1.last_period_profit,
  round(t1.last_period_profit_prorate, 6) as last_period_profit_prorate,
  -- 环比毛利率增长
  round(t1.current_period_profit_prorate - t1.last_period_profit_prorate, 6) as  period_profit_prorate_diff,
  -- 本期业务销售占比
  round(t1.current_period_sales_value / abs(t1.current_period_area_sales_value), 6) as current_business_prorate,
  -- 上期业务销售占比
  round(t1.last_period_sales_value / abs(t1.last_period_area_sales_value), 6) as last_business_prorate,
  -- 业务销售占比环比增长
  round(t1.current_period_sales_value / abs(t1.current_period_area_sales_value) - t1.last_period_sales_value / abs(t1.last_period_area_sales_value), 6) as period_business_prorate_diff,
  -- 本期业务总销售额
  t1.current_period_business_sales_value,
  -- 本期业务总毛利
  t1.current_period_business_profit,
  -- 本期业务总毛利率
  round(t1.current_period_business_profit_prorate, 6) as current_period_business_profit_prorate,
  -- 本期毛利率最高省区
  t2.province_name as top1_sale_province,
  -- 本期最高毛利率
  round(t2.current_period_profit_prorate, 6) as top1_profit_prorate,
  -- 本期毛利率排名
  t1.profit_prorate_rank,
  -- 当前月份排名
  substr(${hiveconf:current_day},1,6) as month
from sale_rank t1 
join top_sale t2 on t1.business_type_code = t2.business_type_code;