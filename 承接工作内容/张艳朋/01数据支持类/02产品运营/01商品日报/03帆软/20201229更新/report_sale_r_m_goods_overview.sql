-- 商品简报 全国大客户日配商品板块业绩概览（MTD）
-- 核心逻辑： 统计商品的销售概况

-- 切换tez计算引擎
set mapred.job.name=report_sale_r_m_goods_overview;
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

-- 计算日期
set current_day = regexp_replace(date_sub(current_date, 1), '-', '');

-- 当月第一天
set current_start_day = regexp_replace(trunc(date_sub(current_date, 1), 'MM'), '-', '');

-- b端客户月统计销售表
set source_sale_detail=csx_dw.dws_sale_r_d_detail;

-- 目标表
set target_table=csx_dw.report_sale_r_m_goods_overview;

with current_sale as 
(
  select 
    region_name,
    city_group_name,
    sum(sales_value) as total_sales_value,								-- 所有品类商品销售额
    sum(profit) as total_profit,  										-- 所有品类商品毛利
	count(distinct customer_no) as customer_amount,						-- 下单客户数
    count(distinct goods_code) as sku_amount,							-- 下单sku数
    -- 生鲜
    sum(case when division_code in ('10', '11') then sales_value else 0 end) as fresh_sales_value,
    sum(case when division_code in ('10', '11') then profit else 0 end) as fresh_profit,
    count(distinct case when division_code in ('10', '11') then customer_no else null end) as fresh_customer_amount,
    count(distinct case when division_code in ('10', '11') then goods_code else null end) as fresh_sku_amount,
    -- 食百
    sum(case when division_code in ('12', '13', '14', '15') then sales_value else 0 end) as shibai_sales_value,
    sum(case when division_code in ('12', '13', '14', '15') then profit else 0 end) as shibai_profit,
    count(distinct case when division_code in ('12', '13', '14', '15') then customer_no else null end) as shibai_customer_amount,
    count(distinct case when division_code in ('12', '13', '14', '15') then goods_code else null end) as shibai_sku_amount,
    -- 非食品
    sum(case when division_code in ('13', '14', '15') then sales_value else 0 end) as not_food_sales_value,
    sum(case when division_code in ('13', '14', '15') then profit else 0 end) as not_food_profit,
    count(distinct case when division_code in ('13', '14', '15') then customer_no else null end) as not_food_customer_amount,
    count(distinct case when division_code in ('13', '14', '15') then goods_code else null end) as not_food_sku_amount,
    -- 食品
    sum(case when division_code in ('12') then sales_value else 0 end) as food_sales_value,
    sum(case when division_code in ('12') then profit else 0 end) as food_profit,
    count(distinct case when division_code in ('12') then customer_no else null end) as food_customer_amount,
    count(distinct case when division_code in ('12') then goods_code else null end) as food_sku_amount
  from ${hiveconf:source_sale_detail} 
  where sdt >= ${hiveconf:current_start_day} and sdt <= ${hiveconf:current_day}
    and channel_code in ('1','9') -- -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
	and business_type_code !=4 -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
  group by region_name, city_group_name
  union all 
  select 
    '全国' as region_name,
    '全国' as city_group_name,
    sum(sales_value) as total_sales_value,								-- 所有品类商品销售额
    sum(profit) as total_profit,  										-- 所有品类商品毛利
	count(distinct customer_no) as customer_amount,						-- 下单客户数
    count(distinct goods_code) as sku_amount,							-- 下单sku数
    -- 生鲜
    sum(case when division_code in ('10', '11') then sales_value else 0 end) as fresh_sales_value,
    sum(case when division_code in ('10', '11') then profit else 0 end) as fresh_profit,
    count(distinct case when division_code in ('10', '11') then customer_no else null end) as fresh_customer_amount,
    count(distinct case when division_code in ('10', '11') then goods_code else null end) as fresh_sku_amount,
    -- 食百
    sum(case when division_code in ('12', '13', '14', '15') then sales_value else 0 end) as shibai_sales_value,
    sum(case when division_code in ('12', '13', '14', '15') then profit else 0 end) as shibai_profit,
    count(distinct case when division_code in ('12', '13', '14', '15') then customer_no else null end) as shibai_customer_amount,
    count(distinct case when division_code in ('12', '13', '14', '15') then goods_code else null end) as shibai_sku_amount,
    -- 非食品
    sum(case when division_code in ('13', '14', '15') then sales_value else 0 end) as not_food_sales_value,
    sum(case when division_code in ('13', '14', '15') then profit else 0 end) as not_food_profit,
    count(distinct case when division_code in ('13', '14', '15') then customer_no else null end) as not_food_customer_amount,
    count(distinct case when division_code in ('13', '14', '15') then goods_code else null end) as not_food_sku_amount,
    -- 食品
    sum(case when division_code in ('12') then sales_value else 0 end) as food_sales_value,
    sum(case when division_code in ('12') then profit else 0 end) as food_profit,
    count(distinct case when division_code in ('12') then customer_no else null end) as food_customer_amount,
    count(distinct case when division_code in ('12') then goods_code else null end) as food_sku_amount
  from ${hiveconf:source_sale_detail} 
  where sdt >= ${hiveconf:current_start_day} and sdt <= ${hiveconf:current_day}
    and channel_code in ('1','9') -- -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
	and business_type_code !=4 -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
)
insert overwrite table ${hiveconf:target_table} partition(month) 
select 
  region_name,
  city_group_name,
  total_sales_value,  										  		-- 总销售额
  round(total_profit / abs(total_sales_value),8) as total_profit_prorate,	-- 总毛利率
  customer_amount,									  				-- 客户数量
  sku_amount,  														-- 客户下单sku数
  -- 生鲜
  fresh_sales_value,											  -- 生鲜销售额
  round(fresh_sales_value/total_sales_value,8) as fresh_sale_prorate,	  -- 生鲜销售额在总销售额中占比
  round(fresh_profit/abs(fresh_sales_value), 8) as fresh_profit_prorate,				  -- 生鲜毛利在总毛利中占比
  round(fresh_customer_amount/customer_amount, 8) as fresh_customer_prorate, -- 生鲜下单客户数占总下单客户数占比
  fresh_sku_amount, -- 客户下单生鲜sku数
  -- 食百
  shibai_sales_value,
  round(shibai_sales_value/total_sales_value, 8) as shibai_sale_prorate,
  round(shibai_profit/abs(shibai_sales_value), 8) as shibai_profit_prorate,
  round(shibai_customer_amount/customer_amount, 8) as shibai_customer_prorate,
  -- 非食品
  not_food_sales_value,
  round(not_food_profit/abs(not_food_sales_value), 8) as not_food_profit_prorate,
  round(not_food_customer_amount/customer_amount, 8) as not_food_customer_prorate,
  not_food_sku_amount,
  -- 食品类
  food_sales_value,
  round(food_profit/abs(food_sales_value), 8) as food_profit_prorate,
  round(food_customer_amount/customer_amount, 8) as food_customer_prorate,
  food_sku_amount,
  substr(${hiveconf:current_day}, 1, 6) as month
from current_sale 
;