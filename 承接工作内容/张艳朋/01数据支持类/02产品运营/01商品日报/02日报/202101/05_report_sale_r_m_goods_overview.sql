-- 商品简报 全国大客户日配商品板块业绩概览（MTD）
-- 核心逻辑： 统计商品的销售概况

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
  from csx_dw.dws_sale_r_d_detail
  where sdt >= regexp_replace(trunc(date_sub(current_date, 1), 'MM'), '-', '') and sdt <= regexp_replace(date_sub(current_date, 1), '-', '')
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
  from csx_dw.dws_sale_r_d_detail
  where sdt >= regexp_replace(trunc(date_sub(current_date, 1), 'MM'), '-', '') and sdt <= regexp_replace(date_sub(current_date, 1), '-', '')
    and channel_code in ('1','9') -- -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
	and business_type_code !=4 -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
)

select 
  region_name,
  city_group_name,
  coalesce(total_sales_value,0) as total_sales_value,  							-- 总销售额
  coalesce(round(total_profit / abs(total_sales_value),8),0) as total_profit_prorate,	-- 总毛利率
  coalesce(customer_amount,0) as customer_amount,									  				-- 客户数量
  coalesce(sku_amount,0) as sku_amount,  														-- 客户下单sku数
  -- 生鲜
  coalesce(fresh_sales_value,0) as fresh_sales_value,											  -- 生鲜销售额
  coalesce(round(fresh_sales_value/total_sales_value,8),0) as fresh_sale_prorate,	  -- 生鲜销售额在总销售额中占比
  coalesce(round(fresh_profit/abs(fresh_sales_value), 8),0) as fresh_profit_prorate,				  -- 生鲜毛利在总毛利中占比
  coalesce(round(fresh_customer_amount/customer_amount, 8),0) as fresh_customer_prorate, -- 生鲜下单客户数占总下单客户数占比
  coalesce(fresh_sku_amount,0) as fresh_sku_amount, -- 客户下单生鲜sku数
  -- 食百
  coalesce(shibai_sales_value) as shibai_sales_value,
  coalesce(round(shibai_sales_value/total_sales_value, 8),0) as shibai_sale_prorate,
  coalesce(round(shibai_profit/abs(shibai_sales_value), 8),0) as shibai_profit_prorate,
  coalesce(round(shibai_customer_amount/customer_amount, 8),0) as shibai_customer_prorate,
  -- 非食品
  coalesce(not_food_sales_value,0) as not_food_sales_value,
  coalesce(round(not_food_profit/abs(not_food_sales_value), 8),0) as not_food_profit_prorate,
  coalesce(round(not_food_customer_amount/customer_amount, 8),0) as not_food_customer_prorate,
  coalesce(not_food_sku_amount,0) as not_food_sku_amount,
  -- 食品类
  coalesce(food_sales_value,0) as food_sales_value,
  coalesce(round(food_profit/abs(food_sales_value), 8),0) as food_profit_prorate,
  coalesce(round(food_customer_amount/customer_amount, 8),0) as food_customer_prorate,
  coalesce(food_sku_amount,0) as food_sku_amount
from current_sale 
;