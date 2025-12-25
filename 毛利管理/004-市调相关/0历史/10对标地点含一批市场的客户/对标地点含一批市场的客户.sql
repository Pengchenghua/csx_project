select
	b.performance_region_name,     --  销售大区名称(业绩划分)
	b.performance_province_name,     --  销售归属省区名称
	b.performance_city_name,     --  城市组名称(业绩划分)
	a.*
from 
(
select 
customer_code,	-- 客户编码
customer_name,	-- 客户名称
warehouse_code,	-- 库存地点编码
warehouse_name,	-- 库存地点名称
-- big_management_classify_code,	-- 管理品类编码
-- big_management_classify_name,	-- 管理品类名称
-- mid_management_classify_code,	-- 管理品类-中类编码
-- mid_management_classify_name,	-- 管理品类-中类名称
-- small_management_classify_code,	-- 管理品类-小类编码
-- small_management_classify_name,	-- 管理品类-小类名称
-- price_type,	-- 定价类型(1-建议售价 2 -  对标对象  3 -销售成本价 4-上一周价格 5-售价 6-采购/库存成本)
case price_type
when 1 then '建议售价'
when 2 then '对标对象'
when 3 then '销售成本价'
when 4 then '上一周价格'
when 5 then '售价'
when 6 then '采购/库存成本' end as price_type,
addition_rate,	-- 加成系数
-- bmk_type,	-- 对标类型(0 - 永辉门店 1 - 网站 2 - 市场 3-终端)
case bmk_type
when 0 then '永辉门店'
when 1 then '网站'
when 2 then '市场'
when 3 then '终端' end as bmk_type,
bmk_code,	-- 对标对象编码
bmk_name,	-- 对标对象名称(描述)
-- is_disable,	-- 是否禁用  (1 - 已禁用)
dimension_value_code,	-- 商品或分类编码
regexp_replace(regexp_replace(dimension_value_name,'\n',''),'\r','') as dimension_value_name,	-- 商品或分类名称
-- dimension_type,	-- 商品 =0 小类 =1 中类 =2 大类 =3
case dimension_type
when 0 then '商品'
when 1 then '小类'
when 2 then '中类'
when 3 then '大类' end as dimension_type,
float_up_rate,	-- 售价类型:上浮点数
float_down_rate,	-- 售价类型:下浮点数
float_amount,	-- 浮动金额
-- float_type,	-- 0-比例系数 1-浮动金额
case float_type
when 0 then '比例系数'
when 1 then '浮动金额' end as float_type,
second_config,	-- (客户报价二级策略)
-- suggest_price_type,	-- 建议售价类型: 1-高;2:中;3:低
case suggest_price_type
when 1 then '高'
when 2 then '中'
when 3 then '低' end as suggest_price_type,
-- bmk_price_type,	-- 对标网站、市场时市调价取值类型: 1: 最高价 2:最低价 3：平均价
case bmk_price_type
when 1 then '最高价'
when 2 then '最低价'
when 3 then '平均价' end as bmk_price_type,
-- is_sensitive,	-- 商品维度为商品时是否为敏感商品(1-敏感商品)
case when is_sensitive=1 then '是' else '否' end as is_sensitive,
-- is_fix_price	-- 是否固定价(1-固定价)
case when is_fix_price=1 then '是' else '否' end as is_fix_price
from csx_ods.csx_ods_csx_price_prod_customer_price_guide_config_df
where sdt=regexp_replace(date_sub(current_date,1),'-','')
and bmk_code like 'Y%'
and is_disable<>1
)a 
left join 
(
select 
	performance_region_name,     --  销售大区名称(业绩划分)
	performance_province_name,     --  销售归属省区名称
	performance_city_name,     --  城市组名称(业绩划分)
	channel_name,
	customer_code,
	customer_name,     --  客户名称
	-- first_category_code,     --  一级客户分类编码
	first_category_name,     --  一级客户分类名称
	-- second_category_code,     --  二级客户分类编码
	second_category_name,     --  二级客户分类名称
	-- third_category_code,     --  三级客户分类编码
	third_category_name     --  三级客户分类名称
from csx_dim.csx_dim_crm_customer_info
where sdt='current'
and customer_type_code=4
) b on a.customer_code=b.customer_code
;










