-- csx_ods_csx_price_prod_market_common_web_ref_config_df(通用市调网站映射配置表)
-- csx_ods_csx_price_prod_market_customer_web_ref_config_df(客户市调网站映射配置表)

-- 通用市调网站映射配置表
select 
warehouse_code as `库存地点编码`,
warehouse_name as `库存地点名称`,
product_code as `商品编码`,
product_name as `商品名称`,
product_region_name as `商品区域化名称`,
product_unit as `商品单位`,
market_code as `市调对象编码`,
market_name as `市调对象名称`,
big_management_classify_name as `管理大类名称`,
big_management_classify_code as `管理大类编码`,
mid_management_classify_name as `管理中类名称`,
mid_management_classify_code as `管理中类编码`,
small_management_classify_name as `管理小类名称`,
small_management_classify_code as `管理小类编码`,
regexp_replace(market_unique_key,'\n|\t|\r|\,|\"|\\\\n','') as `市调唯一标识`,
regexp_replace(market_product_name,'\n|\t|\r|\,|\"|\\\\n','') as `市调商品名称`,
market_unit as `市调商品单位`,
market_spec as `市调商品规格`,
change_rate as `转化系数`,
float_money as `浮动金额`,
case 
when status=0 then '禁用'
when status=1 then '启用'
when status=2 then '已删除'
else status end as `状态`,
update_by as `更新人`,
update_time as `更新时间`
from csx_ods.csx_ods_csx_price_prod_market_common_web_ref_config_df 
where sdt=regexp_replace(add_months(date_sub(current_date,1),0),'-','')
and status=1;


-- 客户市调网站映射配置表
select
warehouse_code as `库存地点编码`,
warehouse_name as `库存地点名称`,
customer_code as `客户编码`,
customer_name as `客户名称`,
product_code as `商品编码`,
product_name as `商品名称`,
product_region_name as `商品区域化名称`,
product_unit as `商品单位`,
market_code as `市调对象编码`,
market_name as `市调对象名称`,
big_management_classify_name as `管理大类名称`,
big_management_classify_code as `管理大类编码`,
mid_management_classify_name as `管理中类名称`,
mid_management_classify_code as `管理中类编码`,
small_management_classify_name as `管理小类名称`,
small_management_classify_code as `管理小类编码`,
regexp_replace(market_unique_key,'\n|\t|\r|\,|\"|\\\\n','') as `市调唯一标识`,
regexp_replace(market_product_name,'\n|\t|\r|\,|\"|\\\\n','') as `市调商品名称`,
market_unit as `市调商品单位`,
market_spec as `市调商品规格`,
change_rate as `转化系数`,
float_money as `浮动金额`,
case 
when status=0 then '禁用'
when status=1 then '启用'
when status=2 then '已删除'
else status end as `状态`,
update_by as `更新人`,
update_time as `更新时间`
from csx_ods.csx_ods_csx_price_prod_market_customer_web_ref_config_df 
where sdt=regexp_replace(add_months(date_sub(current_date,1),0),'-','')
and status=1;


