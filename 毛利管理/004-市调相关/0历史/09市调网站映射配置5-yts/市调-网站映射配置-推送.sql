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



-- --------------------------------------------推送 ----------------------------------------------------------------------------------------
-- 通用市调网站映射配置表
insert overwrite table csx_analyse.csx_analyse_fr_ts_market_common_web_ref
select 
warehouse_code,
warehouse_name,
product_code,
product_name,
product_region_name,
product_unit,
market_code,
market_name,
big_management_classify_name,
big_management_classify_code,
mid_management_classify_name,
mid_management_classify_code,
small_management_classify_name,
small_management_classify_code,
regexp_replace(market_unique_key,'\n|\t|\r|\,|\"|\\\\n','') as market_unique_key,
regexp_replace(market_product_name,'\n|\t|\r|\,|\"|\\\\n','') as market_product_name,
market_unit,
market_spec,
change_rate,
float_money,
case 
when status=0 then '禁用'
when status=1 then '启用'
when status=2 then '已删除'
else status end as status,
update_by,
update_time as update_time0,
from_utc_timestamp(current_timestamp(),'GMT') update_time
from csx_ods.csx_ods_csx_price_prod_market_common_web_ref_config_df 
-- where sdt=regexp_replace(add_months(date_sub(current_date,1),0),'-','')
where sdt='${sdt_yes}'
and status=1;


-- 客户市调网站映射配置表
insert overwrite table csx_analyse.csx_analyse_fr_ts_market_customer_web_ref
select
warehouse_code,
warehouse_name,
customer_code,
customer_name,
product_code,
product_name,
product_region_name,
product_unit,
market_code,
market_name,
big_management_classify_name,
big_management_classify_code,
mid_management_classify_name,
mid_management_classify_code,
small_management_classify_name,
small_management_classify_code,
regexp_replace(market_unique_key,'\n|\t|\r|\,|\"|\\\\n','') as market_unique_key,
regexp_replace(market_product_name,'\n|\t|\r|\,|\"|\\\\n','') as market_product_name,
market_unit,
market_spec,
change_rate,
float_money,
case 
when status=0 then '禁用'
when status=1 then '启用'
when status=2 then '已删除'
else status end as status,
update_by,
update_time as update_time0,
from_utc_timestamp(current_timestamp(),'GMT') update_time
from csx_ods.csx_ods_csx_price_prod_market_customer_web_ref_config_df 
-- where sdt=regexp_replace(add_months(date_sub(current_date,1),0),'-','')
where sdt='${sdt_yes}'
and status=1;



--hive 通用市调网站映射配置表
drop table if exists csx_analyse.csx_analyse_fr_ts_market_common_web_ref;
create table csx_analyse.csx_analyse_fr_ts_market_common_web_ref(
`warehouse_code`	string	COMMENT	'库存地点编码',
`warehouse_name`	string	COMMENT	'库存地点名称',
`product_code`	string	COMMENT	'商品编码',
`product_name`	string	COMMENT	'商品名称',
`product_region_name`	string	COMMENT	'商品区域化名称',
`product_unit`	string	COMMENT	'商品单位',
`market_code`	string	COMMENT	'市调对象编码',
`market_name`	string	COMMENT	'市调对象名称',
`big_management_classify_name`	string	COMMENT	'管理大类名称',
`big_management_classify_code`	string	COMMENT	'管理大类编码',
`mid_management_classify_name`	string	COMMENT	'管理中类名称',
`mid_management_classify_code`	string	COMMENT	'管理中类编码',
`small_management_classify_name`	string	COMMENT	'管理小类名称',
`small_management_classify_code`	string	COMMENT	'管理小类编码',
`market_unique_key`	string	COMMENT	'市调唯一标识',
`market_product_name`	string	COMMENT	'市调商品名称',
`market_unit`	string	COMMENT	'市调商品单位',
`market_spec`	string	COMMENT	'市调商品规格',
`change_rate`	decimal(20,6)	COMMENT	'转化系数',
`float_money`	decimal(20,6)	COMMENT	'浮动金额',
`status`	string	COMMENT	'状态',
`update_by`	string	COMMENT	'更新人',
`update_time0`	string	COMMENT	'更新时间',
`update_time`	string	COMMENT	'报表更新时间'
) COMMENT '通用市调网站映射配置表'
;

--hive 客户市调网站映射配置表
drop table if exists csx_analyse.csx_analyse_fr_ts_market_customer_web_ref;
create table csx_analyse.csx_analyse_fr_ts_market_customer_web_ref(
`warehouse_code`	string	COMMENT	'库存地点编码',
`warehouse_name`	string	COMMENT	'库存地点名称',
`customer_code`	string	COMMENT	'客户编码',
`customer_name`	string	COMMENT	'客户名称',
`product_code`	string	COMMENT	'商品编码',
`product_name`	string	COMMENT	'商品名称',
`product_region_name`	string	COMMENT	'商品区域化名称',
`product_unit`	string	COMMENT	'商品单位',
`market_code`	string	COMMENT	'市调对象编码',
`market_name`	string	COMMENT	'市调对象名称',
`big_management_classify_name`	string	COMMENT	'管理大类名称',
`big_management_classify_code`	string	COMMENT	'管理大类编码',
`mid_management_classify_name`	string	COMMENT	'管理中类名称',
`mid_management_classify_code`	string	COMMENT	'管理中类编码',
`small_management_classify_name`	string	COMMENT	'管理小类名称',
`small_management_classify_code`	string	COMMENT	'管理小类编码',
`market_unique_key`	string	COMMENT	'市调唯一标识',
`market_product_name`	string	COMMENT	'市调商品名称',
`market_unit`	string	COMMENT	'市调商品单位',
`market_spec`	string	COMMENT	'市调商品规格',
`change_rate`	decimal(20,6)	COMMENT	'转化系数',
`float_money`	decimal(20,6)	COMMENT	'浮动金额',
`status`	string	COMMENT	'状态',
`update_by`	string	COMMENT	'更新人',
`update_time0`	string	COMMENT	'更新时间',
`update_time`	string	COMMENT	'报表更新时间'
) COMMENT '客户市调网站映射配置表'
;
