-- 工厂表一般同步到hive用的t-0，其他表用的t-1
-- 人工bom
-- csx_ods.csx_ods_csx_price_prod_market_man_made_bom_config_df  (t-1)
-- csx_ods.csx_ods_csx_price_prod_market_man_made_bom_material_config_df (t-1)

-- 工厂bom csx_b2b_factory 关联字段bom_id
-- factory_setting_bom
-- factory_setting_bom_ability

-- csx_ods_csx_b2b_factory_factory_setting_bom_df(BOM表) (t-0)
-- csx_ods_csx_b2b_factory_factory_setting_bom_ability_df(BOM行项表) (t-0)
-- csx_ods_csx_b2b_factory_factory_setting_bom_craft_ability_df B端报价工艺路线 (t-1)
-- csx_dws_mms_factory_bom_m_df(bom宽表)



-- 人工bom时间用的t-1，工厂bom时间用的t-0
with 
man_made_bom as -- 人工bom
(
select
	'人工bom' as flag,
	a.id,
	a.warehouse_code as location_code,		-- 库存地点编码
	-- a.warehouse_name,		-- 库存地点名称
	'' as factory_location_code,
	'' as factory_location_name,
	a.product_code as goods_code,
	a.product_name as goods_name,
	a.product_unit as goods_unit,		-- 成品单位
	'' as goods_spec,
	null as calc_factor, -- 计算因子
	case 
	when a.status='0' then '禁用'
	when a.status='1' then '启用'
	when a.status='2' then '已删除'
	end as status,		-- 状态:1启用 0禁用 2已删除
	'' as goods_type_name,
	b.material_code as product_code,		-- 原料编码
	b.material_name as product_name,		-- 原料商品名称
	b.material_unit as product_unit,		-- 原料单位
	'' as product_spec, -- 规格
	b.num as product_num,		-- 所需数量
	b.lose_rate*0.01 as scrap_rate,		-- 损耗率
	b.processing_charge,		-- 加工费
	'' as mrp_prop_key,		-- MRP属性键
	'' as mrp_prop_value,		-- MRP属性值	
	null as class_number,  -- 配方:1-配方一 2-配方二 3-配方三
	-- FY001人工费用 FY002机器费用 FY003辅料费用
	null as fy_person,
	null as fy_machine,
	null as fy_accessories	
from 
(
select *
from csx_ods.csx_ods_csx_price_prod_market_man_made_bom_config_df
where sdt='20231130'
and status='1'  -- 启用
)a
left join 
(
select *
from csx_ods.csx_ods_csx_price_prod_market_man_made_bom_material_config_df
where sdt='20231130'
)b on a.id=b.man_made_bom_config_id
),

factory_setting_bom as -- 工厂bom
(
select
	'工厂bom' as flag,
	a.id,
	c.location_code,
	a.location_code as factory_location_code,
	a.location_name as factory_location_name,
	a.product_code as goods_code,
	a.product_name as goods_name,
	a.unit as goods_unit,
	a.spec as goods_spec,
	a.calc_factor, -- 计算因子
  	case 
	when a.status='0' then '禁用'
	when a.status='1' then '启用'
	when a.status='2' then '已删除'
	end as status,		-- 状态:1启用 0禁用 2已删除
	-- a.bom_type goods_type_code,
	case when a.bom_type ='1' then '组合型'
		when a.bom_type ='2' then '分解型'
		when a.bom_type ='3' then '粗加工型' 
		end as goods_type_name,
	b.product_code ,
	b.product_name,
	b.unit as product_unit,
	b.spec as product_spec, -- 规格	
	b.init_number as product_num,
	b.scrap_rate,
	null as processing_charge,		-- 加工费
	b.mrp_prop_key,		-- MRP属性键
	b.mrp_prop_value,		-- MRP属性值	
	d.class_number,  -- 配方:1-配方一 2-配方二 3-配方三
	-- FY001人工费用 FY002机器费用 FY003辅料费用
	d.fy_person,
	d.fy_machine,
	d.fy_accessories	
	
	-- cast(b.init_number/a.calc_factor as string) amount,
	-- cast(b.fact_number/a.calc_factor as string) amount1
from
(
  -- 1.1BOM
  select
    id,
    location_code,
    location_name,
    product_code,
    product_name,
    bom_type, -- 类型
    calc_factor, -- 计算因子
	status,
    unit,
    spec
  from  csx_ods.csx_ods_csx_b2b_factory_factory_setting_bom_df
  where sdt='20231201'
    and `status`='1' -- 启用
    and bom_type<>'2'
)a
  -- 1.2BOM子项
left outer join
(
  select
    bom_id,
    location_code,
    product_code,
    product_name,
    mrp_prop_key,
    mrp_prop_value,
    unit,
    spec,
	scrap_rate*0.01 as scrap_rate,
    `number` as init_number,
    `number`*(1+(scrap_rate*0.01)) fact_number
  from csx_ods.csx_ods_csx_b2b_factory_factory_setting_bom_ability_df
  where sdt='20231201' 
    -- and mrp_prop_key <>'3062' --不包含包装材料
)b on a.id=cast(b.bom_id as string) and a.location_code=b.location_code
left join 
(
select 'WB00' as factory_location_code,'W0R9' as location_code
union all select 'W0R8' as factory_location_code,'W0R9' as location_code
union all select 'W088' as factory_location_code,'W0A5' as location_code
union all select 'W0BZ' as factory_location_code,'W0A5' as location_code
union all select 'W0AR' as factory_location_code,'W0AS' as location_code
union all select 'W053' as factory_location_code,'W0A8' as location_code
union all select 'W039' as factory_location_code,'W0A7' as location_code
union all select 'W0AZ' as factory_location_code,'W0A7' as location_code
union all select 'W0X1' as factory_location_code,'W0X2' as location_code
union all select 'W079' as factory_location_code,'W0A6' as location_code
union all select 'W0S9' as factory_location_code,'W0Q2' as location_code
union all select 'WB04' as factory_location_code,'W0A3' as location_code
union all select 'W048' as factory_location_code,'W0A3' as location_code
union all select 'W0Q8' as factory_location_code,'W0Q9' as location_code
union all select 'W0BT' as factory_location_code,'W0BR' as location_code
union all select 'W080' as factory_location_code,'W0A2' as location_code

-- union all select 'WB82' as factory_location_code,'' as location_code
union all select 'WA93' as factory_location_code,'W0A2' as location_code
union all select 'W0D4' as factory_location_code,'W0A2' as location_code
-- union all select 'W0T3' as factory_location_code,'' as location_code
union all select 'W0E7' as factory_location_code,'W0A8' as location_code
union all select 'WB03' as factory_location_code,'W0A8' as location_code
union all select 'W0Q4' as factory_location_code,'W0BK' as location_code
union all select 'W0Q1' as factory_location_code,'W0Q2' as location_code
union all select 'W0T6' as factory_location_code,'W0Q9' as location_code
union all select 'W0BG' as factory_location_code,'W0BH' as location_code
union all select 'W0S2' as factory_location_code,'W0A5' as location_code
union all select 'W0R7' as factory_location_code,'W0R9' as location_code
union all select 'W0P6' as factory_location_code,'W0P8' as location_code
union all select 'WA99' as factory_location_code,'W0N1' as location_code
union all select 'WA94' as factory_location_code,'W0N1' as location_code
union all select 'W0K3' as factory_location_code,'W0N1' as location_code
union all select 'W0M6' as factory_location_code,'W0A6' as location_code
union all select 'W0P3' as factory_location_code,'W0N0' as location_code
union all select 'W0T0' as factory_location_code,'W0W7' as location_code
union all select 'W0T7' as factory_location_code,'W0A7' as location_code
union all select 'W0Z8' as factory_location_code,'W0Z9' as location_code
union all select 'WB98' as factory_location_code,'W0A7' as location_code
union all select 'WC56' as factory_location_code,'W0A7' as location_code

) c on a.location_code=c.factory_location_code
left join
(
select 
bom_id,
class_number,  -- 配方:1-配方一 2-配方二 3-配方三
-- FY001人工费用 FY002机器费用 FY003辅料费用
sum(case when craft_type='FY001' then number end) as fy_person,
sum(case when craft_type='FY002' then number end) as fy_machine,
sum(case when craft_type='FY003' then number end) as fy_accessories
from csx_ods.csx_ods_csx_b2b_factory_factory_setting_bom_craft_ability_df
where sdt='20231130'
group by bom_id,class_number
)d on a.id=cast(d.bom_id as string)
),

man_made_and_factory_setting_bom as -- 人工工厂bom
(
select *
from man_made_bom
union all
select a.*
from factory_setting_bom a 
left join man_made_bom b on a.location_code=b.location_code and a.goods_code=b.goods_code
where b.product_code is null
)
select *
from
(
select 
	a.flag,
	a.id,
	a.location_code,
	c.shop_name as location_name,
	a.factory_location_code,
	a.factory_location_name,
	a.goods_code,
	if(a.goods_name='',d.goods_name,regexp_replace(regexp_replace(a.goods_name,'\n',''),'\r','')) as goods_name,
	if(a.goods_unit='',d.unit_name,a.goods_unit) as goods_unit,
	if(a.goods_spec='',d.standard,a.goods_spec) as goods_spec,
	a.calc_factor, -- 计算因子
	a.status,		-- 状态:1启用 0禁用 2已删除
	a.goods_type_name,
	a.product_code ,
	if(a.product_name='',e.goods_name,regexp_replace(regexp_replace(a.product_name,'\n',''),'\r','')) as product_name,
	if(a.product_unit='',e.unit_name,a.product_unit) as product_unit,
	if(a.product_spec='',e.standard,a.product_spec) as product_spec,	-- 规格	
	a.product_num,
	a.scrap_rate,
	a.processing_charge,		-- 加工费
	a.mrp_prop_key,		-- MRP属性键
	a.mrp_prop_value,		-- MRP属性值	
	a.class_number,  -- 配方:1-配方一 2-配方二 3-配方三
	-- FY001人工费用 FY002机器费用 FY003辅料费用
	a.fy_person,
	a.fy_machine,
	a.fy_accessories	
from man_made_and_factory_setting_bom a
left join
(
  select 
    purchase_org,
    purchase_org_name,
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code, 
    performance_city_name, 	
    shop_code,
    shop_name,
    company_code,
    company_name,
    city_code,
    city_name,
    province_code,
    province_name,
    purpose,
    purpose_name
  from csx_dim.csx_dim_shop
  where sdt='current'
) c on a.location_code = c.shop_code
left join
(
    select
    goods_code,
    regexp_replace(regexp_replace(goods_name,'\n',''),'\r','') as goods_name,
    purchase_group_code as department_id,purchase_group_name as department_name,    
    classify_large_code,classify_large_name, -- 管理大类
    classify_middle_code,classify_middle_name,-- 管理中类
    classify_small_code,classify_small_name,-- 管理小类
	unit_name,standard
    from csx_dim.csx_dim_basic_goods
    where sdt = 'current'
)d on a.goods_code=d.goods_code
left join
(
    select
    goods_code,
    regexp_replace(regexp_replace(goods_name,'\n',''),'\r','') as goods_name,
    purchase_group_code as department_id,purchase_group_name as department_name,    
    classify_large_code,classify_large_name, -- 管理大类
    classify_middle_code,classify_middle_name,-- 管理中类
    classify_small_code,classify_small_name,-- 管理小类
	unit_name,standard
    from csx_dim.csx_dim_basic_goods
    where sdt = 'current'
)e on a.product_code=e.goods_code
where a.mrp_prop_value not like '%包装%' 
or length(a.mrp_prop_value)=0
)a
  where product_name not like '%真空袋%'
  and product_name not like '%包装袋%'
  and product_name not like '%空白袋%'
  and product_name not like '%涂抹袋%'
  and product_name not like '%套筐袋%'
  and product_name not like '%自立袋%'
  and product_name not like '%尼龙袋%'
  and product_name not like '%印刷袋%'
  and product_name not like '%中封袋%'
  and product_name not like '%复合袋%'
  and product_name not like '%阻隔盒%'
  and product_name not like '%圆盒%'
  and product_name not like '%蔬果盒%'
  and product_name not like '%餐盒%'
  and product_name not like '%托盒%'
  and product_name not like '%调料盒%'
  and product_name not like '%果盒王%'
;









