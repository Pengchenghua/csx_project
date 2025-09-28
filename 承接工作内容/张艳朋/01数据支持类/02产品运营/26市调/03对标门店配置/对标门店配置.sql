select
	id as '主键',
	product_id as '市调商品ID',
	source_type_code  as '来源类型编码：1-永辉门店，2-网站，3-批发市场',
	source_type_name as '来源类型名称',
	shop_code as '对标门店编码',
	shop_name as '对标门店名称',
	status as '状态：0-禁用，1-启用',
	default_local as '是否默认门店',
	remark as '备注',
	create_time as '创建时间',
	create_by as '创建人',
	update_time as '更新时间',
	update_by as '更新人',
	yunchao_product_code as '云超商品编码',
	yunchao_product_name as '云超商品名称',
from
	market_research_source
where
	status=1
	
select
	id as '主键',
	product_code as '商品编码',
	product_name as '商品名称',
	location_level as '地点层级(1-地点,2-采购组织)',
	location_code as '地点编码',
	location_name as '地点名称',
	purchase_org_code as '采购组织编码',
	purchase_org_name as '采购组织名称',
	purchase_group_code as '采购组编码',
	purchase_group_name as '采购组名称',
	root_category_code as '部类编码',
	root_category_name as '部类名称',
	big_category_code as '大类编码',
	big_category_name as '大类名称',
	mid_category_code as '中类编码',
	mid_category_name as '中类名称',
	small_category_code as '小类编码',
	small_category_name as '小类名称',
	remark as '备注',
	create_time as '创建时间',
	create_by as '创建人',
	update_time as '更新时间',
	update_by as '更新人',
	one_product_category_code as '一级品类编码',
	one_product_category_name as '一级品类名称',
	two_product_category_code as '二级品类编码',
	two_product_category_name as '二级品类名称',
	three_product_category_code as '三级品类编码',
	three_product_category_name as '三级品类名称'
from	
	market_research_product
;

-- =======================================================================================================================================================================
select
	a.id as '主键',
	a.product_id as '市调商品ID',
	a.source_type_code  as '来源类型编码：1-永辉门店，2-网站，3-批发市场',
	a.source_type_name as '来源类型名称',
	a.shop_code as '对标门店编码',
	a.shop_name as '对标门店名称',
	a.status as '状态：0-禁用，1-启用',
	a.default_local as '是否默认门店',
	a.remark as '备注',
	a.create_time as '创建时间',
	a.create_by as '创建人',
	a.update_time as '更新时间',
	a.update_by as '更新人',
	a.yunchao_product_code as '云超商品编码',
	a.yunchao_product_name as '云超商品名称',
	b.product_code as '商品编码',
	b.product_name as '商品名称',
	b.location_level as '地点层级(1-地点,2-采购组织)',
	b.location_code as '地点编码',
	b.location_name as '地点名称',
	b.purchase_org_code as '采购组织编码',
	b.purchase_org_name as '采购组织名称',
	b.purchase_group_code as '采购组编码',
	b.purchase_group_name as '采购组名称',
	b.root_category_code as '部类编码',
	b.root_category_name as '部类名称',
	b.big_category_code as '大类编码',
	b.big_category_name as '大类名称',
	b.mid_category_code as '中类编码',
	b.mid_category_name as '中类名称',
	b.small_category_code as '小类编码',
	b.small_category_name as '小类名称',
	b.remark as '备注',
	b.create_time as '创建时间',
	b.create_by as '创建人',
	b.update_time as '更新时间',
	b.update_by as '更新人',
	b.one_product_category_code as '一级品类编码',
	b.one_product_category_name as '一级品类名称',
	b.two_product_category_code as '二级品类编码',
	b.two_product_category_name as '二级品类名称',
	b.three_product_category_code as '三级品类编码',
	b.three_product_category_name as '三级品类名称'
from
	market_research_source a 
	left join market_research_product b on b.id=a.product_id
where
	a.status=1
	
