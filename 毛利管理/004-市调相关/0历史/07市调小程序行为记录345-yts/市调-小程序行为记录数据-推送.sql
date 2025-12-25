-- 市调-小程序行为记录数据 (通用市调)
-- 历史累计小程序行为数据关联昨日通用市调价格

insert overwrite table csx_analyse.csx_analyse_fr_applet_common_market_action_price_detail_df
select a.*,
b.market_research_price,
b.estimated_pricing_gross_margin,
from_utc_timestamp(current_timestamp(),'GMT') update_time
from
(
select 
warehouse_code,
warehouse_name,   -- 库存地点名称
product_code,   -- 商品编码
product_name,   -- 商品名称
product_unit,   -- 商品单位
big_management_classify_code,   -- 管理大类编码
big_management_classify_name,   -- 管理大类名称
mid_management_classify_code,   -- 管理中类编码
mid_management_classify_name,   -- 管理中类名称
small_management_classify_code,   -- 管理小类编码
small_management_classify_name,   -- 管理小类名称
market_code,   -- 市调对象编码
market_name,   -- 市调对象名称
offline_product_name,   -- 线下商品名称
offline_spec,   -- 线下商品规格
case 
when source=0 then '添加'
when source=1 then '导入'
when source=2 then '小程序'
end as source_name,   -- 来源：0:添加;1:导入;2:小程序
case 
when status=0 then '禁用'
when status=1 then '启用'
when status=2 then '已删除'
end as status_name,   -- 状态:1启用 0禁用 2已删除
-- create_by,   -- 创建人
-- create_time,   -- 创建时间
update_by,   -- 更新人
update_time as update_time_0  -- 更新时间
from csx_dwd.csx_dwd_csx_price_prod_market_common_market_action_product_di
-- where sdt='${sdt_yes}'
where status=1
and source=2
)a 
-- 昨日市调最新数据
left join
(
select
	performance_province_name,performance_city_name,location_code,market_source_type_name,shop_code,shop_name,product_code,product_name,price as market_research_price,
	min_price,max_price,price_begin_time,price_end_time,create_date,a.create_by,a.remark,a.unit_name,a.estimated_pricing_gross_margin,
	a.one_product_category_code,a.one_product_category_name,a.two_product_category_code,a.two_product_category_name,a.update_time
from
	(
	select 
		c.performance_province_name,
		c.performance_city_name,
		b.location_code,
		(case when a.source_type_code=2 then '网站' 
			  when a.source_type_code=3 then '批发市场' 
			  when a.source_type_code=4 then '一批' 
			  when a.source_type_code=5 then '二批' 
			  when a.source_type_code=6 then '终端' end) as market_source_type_name,
		a.shop_code,
		--a.shop_name,
		regexp_replace(a.shop_name,'\n|\t|\r|\,|\"|\\\\n','') as shop_name, 
		b.product_code,
		--b.product_name,
		regexp_replace(b.product_name,'\n|\t|\r|\,|\"|\\\\n','') as product_name, 
		a.price,
		a.min_price,
		a.max_price,
		a.price_begin_time,
		a.price_end_time,
		to_date(a.create_time) as create_date,
		row_number()over(partition by b.location_code,b.product_code,a.shop_code order by a.create_time desc) as rn,
		a.create_by,
		a.remark,
		d.unit_name,
		a.estimated_pricing_gross_margin,
		b.one_product_category_code,
		b.one_product_category_name,
		b.two_product_category_code,
		b.two_product_category_name,
		a.update_time	
	from 
		(
		select 
			* 
		from 
			(
			select
				source_type_code,shop_code,shop_name,price,min_price,max_price,price_begin_time,price_end_time,create_time,create_by,remark,
				estimated_pricing_gross_margin,goods_id as product_id,update_time
			from	
				csx_dwd.csx_dwd_price_market_research_not_yh_price_effective_di -- 非永辉 生效
			where 
				sdt='${sdt_yes}'
				and source_type_code!=1	
			) tmp
		) a 
		left join (select * from csx_ods.csx_ods_csx_price_prod_market_research_product_df where sdt='${sdt_yes}') b on a.product_id=b.id 
		left join (select * from csx_dim.csx_dim_shop where sdt='current') c on c.shop_code=b.location_code
		left join (select * from csx_dim.csx_dim_basic_goods where sdt='current')d on d.goods_code=b.product_code
	) a 
where
	rn=1
)b on a.warehouse_code=b.location_code and a.product_code=b.product_code and a.market_code=b.shop_code;




--hive 小程序行为数据关联昨日通用市调价格
drop table if exists csx_analyse.csx_analyse_fr_applet_common_market_action_price_detail_df;
create table csx_analyse.csx_analyse_fr_applet_common_market_action_price_detail_df(
`warehouse_code`	string	COMMENT	'库存地点编码',
`warehouse_name`	string	COMMENT	'库存地点名称',
`product_code`	string	COMMENT	'商品编码',
`product_name`	string	COMMENT	'商品名称',
`product_unit`	string	COMMENT	'基础单位',
`big_management_classify_code`	string	COMMENT	'管理品类编码',
`big_management_classify_name`	string	COMMENT	'管理品类名称',
`mid_management_classify_code`	string	COMMENT	'管理品类-中类编码',
`mid_management_classify_name`	string	COMMENT	'管理品类-中类名称',
`small_management_classify_code`	string	COMMENT	'管理品类-小类编码',
`small_management_classify_name`	string	COMMENT	'管理品类-小类名称',
`market_code`	string	COMMENT	'市调对象编码',
`market_name`	string	COMMENT	'市调对象名称',
`offline_product_name`	string	COMMENT	'线下商品名称',
`offline_spec`	string	COMMENT	'线下商品规格',
`source_name`	string	COMMENT	'来源类型名称',
`status_name`	string	COMMENT	'状态',
`update_by`	string	COMMENT	'更新人',
`update_time_0`	string	COMMENT	'更新时间',
`market_research_price`	decimal(20,6)	COMMENT	'市调价格',
`estimated_pricing_gross_margin`	decimal(20,6)	COMMENT	'预估定价毛利率=（市调价-库存平均价）/市调价',
`update_time`	string	COMMENT	'报表更新时间'
) COMMENT '小程序行为数据关联昨日通用市调价格'
;

