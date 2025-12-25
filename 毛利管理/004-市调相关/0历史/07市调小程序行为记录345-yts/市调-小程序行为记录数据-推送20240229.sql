-- 市调-小程序行为记录数据 (通用市调)
-- 历史累计小程序行为数据关联昨日通用市调价格

insert overwrite table csx_analyse.csx_analyse_fr_applet_common_market_action_price_detail_df
select a.*,
b.market_research_price,
b.estimated_pricing_gross_margin,
from_utc_timestamp(current_timestamp(),'GMT') update_time,
b.product_status, -- 商品状态:1促销 0正常
-- 供应链周期进价生效区平均值 
c.purchase_price_avg,
-- 取库存平均价
d.stock_price_avg,
-- 生鲜近7天食百近1个月最近一次入库价
e.price as price_last_stock,
-- 生效采购报价
f.purchase_price
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
-- 本周市调最新数据  
left join
(
select
	performance_province_name,performance_city_name,location_code,market_source_type_name,shop_code,shop_name,product_code,product_name,price as market_research_price,
	min_price,max_price,price_begin_time,price_end_time,create_date,a.create_by,a.remark,a.unit_name,a.estimated_pricing_gross_margin,a.product_status,
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
		a.product_status, -- 商品状态:1促销 0正常
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
				source, -- 来源: 0:市调导入；1:小程序;2:小程序-pc端 3: 线上网站市调
				case product_status when 0 then '正常' when 1 then '促销' end as product_status, -- 商品状态:1促销 0正常
				bom, -- bom配置
				bom_type, -- bom类型(1:人工bom;2:报价策略人工bom;3:工厂bom;4:报价策略工厂bom)
				source_type_code,shop_code,shop_name,price,min_price,max_price,price_begin_time,price_end_time,create_time,create_by,remark,
				estimated_pricing_gross_margin,product_id,update_time
			from csx_ods.csx_ods_csx_price_prod_market_research_not_yh_price_effective_df  -- 非永辉 生效
			where source_type_code!=1
			
			-- -- 此表数仓同步的有问题暂时切表
			-- select 
			-- 	source, -- 来源: 0:市调导入；1:小程序;2:小程序-pc端 3: 线上网站市调
			-- 	product_status, -- 商品状态:1促销 0正常
			-- 	bom, -- bom配置
			-- 	bom_type, -- bom类型(1:人工bom;2:报价策略人工bom;3:工厂bom;4:报价策略工厂bom)
			-- 	source_type_code,shop_code,shop_name,price,min_price,max_price,price_begin_time,price_end_time,create_time,create_by,remark,
			-- 	estimated_pricing_gross_margin,goods_id as product_id,update_time
			-- from csx_dwd.csx_dwd_price_market_research_not_yh_price_effective_di -- 非永辉 生效  
			-- where sdt='${sdt_yes}'
			-- and source_type_code!=1		
			) tmp
		)a 
		left join 
		(
		select * from csx_ods.csx_ods_csx_price_prod_market_research_product_df 
		-- where sdt='${sdt_yes}'
		-- 本周市调价 本周一开始
		where sdt>=regexp_replace(date_sub(current_date,dayofweek(current_date)-2),'-','')
		)b on a.product_id=b.id 
		left join (select * from csx_dim.csx_dim_shop where sdt='current') c on c.shop_code=b.location_code
		left join (select * from csx_dim.csx_dim_basic_goods where sdt='current')d on d.goods_code=b.product_code
	) a 
where
	rn=1
)b on a.warehouse_code=b.location_code and a.product_code=b.product_code and a.market_code=b.shop_code		
-- 供应链周期进价生效区平均值 
left join
(
select 
	location_code,product_code,
	avg(purchase_price) as purchase_price_avg
from csx_ods.csx_ods_csx_b2b_scm_scm_product_purchase_cycle_price_df  
where sdt='${sdt_yes}' 
and cycle_price_status=0 
group by location_code,product_code
)c on a.warehouse_code=c.location_code and a.product_code=c.product_code
-- 取库存平均价
left join
(
select
	-- shipper_code,
	dc_code,
	goods_code,
	sum(amt) as amt,
	sum(qty) as qty,
	sum(amt_no_tax) as amt_no_tax,
	sum(amt)/sum(qty) as stock_price_avg
from csx_dws.csx_dws_cas_accounting_stock_m_df
where sdt='${sdt_yes}'
and is_bz_reservoir = 1 
and qty > 0 
group by dc_code,goods_code
)d on a.warehouse_code=d.dc_code and a.product_code=d.goods_code
-- 生鲜近7天食百近1一个月最近一次入库价
left join
(
	select *
	from 
	(
		select a.*,
			row_number()over(partition by a.location_code,a.product_code order by a.update_time desc) as rn
		from csx_ods.csx_ods_csx_b2b_accounting_accounting_last_in_stock_df a
		left join 
		(
			select * 
			from csx_dim.csx_dim_basic_goods 
			where sdt='current' 
		) b on a.product_code=b.goods_code  
		where (
		(((b.business_division_name like '%生鲜%' and b.classify_middle_code='B0101') or  b.business_division_name like '%食百%') 
			and regexp_replace(substr(a.update_time,1,10),'-','')>=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-30),'-',''))
		or 
		(b.business_division_name like '%生鲜%' and (b.classify_middle_code<>'B0101' or b.classify_middle_code is null) 
			and regexp_replace(substr(a.update_time,1,10),'-','')>=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-7),'-',''))
		)
	)a 
	where rn=1	
)e on a.warehouse_code=e.location_code and a.product_code=e.product_code
-- 生效采购报价表 一天中间可能有新的采购报价生效致1天多条
left join
(
	select *
	from 
	(
		select 
			warehouse_code,
			product_code,
			purchase_price,
			row_number()over(partition by warehouse_code,product_code order by price_begin_time desc) as rn
		from csx_dwd.csx_dwd_price_effective_purchase_prices_di 
		where sdt>='${sdt_bf30d}'
		and normal_status=0  -- 正常 = 0 异常 = 1
		and base_product_status=0  -- 0正常 3停售 6退场 7停购
		and regexp_replace(substr(price_end_time,1,10),'-','')>='${sdt_yes}'
		and regexp_replace(substr(price_begin_time,1,10),'-','')<='${sdt_yes}'
	)a 
	where rn=1
)f on a.warehouse_code=f.warehouse_code and a.product_code=f.product_code
;




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






			
			
			







周期进价表的用法如下：
select 
    a.location_code as `地点编码`,
    a.location_name as `地点名称`,
    b.classify_large_name as `管理大类名称`,
    b.classify_middle_name as `管理中类名称`,
    b.classify_small_name as `管理小类名称`,
    a.product_code as `商品编码`,
    a.product_name as `商品名称`,
    a.supplier_code as `供应商编号`,
    a.supplier_name as `供应商名称`,
    a.purchase_group_code as `采购组编码`,
    a.purchase_group_name as `采购组名称`,
    a.purchase_org_code as `采购组织编码`,
    a.purchase_org_name as `采购组织名称`,
    a.unit as `单位`,
    a.spec as `规格`,
    a.purchase_price as `进价`,
    a.cycle_start_time as `开始生效时间`,
    a.cycle_end_time as `结束生效时间`,
    (case when a.cycle_price_status=0 then '生效' 
          when a.cycle_price_status=1 then '失效' 
    end) as `状态`,
    a.create_time as `创建时间`,
    a.create_by as `创建者`,
    a.update_time as `更新时间`,
    a.update_by as `更新者`,
    a.create_by_id as `创建者ID`,
    a.update_by_id as `更新者ID`,
    (case when a.cycle_price_source=1 then '手工维护' 
          when a.cycle_price_source=2 then '集采商品池' 
    end)as `来源` 
from 
(select * 
from csx_ods.csx_ods_csx_b2b_scm_scm_product_purchase_cycle_price_df  
where sdt='20231123' 
and cycle_price_status=0 
and location_code in ('W0A2','WA93') 
) a 
left join 
(select * 
from csx_dim.csx_dim_basic_goods 
where sdt='current' 
) b 
on a.product_code=b.goods_code

采购报价表：csx_dwd.csx_dwd_price_effective_purchase_prices_di 
 
select  
	id,shipper_code,shipper_name,location_code,location_name,
	product_code,product_name,qty,price,amt,amt_no_tax,
	price_no_tax,tax_rate,tax_code,create_time,
	create_by,update_time,update_by  
from csx_ods.csx_ods_csx_b2b_accounting_accounting_last_in_stock_df 
-- where  (product_code in ('1005') and location_code in ('W053') and shipper_code in ('YHCSX') and update_time >= '2023-06-28 00:00:00' and update_time <= '2023-07-05 23:59:59')
where substr(update_time,1,10)>=date_add(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'),-7-dayofweek(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd')))  
