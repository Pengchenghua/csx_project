
insert overwrite table csx_analyse.csx_analyse_fr_price_market_research_price_detail_df
select a.*,
-- 供应链周期进价生效区平均值 
c.purchase_price_avg,
-- 取库存平均价
d.stock_price_avg,
-- 生鲜近7天食百近1个月最近一次入库价
e.price as price_last_stock,
-- 生效采购报价
f.purchase_price,
from_utc_timestamp(current_timestamp(),'GMT') update_time_b
from 
(
	select
		performance_province_name,performance_city_name,location_code,market_source_type_name,shop_code,shop_name,product_code,product_name,price as market_research_price,
		min_price,max_price,price_begin_time,price_end_time,create_date,a.create_by,a.remark,a.unit_name,a.estimated_pricing_gross_margin,
		a.one_product_category_code,a.one_product_category_name,a.two_product_category_code,a.two_product_category_name,a.update_time,
		a.source, -- 来源: 0:市调导入；1:小程序;2:小程序-pc端 3: 线上网站市调
		a.product_status, -- 商品状态:1促销 0正常
		a.bom, -- bom配置
		a.bom_type -- bom类型(1:人工bom;2:报价策略人工bom;3:工厂bom;4:报价策略工厂bom)			
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
			a.update_time,
			a.source, -- 来源: 0:市调导入；1:小程序;2:小程序-pc端 3: 线上网站市调
			a.product_status, -- 商品状态:1促销 0正常
			a.bom, -- bom配置
			a.bom_type -- bom类型(1:人工bom;2:报价策略人工bom;3:工厂bom;4:报价策略工厂bom)				
		from 
			(
			select 
				* 
			from 
				(
				select
					case 
					when source=0 then '市调导入'
					when source=1 then '小程序'
					when source=2 then '小程序-pc端'
					when source=3 then '线上网站市调'				
					else source end as source, -- 来源: 0:市调导入；1:小程序;2:小程序-pc端 3: 线上网站市调
					case product_status when 0 then '正常' when 1 then '促销' end as product_status, -- 商品状态:1促销 0正常
					bom, -- bom配置
					case 
					when bom_type=1 then '人工bom'
					when bom_type=2 then '报价策略人工bom'
					when bom_type=3 then '工厂bom'
					when bom_type=4 then '报价策略工厂bom'
					else bom_type end as bom_type, -- bom类型(1:人工bom;2:报价策略人工bom;3:工厂bom;4:报价策略工厂bom)				
					source_type_code,shop_code,shop_name,price,min_price,max_price,price_begin_time,price_end_time,create_time,create_by,remark,
					estimated_pricing_gross_margin,product_id,update_time
				from csx_ods.csx_ods_csx_price_prod_market_research_not_yh_price_effective_df  -- 非永辉 生效
				where regexp_replace(split(create_time,' ')[0],'-','')>='${last_year_day}'
				and source_type_code!=1
				-- from csx_dwd.csx_dwd_price_market_research_not_yh_price_effective_di -- 非永辉 生效
				-- where sdt>='${last_year_day}'
				-- and source_type_code!=1	
					
				union all
				select
					'' as source, -- 来源: 0:市调导入；1:小程序;2:小程序-pc端 3: 线上网站市调
					'' as product_status, -- 商品状态:1促销 0正常
					'' as bom, -- bom配置
					'' as bom_type, -- bom类型(1:人工bom;2:报价策略人工bom;3:工厂bom;4:报价策略工厂bom)				
					source_type_code,shop_code,shop_name,price,min_price,max_price,price_begin_time,price_end_time,create_time,create_by,remark,
					estimated_pricing_gross_margin,product_id,update_time
				from csx_dwd.csx_dwd_market_research_not_yh_price_di -- 非永辉 失效
				where sdt>='${last_year_day}'
				and source_type_code!=1							
				) tmp
			) a 
			left join (select * from csx_ods.csx_ods_csx_price_prod_market_research_product_df where sdt='${sdt_yes}') b on a.product_id=b.id 
			left join (select * from csx_dim.csx_dim_shop where sdt='current') c on c.shop_code=b.location_code
			left join (select * from csx_dim.csx_dim_basic_goods where sdt='current')d on d.goods_code=b.product_code
		) a 
	where rn=1
)a 
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
)c on a.location_code=c.location_code and a.product_code=c.product_code
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
)d on a.location_code=d.dc_code and a.product_code=d.goods_code
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
)e on a.location_code=e.location_code and a.product_code=e.product_code
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
		where sdt>='${sdt_bf60d}'
		and normal_status=0  -- 正常 = 0 异常 = 1
		and base_product_status=0  -- 0正常 3停售 6退场 7停购
		and regexp_replace(substr(price_end_time,1,10),'-','')>='${sdt_yes}'
		and regexp_replace(substr(price_begin_time,1,10),'-','')<='${sdt_yes}'
	)a 
	where rn=1
)f on a.location_code=f.warehouse_code and a.product_code=f.product_code
;


