-- 202311至今客户市调数据 20240123
select
	c.performance_province_name,
	c.performance_city_name,
	b.location_code,  -- 地点编码
	c.shop_name as location_name,  -- 地点名称
	-- a.product_id,  -- 市调商品id	
	a.customer_code,  -- 客户号
	regexp_replace(a.customer_name,'\n|\t|\r|\,|\"|\\\\n','') as customer_name,  -- 客户名称
	b.product_code,
	--b.product_name,
	regexp_replace(b.product_name,'\n|\t|\r|\,|\"|\\\\n','') as product_name, 	
	d.standard,
	d.unit_name,
	a.price_date,  -- 市调日期
	-- a.source_type_code,  -- 市调地点类型一级:4:一批,5:二批,6:终端
	-- a.source_type_two_level_code,  -- 市调地点类型二级:1:一批批发市场,2:一批网站,3:二批批发市场,4:二批网站
	(case when a.source_type_code=2 then '网站' 
		  when a.source_type_code=3 then '批发市场' 
		  when a.source_type_code=4 then '一批' 
		  when a.source_type_code=5 then '二批' 
		  when a.source_type_code=6 then '终端' end) as market_source_type_name,

	(case when a.source_type_two_level_code=1 then '一批批发市场' 
		  when a.source_type_two_level_code=2 then '一批网站' 
		  when a.source_type_two_level_code=3 then '二批批发市场' 
		  when a.source_type_two_level_code=4 then '二批网站' end) as source_type_two_level_name,		  
	a.market_code,  -- 市调对象编码
	a.market_name,  -- 市调对象名称
	a.price,  -- 市调价格
	a.min_price,  -- 最低价
	a.max_price,  -- 最高价
	a.market_price_wave,  -- 市调价波动=（当天市调价-最近一次市调价）/最近一次市调价
	a.estimated_pricing_gross_margin,  -- 预估定价毛利率=（市调价-库存平均价）/市调价
	a.price_begin_time,  -- 生效开始时间
	a.price_end_time,  -- 生效结束时间
	-- a.status,  -- 状态(1有效 0无效)
	regexp_replace(a.remark,'\n|\t|\r|\,|\"|\\\\n','') as remark,  -- 备注
	-- a.create_by,  -- 创建人
	-- a.create_time,  -- 创建时间
	a.update_by,  -- 更新人
	a.update_time,  -- 更新时间
	-- a.sdt  -- 创建时间分区{\"FORMAT\":\"yyyymmdd\"}
	b.one_product_category_code,
	b.one_product_category_name,
	b.two_product_category_code,
	b.two_product_category_name,
	b.three_product_category_code,
	b.three_product_category_name	
from 
	(
	select *
	from 
		(
		select *,
			row_number()over(partition by customer_code,goods_id order by price_date desc,create_time desc) as rn
		-- from csx_dwd.csx_dwd_price_market_customer_research_price_di
		from csx_dwd.csx_dwd_price_market_customer_research_price_effective_di
		where sdt>='20230101'
		and status=1
		-- and price_date>='2023-11-01'
		)a
		where rn=1
	)a
		left join (select * from csx_ods.csx_ods_csx_price_prod_market_research_product_df 
				where sdt=regexp_replace(add_months(date_sub(current_date,1),0),'-','')) b on a.goods_id=b.id 
		left join (select * from csx_dim.csx_dim_shop where sdt='current') c on c.shop_code=b.location_code
		left join (select * from csx_dim.csx_dim_basic_goods where sdt='current')d on d.goods_code=b.product_code
;
