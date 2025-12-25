select * from 
(select 
* ,row_number()over(partition by location_code,product_code order by create_time desc) as rn
from 
(select 
	c.performance_province_name,
	c.performance_city_name,
	b.location_code,
	(case when a.market_source_type_code=1 then '永辉门店' end) as market_source_type_name,
	-- a.shop_code,
	-- regexp_replace(a.shop_name,'\n|\t|\r|\,|\"|\\\\n','') as shop_name, 
	b.product_code,
	regexp_replace(b.product_name,'\n|\t|\r|\,|\"|\\\\n','') as product_name, 		
	d.classify_large_name,
	d.classify_middle_name,
	d.classify_small_name,
	a.market_research_date,
	a.market_research_price,
	a.create_time
from 
	(
	select 
		*
	from 
		csx_dwd.csx_dwd_price_market_research_price_di 
	where sdt>='20240501'  -- regexp_replace(date_sub(current_date,1),'-','')
		and market_source_type_code='1' -- 市调来源类型编码：1-永辉门店,2-网站,3-批发市场,4-一批,5-二批,6-终端
	) a 
	left join 
	(
	select * from csx_ods.csx_ods_csx_price_prod_market_research_product_df 
	where sdt='${sdt_yes}'  -- regexp_replace(date_sub(current_date,1),'-','')
	) b on a.market_goods_id=b.id 
	left join (select * from csx_dim.csx_dim_shop where sdt='current') c on c.shop_code=b.location_code
	left join (select * from csx_dim.csx_dim_basic_goods where sdt='current') d on d.goods_code=b.product_code
group by 
	c.performance_province_name,
	c.performance_city_name,
	b.location_code,
	(case when a.market_source_type_code=1 then '永辉门店' end),
	-- a.shop_code,
	-- regexp_replace(a.shop_name,'\n|\t|\r|\,|\"|\\\\n','') as shop_name, 
	b.product_code,
	regexp_replace(b.product_name,'\n|\t|\r|\,|\"|\\\\n','') , 		
	d.classify_large_name,
	d.classify_middle_name,
	d.classify_small_name,
	a.market_research_date,
	a.market_research_price,
	a.create_time
)a
)a 
where rn=1 and location_code='W0BK';