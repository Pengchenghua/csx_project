
insert overwrite table csx_analyse.csx_analyse_fr_price_market_research_price_detail_df
 
select
	performance_province_name,performance_city_name,location_code,market_source_type_name,shop_code,shop_name,product_code,product_name,market_research_price,
	min_price,max_price,price_begin_time,price_end_time,create_date,a.create_by,a.remark
from
	(
	select 
		c.performance_province_name,
		c.performance_city_name,
		b.location_code,
		(case when a.market_source_type_code=2 then '网站' 
			  when a.market_source_type_code=3 then '批发市场' 
			  when a.market_source_type_code=4 then '一批' 
			  when a.market_source_type_code=5 then '二批' 
			  when a.market_source_type_code=6 then '终端' end) as market_source_type_name,
		a.shop_code,
		--a.shop_name,
		regexp_replace(a.shop_name,'\n|\t|\r|\,|\"|\\\\n','') as shop_name, 
		b.product_code,
		--b.product_name,
		regexp_replace(b.product_name,'\n|\t|\r|\,|\"|\\\\n','') as product_name, 
		a.market_research_price,
		a.min_price,
		a.max_price,
		a.price_begin_time,
		a.price_end_time,
		to_date(a.create_time) as create_date,
		row_number()over(partition by b.location_code,b.product_code,a.shop_code order by a.create_time desc) as rn,
		a.create_by,
		a.remark
	from 
		(
		select 
			* 
		from 
			csx_dwd.csx_dwd_price_market_research_price_di 
		where 
			sdt>='${last_year_day}'
			and market_source_type_code!='1'
			--and regexp_replace(to_date(price_begin_time),'-','')<='${ytd}'
			--and regexp_replace(to_date(price_end_time),'-','')>='${last_year_day}'
		) a 
		left join (select * from csx_ods.csx_ods_csx_price_prod_market_research_product_df where sdt='${ytd}') b on a.market_goods_id=b.id 
		left join (select * from csx_dim.csx_dim_shop where sdt='current') c on c.shop_code=b.location_code
	) a 
where
	rn=1



