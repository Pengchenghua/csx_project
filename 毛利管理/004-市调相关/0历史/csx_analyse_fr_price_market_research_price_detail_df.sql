
select
	*
from
	(
	select distinct 
		c.performance_province_name,
		c.performance_city_name,
		b.location_code,
		(case when a.source_type_code=2 then '网站' 
			  when a.source_type_code=3 then '批发市场' 
			  when a.source_type_code=4 then '一批' 
			  when a.source_type_code=5 then '二批' 
			  when a.source_type_code=6 then '终端' 
		end) as market_source_type_name,
		a.shop_code,
		regexp_replace(a.shop_name,'\n|\t|\r|\,|\"|\\\\n','') as shop_name, 
		b.product_code,
		regexp_replace(b.product_name,'\n|\t|\r|\,|\"|\\\\n','') as product_name, 
		a.price,
		to_date(a.price_date) as price_date,
		a.min_price,
		a.max_price,
		a.price_begin_time,
		a.price_end_time,
		to_date(a.create_time) as create_date,
		a.create_by,
		d.unit_name,
		a.estimated_pricing_gross_margin,
		b.one_product_category_code,
		b.one_product_category_name,
		b.two_product_category_code,
		b.two_product_category_name,
		a.update_time,
		row_number()over(partition by b.location_code,a.shop_code,b.product_code order by a.price_date desc) as rn
	from 
		(
		select 
			* 
		from 
			(
			select
				product_id,source_type_code,shop_code,shop_name,price_date,price,remark,create_time,create_by,update_time,status,min_price,max_price,price_begin_time,price_end_time,source_type_two_level_code,estimated_pricing_gross_margin,cast(source as int) source
			from	
				csx_ods.csx_ods_csx_price_prod_market_research_not_yh_price_effective_df   -- 非永辉 生效
			where 
				source_type_code!=1
				
			union all
			
			select
				product_id,source_type_code,shop_code,shop_name,price_date,price,remark,create_time,create_by,update_time,status,min_price,max_price,price_begin_time,price_end_time,source_type_two_level_code,estimated_pricing_gross_margin,source 
			from	
				csx_dwd.csx_dwd_market_research_not_yh_price_di -- 非永辉 失效
			where 
				sdt>='${sdate}'
				and source_type_code!=1
			
			) tmp
		) a 
		left join (select * from csx_ods.csx_ods_csx_price_prod_market_research_product_df where sdt='${ytd}') b on a.product_id=b.id 
		left join (select * from csx_dim.csx_dim_shop where sdt='current') c on c.shop_code=b.location_code
		left join (select * from csx_dim.csx_dim_basic_goods where sdt='current')d on d.goods_code=b.product_code
	) a 
where
	location_code='W0A7' and shop_code='ZD109' and product_code ='1009530'



