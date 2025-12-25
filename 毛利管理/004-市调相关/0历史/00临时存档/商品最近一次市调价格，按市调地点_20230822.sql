		--b.location_code as `地点`,
		--(case when a.source_type_code=2 then '网站' 
		--	  when a.source_type_code=3 then '批发市场' 
		--	  when a.source_type_code=4 then '一批' 
		--	  when a.source_type_code=5 then '二批' 
		--	  when a.source_type_code=6 then '终端' end) as `市调类型`,
		--a.shop_code as `市调对象编码`,
		--a.shop_name as `市调对象名称`,
		--b.product_code as `商品编码`,
		--b.product_name as `商品名称`,
		--a.price as `价格`,
		--a.min_price as `最低价`,
		--a.max_price as `最高价`,
		--a.price_begin_time as `生效开始日期`,
		--a.price_end_time as `生效结束日期` ,
		--a.price_date as `市调日期`,

drop table csx_analyse_tmp.csx_analyse_tmp_market_research_shidiao_00;
create table csx_analyse_tmp.csx_analyse_tmp_market_research_shidiao_00
as
select
	*
from
	(
	select 
		b.location_code,
		(case when a.source_type_code=2 then '网站' 
			  when a.source_type_code=3 then '批发市场' 
			  when a.source_type_code=4 then '一批' 
			  when a.source_type_code=5 then '二批' 
			  when a.source_type_code=6 then '终端' end) as source_type_name,
		a.shop_code,
		a.shop_name,
		b.product_code,
		b.product_name,
		a.price,
		a.min_price,
		a.max_price,
		a.price_begin_time,
		a.price_end_time,
		to_date(a.price_date) as price_date,
		row_number()over(partition by b.location_code,b.product_code,a.shop_code order by a.price_date desc) as rn
	from 
		(select * from csx_ods.csx_ods_csx_price_prod_market_research_not_yh_price_di where sdt>='20220821') a 
		left join (select * from csx_ods.csx_ods_csx_price_prod_market_research_product_df where sdt='20230821') b on a.product_id=b.id 
		left join (select * from csx_dim.csx_dim_shop where sdt='current') c on c.shop_code=b.location_code
	where 
		regexp_replace(to_date(price_begin_time),'-','')<='20230821' 
		and regexp_replace(to_date(price_end_time),'-','')>='20220821' 
	) a 
where
	rn=1
;
select * from csx_analyse_tmp.csx_analyse_tmp_market_research_shidiao_00


drop table csx_analyse_tmp.csx_analyse_tmp_market_research_shidiao_00;
create table csx_analyse_tmp.csx_analyse_tmp_market_research_shidiao_00
as
select
	*
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
		row_number()over(partition by b.location_code,b.product_code,a.shop_code order by a.create_time desc) as rn
	from 
		(select * from csx_dwd.csx_dwd_price_market_research_price_di where sdt>='20220821' and market_source_type_code!='1') a 
		left join (select * from csx_ods.csx_ods_csx_price_prod_market_research_product_df where sdt='20230821') b on a.market_goods_id=b.id 
		left join (select * from csx_dim.csx_dim_shop where sdt='current') c on c.shop_code=b.location_code
	where 
		regexp_replace(to_date(price_begin_time),'-','')<='20230930' 
		and regexp_replace(to_date(price_end_time),'-','')>='20220821' 
	) a 
where
	rn=1
;
select * from csx_analyse_tmp.csx_analyse_tmp_market_research_shidiao_00