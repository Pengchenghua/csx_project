
drop table csx_analyse_tmp.tmp_goods_days_not_yh_price;
create temporary table csx_analyse_tmp.tmp_goods_days_not_yh_price
as
select 
	a.*,
    row_number()over(partition by location_code,market_source_type_name,source_type_two_level_name,shop_code,product_code order by create_time desc) as rn
from 
(select 
	distinct 
	c.performance_province_name,
	c.performance_city_name,
	b.location_code,
	(case when a.source_type_code=2 then '网站' 
		  when a.source_type_code=3 then '批发市场' 
		  when a.source_type_code=4 then '一批' 
		  when a.source_type_code=5 then '二批' 
		  when a.source_type_code=6 then '终端' 
	end) as market_source_type_name,
	(case when a.source_type_two_level_code=1 then '一批批发市场' 
		  when a.source_type_two_level_code=2 then '一批网站' 
		  when a.source_type_two_level_code=3 then '二批批发市场' 
		  when a.source_type_two_level_code=4 then '二批网站' 
	end) as source_type_two_level_name,
	a.shop_code,
	regexp_replace(a.shop_name,'\n|\t|\r|\,|\"|\\\\n','') as shop_name,
	b.product_code,
	regexp_replace(b.product_name,'\n|\t|\r|\,|\"|\\\\n','') as product_name, 
	b.one_product_category_code,
	b.one_product_category_name,
	b.two_product_category_code,
	b.two_product_category_name,
	cast(a.price as decimal(20,6)) as price,  
	regexp_replace(to_date(a.price_date),'-','') as price_date,
	regexp_replace(to_date(a.price_begin_time),'-','') as price_begin_date,		
	regexp_replace(to_date(a.price_end_time),'-','') as price_end_date,
	d.unit_name,
	a.create_time
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
		sdt>='${sdt_13mago}'
		and source_type_code!=1			
	) a	
	left join (select * from csx_ods.csx_ods_csx_price_prod_market_research_product_df where sdt='${sdt_yes}') b on a.product_id=b.id 
	left join (select * from csx_dim.csx_dim_shop where sdt='current') c on c.shop_code=b.location_code
	left join (select * from csx_dim.csx_dim_basic_goods where sdt='current')d on d.goods_code=b.product_code
	-- 市调价格更新日志表--价格失效原因
	left join 
	(select 
		shop_code,product_id,
		update_by,update_time,
		change_type,
		case 
			when change_type=1 then '页面修改价格'
			when change_type=2 then '导入修改价格'
			when change_type=3 then '导入修改时间'
			when change_type=4 then '导入失效'  -- 有冲突截断时间后失效旧数据生效新数据
			when change_type=5 then '手动失效'
			when change_type=6 then '导入引用%s客户市调'
			when change_type=7 then '添加商品'
			when change_type=8 then '添加修改价格'
			when change_type=9 then '添加修改时间'
			when change_type=10 then '添加失效'
			when change_type=11 then '添加引用%s客户市调'
			when change_type=12 then '冲突'	
			else change_type end as change_type_name		-- 页面修改价格		
			-- row_number()over(partition by shop_code,product_id order by update_time desc) as rno
	from csx_ods.csx_ods_csx_price_prod_market_research_price_log_df  
	where sdt='${sdt_yes}'
	) e on a.shop_code=e.shop_code and a.product_id=e.product_id and substr(a.update_time,1,16)=substr(e.update_time,1,16) and status=0
where change_type != 5	or change_type is null
)a





insert overwrite table csx_analyse.csx_analyse_fr_goods_days_not_yh_price_df
select 
	concat_ws('-',location_code,market_source_type_name,source_type_two_level_name,shop_code,product_code,cast(price as string),price_date ) as biz_id,
	performance_province_name,
	performance_city_name,
	location_code,
	market_source_type_name,
	source_type_two_level_name,
	shop_code,
	shop_name,
	product_code,
	product_name, 
	one_product_category_code,
	one_product_category_name,
	two_product_category_code,
	two_product_category_name,
	price,  
	price_date,
	price_begin_date,		
	price_end_date,
	unit_name	
from csx_analyse_tmp.tmp_goods_days_not_yh_price
where 
	rn =1
	
	
	

	
insert overwrite table csx_analyse.csx_analyse_fr_goods_days_not_yh_price_df
select 
	concat_ws('-',location_code,market_source_type_name,source_type_two_level_name,shop_code,product_code,cast(price as string),price_date ) as biz_id,
	a.*
from 
(select 
	distinct 
	c.performance_province_name,
	c.performance_city_name,
	b.location_code,
	(case when a.source_type_code=2 then '网站' 
		  when a.source_type_code=3 then '批发市场' 
		  when a.source_type_code=4 then '一批' 
		  when a.source_type_code=5 then '二批' 
		  when a.source_type_code=6 then '终端' 
	end) as market_source_type_name,
	(case when a.source_type_two_level_code=1 then '一批批发市场' 
		  when a.source_type_two_level_code=2 then '一批网站' 
		  when a.source_type_two_level_code=3 then '二批批发市场' 
		  when a.source_type_two_level_code=4 then '二批网站' 
	end) as source_type_two_level_name,
	a.shop_code,
	regexp_replace(a.shop_name,'\n|\t|\r|\,|\"|\\\\n','') as shop_name,
	b.product_code,
	regexp_replace(b.product_name,'\n|\t|\r|\,|\"|\\\\n','') as product_name, 
	b.one_product_category_code,
	b.one_product_category_name,
	b.two_product_category_code,
	b.two_product_category_name,
	cast(a.price as decimal(20,6)) as price,  
	regexp_replace(to_date(a.price_date),'-','') as price_date,
	regexp_replace(to_date(a.price_begin_time),'-','') as price_begin_date,		
	regexp_replace(to_date(a.price_end_time),'-','') as price_end_date,
	d.unit_name
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
		sdt>='${sdt_13mago}'
		and source_type_code!=1			
	) a	
	left join (select * from csx_ods.csx_ods_csx_price_prod_market_research_product_df where sdt='${sdt_yes}') b on a.product_id=b.id 
	left join (select * from csx_dim.csx_dim_shop where sdt='current') c on c.shop_code=b.location_code
	left join (select * from csx_dim.csx_dim_basic_goods where sdt='current')d on d.goods_code=b.product_code
	-- 市调价格更新日志表--价格失效原因
	left join 
	(select 
		shop_code,product_id,
		update_by,update_time,
		change_type,
		case 
			when change_type=1 then '页面修改价格'
			when change_type=2 then '导入修改价格'
			when change_type=3 then '导入修改时间'
			when change_type=4 then '导入失效'  -- 有冲突截断时间后失效旧数据生效新数据
			when change_type=5 then '手动失效'
			when change_type=6 then '导入引用%s客户市调'
			when change_type=7 then '添加商品'
			when change_type=8 then '添加修改价格'
			when change_type=9 then '添加修改时间'
			when change_type=10 then '添加失效'
			when change_type=11 then '添加引用%s客户市调'
			when change_type=12 then '冲突'	
			else change_type end as change_type_name		-- 页面修改价格		
			-- row_number()over(partition by shop_code,product_id order by update_time desc) as rno
	from csx_ods.csx_ods_csx_price_prod_market_research_price_log_df  
	where sdt='${sdt_yes}'
	) e on a.shop_code=e.shop_code and a.product_id=e.product_id and substr(a.update_time,1,16)=substr(e.update_time,1,16) and status=0
where change_type != 5	or change_type is null
)a