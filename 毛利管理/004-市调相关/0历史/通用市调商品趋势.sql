-- 拿到失效区最近一条数据
drop table if exists csx_analyse_tmp.csx_analyse_tmp_not_effective_price;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_not_effective_price as 
select * from 
 (select distinct 
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
	a.product_id,
	b.product_code,
	regexp_replace(b.product_name,'\n|\t|\r|\,|\"|\\\\n','') as product_name, 
	cast(a.price as decimal(20,6)) as price,
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
		product_id,source_type_code,shop_code,shop_name,price_date,price,remark,create_time,create_by,update_time,status,min_price,max_price,price_begin_time,price_end_time,source_type_two_level_code,estimated_pricing_gross_margin,source 
	from	
		csx_dwd.csx_dwd_market_research_not_yh_price_di -- 非永辉 失效
	where 
		sdt>='${sdate}'
		and source_type_code!=1
	) a 
	left join (select * from csx_ods.csx_ods_csx_price_prod_market_research_product_df where sdt='${ytd}') b on a.product_id=b.id 
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
			when change_type=4 then '导入失效'  --有冲突截断时间后失效旧数据生效新数据
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
	where sdt='${sdt_bf1}'
	) e on a.shop_code=e.shop_code and a.product_id=e.product_id and substr(a.update_time,1,16)=substr(e.update_time,1,16) and status=0
	
	
)a 
where
	rn = 1 and change_type != 5	 or change_type is null;


----------------

select
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
	min(case when rn=1 then price end) as bc_price,
    min(case when rn=1 then price_date end) as bc_price_date,
    max(case when rn=2 then price end) as sc_price,
    max(case when rn=2 then price_date end) as sc_price_date	
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
		(case when a.source_type_two_level_code=1 then '一批批发市场' 
			  when a.source_type_two_level_code=2 then '一批网站' 
			  when a.source_type_two_level_code=3 then '二批批发市场' 
			  when a.source_type_two_level_code=4 then '二批网站' 
		end) as source_type_two_level_name,
		a.shop_code,
		regexp_replace(a.shop_name,'\n|\t|\r|\,|\"|\\\\n','') as shop_name,
		a.product_id,		
		b.product_code,
		regexp_replace(b.product_name,'\n|\t|\r|\,|\"|\\\\n','') as product_name, 
		b.one_product_category_code,
		b.one_product_category_name,
		b.two_product_category_code,
		b.two_product_category_name,
		cast(a.price as decimal(20,6)) as price,  
		to_date(a.price_date) as price_date,
		row_number()over(partition by b.location_code,a.shop_code,b.product_code order by a.price_date desc) as rn
	from 
		( 
		select
			product_id,source_type_code,shop_code,shop_name,price_date,price,remark,create_time,create_by,update_time,status,min_price,max_price,price_begin_time,price_end_time,source_type_two_level_code,estimated_pricing_gross_margin,source
		from	
			csx_ods.csx_ods_csx_price_prod_market_research_not_yh_price_effective_df   -- 非永辉 生效
		where 
			source_type_code!=1			
		) a	
	left join (select * from csx_ods.csx_ods_csx_price_prod_market_research_product_df where sdt='${ytd}') b on a.product_id=b.id 
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
			when change_type=4 then '导入失效'  --有冲突截断时间后失效旧数据生效新数据
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
	where sdt='${ytd}'
	) e on a.shop_code=e.shop_code and a.product_id=e.product_id and substr(a.update_time,1,16)=substr(e.update_time,1,16) and status=0) a 
where  rn <= 2 and change_type != 5	 or change_type is null
group by 
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
	two_product_category_name;

	
	
	
	
		left join (select * from csx_analyse_tmp.csx_analyse_tmp_not_effective_price ) e on a.product_id = e.product_id and a.shop_code= e.shop_code 	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
-- 生效union失效，拿最近的一条============	
	
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



