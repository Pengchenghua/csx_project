-- 口径：①市调日期近7天；②市调类型：终端；③市调来源：小程序、市调图片导入；④剔除手动失效；⑤指定市调地点：'ZD168','ZD109','ZD145','ZD149','ZD147','ZD160','ZD125','ZD165','ZD371','ZD167','ZD411','ZD408','ZD295','ZD161','ZD148','ZD393','ZD409','ZD146'

-- 客户市调+ 通用市调
drop table if exists csx_analyse_tmp.csx_analyse_tmp_customer_price;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_customer_price as 
select 
	a.stype,
	a.price_date,
	c.performance_province_name,  
	c.performance_city_name,
	b.location_code,  -- 仓
	a.customer_code,  -- 客户编码
	a.customer_name,
	a.market_code,    -- 市调地点
	regexp_replace(a.market_name,'\n|\t|\r|\,|\"|\\\\n','') as market_name, 
	b.product_code,   -- 商品编码
	regexp_replace(b.product_name,'\n|\t|\r|\,|\"|\\\\n','') as product_name, 
	cast(a.price as decimal(20,6)) as price,   -- 市调价
	a.status,  -- 状态(1-有效 0-无效)
	e.change_type	-- 页面修改价格		
from 
	(
	select *from 
        (select   distinct     -- 客户市调生效区
        	'客户市调' as  stype
        	,product_id
        	,customer_code
        	,customer_name
        	,price_date
        	,market_code
        	,market_name
        	,price
        	,status
        	,update_time
        from csx_ods.csx_ods_csx_price_prod_market_customer_research_price_effective_di
        where sdt >='${sdt_bf14}'	
            and regexp_replace(substr(price_date,1,10),'-','') >='${sdt_bf7}'
        	and source_type_code =6   -- 市调地点，终端
        	and source in (1,5)       -- 来源：5，小程序
        	and market_code in ('ZD168','ZD109','ZD145','ZD149','ZD147','ZD160','ZD125','ZD165','ZD371','ZD167','ZD411','ZD408','ZD295','ZD161','ZD148','ZD393','ZD409','ZD146')	
        	
        union all
        
        select  distinct    -- 客户市调失效区
        	'客户市调' as  stype
        	,product_id
        	,customer_code
        	,customer_name
        	,price_date
        	,market_code
        	,market_name
        	,price
        	,cast(status as int) status
        	,update_time
        from csx_dwd.csx_dwd_price_market_customer_research_price_di
        where sdt >='${sdt_bf14}'	
            and regexp_replace(substr(price_date,1,10),'-','') >='${sdt_bf7}'
        	and status = 0      
        	and source_type_code =6   -- 市调地点，终端
        	-- and source in (1,5)       -- 来源：5，小程序
        	and market_code in ('ZD168','ZD109','ZD145','ZD149','ZD147','ZD160','ZD125','ZD165','ZD371','ZD167','ZD411','ZD408','ZD295','ZD161','ZD148','ZD393','ZD409','ZD146')
        
        union all	
        
        select distinct
            '通用市调' as stype,
        	product_id,
        	'' customer_code,
        	'' customer_name,
        	price_date,
        	shop_code as market_code,
        	shop_name as market_name,
        	price,
        	cast(status as int) status,
        	update_time
        from csx_ods.csx_ods_csx_price_prod_market_research_not_yh_price_effective_df  -- 非永辉 生效
        where -- regexp_replace(split(create_time,' ')[0],'-','')>='${sdt_bf1}'
        	regexp_replace(substr(price_date,1,10),'-','')>='${sdt_bf7}'	 -- 市调日期近7天
        	and source_type_code= 6  -- 市调类型：1-永辉门店,2-网站,3-批发市场,4-一批,5-二批,6-终端'
        	and source in (1,5)		 -- 市调来源: 0:市调导入；1:小程序;2:小程序-pc端 3: 线上网站市调 4:通用市调系统生成 5:市调图片导入',	
        	and shop_code in ('ZD168','ZD109','ZD145','ZD149','ZD147','ZD160','ZD125','ZD165','ZD371','ZD167','ZD411','ZD408','ZD295','ZD161','ZD148','ZD393','ZD409','ZD146')
        	
        union all
        
        select distinct
        	'通用市调' as stype,
        	product_id,
        	'' customer_code,
        	'' customer_name,
        	price_date,
        	shop_code as market_code,
        	shop_name as market_name,
        	price,
        	cast(status as int) status,
        	update_time
        from csx_dwd.csx_dwd_market_research_not_yh_price_di -- 非永辉 失效
        where sdt>='${sdt_bf14}'	
        	and regexp_replace(substr(price_date,1,10),'-','')>='${sdt_bf7}'
        	and status= 0			
        	and source_type_code= 6	
        	-- and source in (1,5)	 
        	and shop_code in ('ZD168','ZD109','ZD145','ZD149','ZD147','ZD160','ZD125','ZD165','ZD371','ZD167','ZD411','ZD408','ZD295','ZD161','ZD148','ZD393','ZD409','ZD146')					
        ) a
	)a
	left join (select * from csx_ods.csx_ods_csx_price_prod_market_research_product_df where sdt='${sdt_bf1}') b on a.product_id=b.id 
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
	) e on a.market_code=e.shop_code and a.product_id=e.product_id and substr(a.update_time,1,16)=substr(e.update_time,1,16) and status=0
where  change_type != 5	 or change_type is null;

	
--最高价------------------------------------------------
select 
	a.*
from         
(select 
    stype,
    customer_code,  -- 客户编码
    customer_name,
    market_code,
    market_name,
    product_code,
    product_name,
    price,
    price_date,
    row_number()over(partition by product_code order by  price desc, price_date desc) as srank
from csx_analyse_tmp.csx_analyse_tmp_customer_price        
)a     
where srank = 1
;






-- 客户市调  
drop table if exists csx_analyse_tmp.csx_analyse_tmp_customer_price;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_customer_price as 
select 
	a.stype,
	a.price_date,
	c.performance_province_name,  
	c.performance_city_name,
	b.location_code,  -- 仓
	a.customer_code,  -- 客户编码
	a.customer_name,
	a.market_code,    -- 市调地点
	regexp_replace(a.market_name,'\n|\t|\r|\,|\"|\\\\n','') as market_name, 
	b.product_code,   -- 商品编码
	regexp_replace(b.product_name,'\n|\t|\r|\,|\"|\\\\n','') as product_name, 
	a.price,   -- 市调价
	a.status,  -- 状态(1-有效 0-无效)
	e.change_type	-- 页面修改价格		
from 
	(
	select   distinct     -- 客户市调生效区
		'客户市调' as  stype
		,product_id
		,customer_code
		,customer_name
		,price_date
		,market_code
		,market_name
		,price
		,status
		,update_time
	from csx_ods.csx_ods_csx_price_prod_market_customer_research_price_effective_di
	where sdt >='${sdt_bf14}'	
	    and regexp_replace(substr(price_date,1,10),'-','') >='${sdt_bf7}'
		and source_type_code =6   -- 市调地点，终端
		and source in (1,5)       -- 来源：5，小程序
		and market_code in ('ZD168','ZD109','ZD145','ZD149','ZD147','ZD160','ZD125','ZD165','ZD371','ZD167','ZD411','ZD408','ZD295','ZD161','ZD148','ZD393','ZD409','ZD146')	
		
	union all
	
	select  distinct    -- 客户市调失效区
		'客户市调' as  stype
		,product_id
		,customer_code
		,customer_name
		,price_date
		,market_code
		,market_name
		,price
		,cast(status as int) status
		,update_time
	from csx_dwd.csx_dwd_price_market_customer_research_price_di
	where sdt >='${sdt_bf14}'	
	    and regexp_replace(substr(price_date,1,10),'-','') >='${sdt_bf7}'
		and cast(status as int ) = 0      
		and source_type_code =6   -- 市调地点，终端
		-- and source in (1,5)       -- 来源：5，小程序
		and market_code in ('ZD168','ZD109','ZD145','ZD149','ZD147','ZD160','ZD125','ZD165','ZD371','ZD167','ZD411','ZD408','ZD295','ZD161','ZD148','ZD393','ZD409','ZD146')
	) a	
	left join (select * from csx_ods.csx_ods_csx_price_prod_market_research_product_df where sdt='${sdt_bf1}') b on a.product_id=b.id 
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
	) e on a.market_code=e.shop_code and a.product_id=e.product_id and substr(a.update_time,1,16)=substr(e.update_time,1,16) and status=0
where  change_type != 5	 or change_type is null;
	
--最高价------------------------------------------------
select 
	a.*
from 	
(select 
	stype,
	customer_code,  -- 客户编码
	customer_name,
	market_code,
	market_name,
	product_code,
	product_name,
	price,
	regexp_replace(substr(price_date,1,10),'-','') price_date,
	row_number()over(partition by customer_code, market_code, product_code order by price desc) as srank
from csx_analyse_tmp.csx_analyse_tmp_customer_price	
)a	
where srank = 1
;
	
	
	
	
	
	
	
-- 通用市调		
drop table if exists csx_analyse_tmp.csx_analyse_tmp_tongyongshidiao;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_tongyongshidiao as 
select distinct 
	a.stype,
	c.performance_province_name,
	c.performance_city_name,
	b.location_code,
	a.market_code,
	regexp_replace(a.market_name,'\n|\t|\r|\,|\"|\\\\n','') as market_name, 
	b.product_code,
	regexp_replace(b.product_name,'\n|\t|\r|\,|\"|\\\\n','') as product_name, 
	a.price,
	a.price_date,
	e.change_type		-- 页面修改价格			
from 
	(
	select distinct 
	    '通用市调' as stype,
		product_id,
		shop_code as market_code,
		shop_name as market_name,
		price_date,
		price,
		status,
		update_time
	from csx_ods.csx_ods_csx_price_prod_market_research_not_yh_price_effective_df  -- 非永辉 生效
		
	union all
	
	select distinct
		'通用市调' as stype,
		product_id,
		shop_code as market_code,
		shop_name as market_name,
		price_date,
		price,
		status,
		update_time
	from csx_dwd.csx_dwd_market_research_not_yh_price_di -- 非永辉 失效
	where sdt>='${sdt_bf14}'	
		and regexp_replace(substr(price_date,1,10),'-','')>='${sdt_bf7}'		
	) a 
	left join (select * from csx_ods.csx_ods_csx_price_prod_market_research_product_df where sdt='${sdt_bf1}') b on a.product_id=b.id 
	left join (select * from csx_dim.csx_dim_shop where sdt='current') c on c.shop_code=b.location_code
	left join (select * from csx_dim.csx_dim_basic_goods where sdt='current')d on d.goods_code=b.product_code
	-- 市调价格更新日志表--价格失效原因
	left join 
	(
	select 
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
	)e on a.market_code=e.shop_code and a.product_id=e.product_id and substr(a.update_time,1,16)=substr(e.update_time,1,16) and status=0		
-- where  change_type != 5	 or change_type is null
	
	
--最高价------------------------------------------------
select 
	a.*
from 	
(select 
	stype,
	customer_code,  -- 客户编码
	customer_name,
	market_code,
	market_name,
	product_code,
	product_name,
	price,
	regexp_replace(substr(price_date,1,10),'-','') price_date,
	row_number()over(partition by customer_code, market_code, product_code order by price desc) as srank
from csx_analyse_tmp.csx_analyse_tmp_tongyongshidiao 
)a	
where srank = 1
;	
	
	
	
	
	
	
	
	
	
			case 
				when source=0 then '市调导入'
				when source=1 then '小程序'
				when source=2 then '小程序-pc端'
				when source=3 then '线上网站市调'	
				when source=4 then '通用市调系统生成'	
				when source=5 then '市调图片导入'				
			else source end as source, -- 来源: 0:市调导入；1:小程序;2:小程序-pc端 3: 线上网站市调 4:通用市调系统生成 5:市调图片导入',