-- 口径：①市调日期近7天；②市调类型：终端；③市调来源：小程序、市调图片导入；④剔除手动失效；⑤指定市调地点：'ZD168','ZD109','ZD145','ZD149','ZD147','ZD160','ZD125','ZD165','ZD371','ZD167','ZD411','ZD408','ZD295','ZD161','ZD148','ZD393','ZD409','ZD146'

-- 客户市调数据
drop table if exists csx_analyse_tmp.csx_analyse_tmp_customer_price;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_customer_price as 
select distinct
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
	cast(a.price as decimal(20,6)) as price,  -- 市调价
	a.source_type_code
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
			,source_type_code
        from csx_ods.csx_ods_csx_price_prod_market_customer_research_price_effective_di
        where sdt >='${sdt_bf14}'	
            and regexp_replace(substr(price_date,1,10),'-','') >='${sdt_bf7}'
        	-- and source_type_code =6   -- 市调地点，终端
        	-- and source in (1,5)       -- 来源：5，小程序
        	-- and market_code in ('ZD168','ZD109','ZD145','ZD149','ZD147','ZD160','ZD125','ZD165','ZD371','ZD167','ZD411','ZD408','ZD295','ZD161','ZD148','ZD393','ZD409','ZD146')	
        	
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
			,source_type_code
        from csx_dwd.csx_dwd_price_market_customer_research_price_di
        where sdt >='${sdt_bf14}'	
            and regexp_replace(substr(price_date,1,10),'-','') >='${sdt_bf7}'
        	and status = 0      
        	-- and source_type_code =6   -- 市调地点，终端
        	-- and source in (1,5)       -- 来源：5，小程序
        	-- and market_code in ('ZD168','ZD109','ZD145','ZD149','ZD147','ZD160','ZD125','ZD165','ZD371','ZD167','ZD411','ZD408','ZD295','ZD161','ZD148','ZD393','ZD409','ZD146')
		)a 
	)a
	left join (select * from csx_ods.csx_ods_csx_price_prod_market_research_product_df where sdt='${sdt_bf1}') b on a.product_id=b.id 
	left join (select * from csx_dim.csx_dim_shop where sdt='current') c on c.shop_code=b.location_code
	left join (select * from csx_dim.csx_dim_basic_goods where sdt='current')d on d.goods_code=b.product_code;




-- 通用市调
drop table if exists csx_analyse_tmp.csx_analyse_tmp_ty_price;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_ty_price as 
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
	a.source_type_code
from 
	(
	select *from 
        (      
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
        	update_time,
			source_type_code
        from csx_ods.csx_ods_csx_price_prod_market_research_not_yh_price_effective_df  -- 非永辉 生效
        where -- regexp_replace(split(create_time,' ')[0],'-','')>='${sdt_bf1}'
        	regexp_replace(substr(price_date,1,10),'-','')>='${sdt_bf7}'	 -- 市调日期近7天
        	-- and source_type_code= 6  -- 市调类型：1-永辉门店,2-网站,3-批发市场,4-一批,5-二批,6-终端'
        	-- and source in (1,5)		 -- 市调来源: 0:市调导入；1:小程序;2:小程序-pc端 3: 线上网站市调 4:通用市调系统生成 5:市调图片导入',	
        	-- and shop_code in ('ZD168','ZD109','ZD145','ZD149','ZD147','ZD160','ZD125','ZD165','ZD371','ZD167','ZD411','ZD408','ZD295','ZD161','ZD148','ZD393','ZD409','ZD146')
        	
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
        	update_time,
			source_type_code
        from csx_dwd.csx_dwd_market_research_not_yh_price_di -- 非永辉 失效				     
        where sdt>='${sdt_bf14}'	
        	and regexp_replace(substr(price_date,1,10),'-','')>='${sdt_bf7}'
        	and status= 0			
        	-- and source_type_code= 6	
        	-- and source in (1,5)	 
        	-- and shop_code in ('ZD168','ZD109','ZD145','ZD149','ZD147','ZD160','ZD125','ZD165','ZD371','ZD167','ZD411','ZD408','ZD295','ZD161','ZD148','ZD393','ZD409','ZD146')					
        ) a
	)a
	left join (select * from csx_ods.csx_ods_csx_price_prod_market_research_product_df where sdt='${sdt_bf1}') b on a.product_id=b.id 
	left join (select * from csx_dim.csx_dim_shop where sdt='current') c on c.shop_code=b.location_code
	left join (select * from csx_dim.csx_dim_basic_goods where sdt='current')d on d.goods_code=b.product_code;	
	

-- 日配30w客户
drop table if exists csx_analyse_tmp.csx_analyse_tmp_customer_30w;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_customer_30w as 
select * from 
(select 
	performance_region_name,
	performance_province_name,
	performance_city_name,
	customer_code,	
	customer_name,	
	sum(sale_amt)as sale_amt,
	sum(profit)as profit,
	sum(profit)/abs(sum(sale_amt))sale_rate
from 
	(select * 
	     from csx_dws.csx_dws_sale_detail_di 
	     where 
	        sdt>='${sdate}' and sdt<='${edate}'  
	        and channel_code <> '2' and substr(customer_code, 1, 1) <> 'S' -- 剔除商超数据 
	        and business_type_code in ('1') 
			-- and performance_province_name in ('上海松江','浙江省','北京市')
		) a
		join 
		(select  
			distinct shop_code 
		from csx_dim.csx_dim_shop 
		where sdt='current' 
			and shop_low_profit_flag=0  
		)c
		on a.inventory_dc_code = c.shop_code
group by 
	performance_region_name,
	performance_province_name,
	performance_city_name,
	customer_code,	
	customer_name
)a 
where sale_amt>=300000;



-- 结果
select 
	a.performance_province_name,
	a.performance_city_name,
	a.location_code,
    a.customer_code,  -- 客户编码
    a.customer_name,
    a.market_code,
    a.market_name,
    a.product_code,
    a.product_name,
    a.price,
    a.price_date,
	a.source_type_code,
	b.price as ty_price,
    b.price_date as ty_price_date,
	d.sale_amt,
	d.profit,
	d.sale_qty	
from 
	(select * from 
		(select *,
			row_number()over(partition by customer_code,market_code,product_code order by price_date desc) as srank
		from csx_analyse_tmp.csx_analyse_tmp_customer_price
		) a where  srank =1
	)a
	join 
	(select * from 
		(select *,
			row_number()over(partition by market_code,product_code order by price_date desc) as srank
			from csx_analyse_tmp.csx_analyse_tmp_ty_price
		) b where srank=1
	)b on a.market_code=b.market_code and a.product_code=b.product_code and a.source_type_code=b.source_type_code 
	join csx_analyse_tmp.csx_analyse_tmp_customer_30w c on a.customer_code =c.customer_code
 left join 
	(select 
		performance_region_name,
		performance_province_name,
		performance_city_name,
		customer_code,	
		customer_name,
		goods_code,
		goods_name,
		sum(sale_amt)as sale_amt,
		sum(profit)as profit,
		sum(profit)/abs(sum(sale_amt))sale_rate,
		sum(sale_qty)as sale_qty
	from 
		(select * 
			from csx_dws.csx_dws_sale_detail_di 
			where 
				sdt>='${sdate}' and sdt<='${edate}'  
				and channel_code <> '2' and substr(customer_code, 1, 1) <> 'S' -- 剔除商超数据 
				and business_type_code in ('1') 
				-- and performance_province_name in ('上海松江','浙江省','北京市')
			) a
			join 
			(select  
				distinct shop_code 
			from csx_dim.csx_dim_shop 
			where sdt='current' 
				and shop_low_profit_flag=0  
			)c
			on a.inventory_dc_code = c.shop_code
	group by 
		performance_region_name,
		performance_province_name,
		performance_city_name,
		customer_code,	
		customer_name,
		goods_code,
		goods_name
	) d  on  a.customer_code =d.customer_code and a.product_code =d.goods_code	


	
	
	
	
	
	
	


	
	
	
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
