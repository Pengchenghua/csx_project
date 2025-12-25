/*
1.标品清单
近三个月动销
食百所有商品，干货加工，肉禽水产，三个都删除单位KG
商品状态（B+K)
全国近三个月动销大于0，剔除直送和自提

只保留三个月累计动销数据


2.非标品
干货加工（保留计价单位“KG”，蛋类所有），肉禽水产（删除预制菜），蔬菜水果
近一个月动销
商品状态（B+K)
*/

--标品
drop table if exists csx_analyse_tmp.csx_analyse_tmp_shidiao_goods_bp;
create table csx_analyse_tmp.csx_analyse_tmp_shidiao_goods_bp
as
select
	e.performance_province_name,
	e.performance_city_name,
	a.inventory_dc_code,
	c.business_division_name,
	c.classify_large_name,
	c.classify_middle_name,
	c.category_small_name,
	c.spu_goods_name,
	c.brand_name,
	a.goods_code,
	c.goods_name,
	regexp_replace(d.regionalized_goods_name,'\n|\t|\r|\,|\"|\\\\n','') as regionalized_goods_name, 
	c.unit_name,
	c.standard,
	d.goods_status_name,
	case when d.stock_attribute_code='1' then '是' else '否' end as is_beihuo_goods,
	c.goods_bar_code,
	sum(a.sale_amt) sale_amt,
	sum(a.sale_qty) sale_qty,
	sum(a.profit)/abs(sum(a.sale_amt)) as profit_rate,
	count(distinct a.customer_code) customer_cnt,
	count(distinct a.sdt) as day_cnt,
	count(a.goods_code) as goods_cnt
from 
	(
	select 
		sdt,goods_code,customer_code,inventory_dc_code,order_code,sale_amt,sale_qty,profit
	from 
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt between '20240701' and '20240929'
		and channel_code in ('1','7','9')
		and business_type_code =1 -- 仅日配 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
		and delivery_type_code in (1) -- 剔除直送和自提 配送类型编码：1-配送 2-直送 3-自提 4-直通 11-同城配送 12-快递配送 13-一件代发
		--and order_channel_code not in (5) -- 剔除调价返利和价格补救 订单来源渠道: 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
		--and order_channel_detail_code in (11,12) -- 11系统手工单 12小程序大宗单 25客户返利 26价格补救 27客户调价
		and inventory_dc_code in('W0R9','W0A5','W0N0','W0W7','W0X6','W0T1','W0N1','W0AS','W0A8','W0F4','W0L3','WB56','W0AH','W0G9','WA96','WB67','W0K6','W0BK','W0A7','W0X2','W0Z9','W0A6','W0Q2','W0A3','W0Q9','W0P8','W0A2','W0L4','W0BR','W0BH','WB95','WC53')
	) a 	
	join 
		(
		select 
			goods_code,goods_name,unit_name,classify_large_name,classify_middle_name,classify_small_name,brand_name,standard,category_small_name,spu_goods_name,goods_bar_code,business_division_name
		from 
			csx_dim.csx_dim_basic_goods
		where 
			sdt ='current'
			and ( 
				(business_division_name like '%食百%' and unit_name!='KG') 
				or (classify_large_name='干货加工' and unit_name!='KG')
				or (classify_large_name='肉禽水产' and unit_name!='KG' )			
				) --  标品
		) c on a.goods_code=c.goods_code
	join
		(
		select
			dc_code,goods_code,shop_special_goods_status,goods_status_name,stock_attribute_code,regionalized_goods_name,stock_attribute_name --1存储 2货到即配
		from
			csx_dim.csx_dim_basic_dc_goods
		where 
			sdt = 'current'
			and shop_special_goods_status in('0','7') -- 0：B 正常商品；3：H 停售；6：L 退场；7：K 永久停购；
		) d on d.dc_code=a.inventory_dc_code and d.goods_code=a.goods_code	
	left join 
		(
		select 
			shop_code,shop_name,performance_province_name,performance_city_name
		from 
			csx_dim.csx_dim_shop 
		where 
			sdt='current'
		) e on e.shop_code=a.inventory_dc_code
group by 
	e.performance_province_name,
	e.performance_city_name,
	a.inventory_dc_code,
	c.business_division_name,
	c.classify_large_name,
	c.classify_middle_name,
	c.category_small_name,
	c.spu_goods_name,
	c.brand_name,
	a.goods_code,
	c.goods_name,
	regexp_replace(d.regionalized_goods_name,'\n|\t|\r|\,|\"|\\\\n',''),
	c.unit_name,
	c.standard,
	d.goods_status_name,
	case when d.stock_attribute_code='1' then '是' else '否' end,
	c.goods_bar_code
having
	sum(a.sale_amt) >0
;
select * from csx_analyse_tmp.csx_analyse_tmp_shidiao_goods_bp
;
-- ====================================================================================================================================================================
--非标品
drop table if exists csx_analyse_tmp.csx_analyse_tmp_shidiao_goods_fbp;
create table csx_analyse_tmp.csx_analyse_tmp_shidiao_goods_fbp
as
select
	e.performance_province_name,
	e.performance_city_name,
	a.inventory_dc_code,
	c.business_division_name,
	c.classify_large_name,
	c.classify_middle_name,
	c.category_small_name,
	c.spu_goods_name,
	c.brand_name,
	a.goods_code,
	c.goods_name,
	regexp_replace(d.regionalized_goods_name,'\n|\t|\r|\,|\"|\\\\n','') as regionalized_goods_name, 
	c.unit_name,
	c.standard,
	d.goods_status_name,
	case when d.stock_attribute_code='1' then '是' else '否' end as is_beihuo_goods,
	c.goods_bar_code,
	sum(a.sale_amt) sale_amt,
	sum(a.sale_qty) sale_qty,
	sum(a.profit)/abs(sum(a.sale_amt)) as profit_rate,
	count(distinct a.customer_code) customer_cnt,
	count(distinct a.sdt) as day_cnt,
	count(a.goods_code) as goods_cnt
from 
	(
	select 
		sdt,goods_code,customer_code,inventory_dc_code,order_code,sale_amt,sale_qty,profit
	from 
		csx_dws.csx_dws_sale_detail_di
	-- where sdt between '20231208' and '20240107'
	where sdt between regexp_replace(cast(to_date(add_months(now(),-1)) as string),'-','') and regexp_replace(cast(to_date(date_sub(now(),1)) as string),'-','')   -- impala
	-- where sdt between regexp_replace(add_months(current_date,-1),'-','') and regexp_replace(date_sub(current_date,1),'-','')   -- hive
		and channel_code in ('1','7','9')
		and business_type_code =1 -- 仅日配 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
		and delivery_type_code in (1) -- 剔除直送和自提 配送类型编码：1-配送 2-直送 3-自提 4-直通 11-同城配送 12-快递配送 13-一件代发
		--and order_channel_code not in (5) -- 剔除调价返利和价格补救 订单来源渠道: 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
		--and order_channel_detail_code in (11,12) -- 11系统手工单 12小程序大宗单 25客户返利 26价格补救 27客户调价
		and inventory_dc_code in('W0R9','W0A5','W0N0','W0W7','W0X6','W0T1','W0N1','W0AS','W0A8','W0F4','W0L3','WB56','W0AH','W0G9','WA96','WB67','W0K6','W0BK','W0A7','W0X2','W0Z9','W0A6','W0Q2','W0A3','W0Q9','W0P8','W0A2','W0L4','W0BR','W0BH','WB95','WC53')
	) a 	
	join 
		(
		select 
			goods_code,goods_name,unit_name,classify_large_name,classify_middle_name,classify_small_name,brand_name,standard,category_small_name,spu_goods_name,goods_bar_code,business_division_name
		from 
			csx_dim.csx_dim_basic_goods
		where 
			sdt ='current'
			and (
				(classify_large_name='干货加工' and (classify_middle_name='蛋' or unit_name='KG'))
				or (classify_large_name='肉禽水产' and classify_middle_name!='预制菜')	
				or classify_large_name='蔬菜水果'			
				) --  标品
		) c on a.goods_code=c.goods_code
	join
		(
		select
			dc_code,goods_code,shop_special_goods_status,goods_status_name,stock_attribute_code,regionalized_goods_name,stock_attribute_name --1存储 2货到即配
		from
			csx_dim.csx_dim_basic_dc_goods
		where 
			sdt = 'current'
			and shop_special_goods_status in('0','7') -- 0：B 正常商品；3：H 停售；6：L 退场；7：K 永久停购；
		) d on d.dc_code=a.inventory_dc_code and d.goods_code=a.goods_code	
	left join 
		(
		select 
			shop_code,shop_name,performance_province_name,performance_city_name
		from 
			csx_dim.csx_dim_shop 
		where 
			sdt='current'
		) e on e.shop_code=a.inventory_dc_code
group by 
	e.performance_province_name,
	e.performance_city_name,
	a.inventory_dc_code,
	c.business_division_name,
	c.classify_large_name,
	c.classify_middle_name,
	c.category_small_name,
	c.spu_goods_name,
	c.brand_name,
	a.goods_code,
	c.goods_name,
	regexp_replace(d.regionalized_goods_name,'\n|\t|\r|\,|\"|\\\\n',''),
	c.unit_name,
	c.standard,
	d.goods_status_name,
	case when d.stock_attribute_code='1' then '是' else '否' end,
	c.goods_bar_code
having
	sum(a.sale_amt)>0
;
select * from csx_analyse_tmp.csx_analyse_tmp_shidiao_goods_fbp
;





-- 永辉门店市调 
drop table if exists csx_analyse_tmp.yh;
create table if not exists csx_analyse_tmp.yh as  
select 	* 
from 
(select 
	a.*,
	row_number() over (partition by location_code,product_code order by price_begin_time desc) as rank_price	
from
	(select 
		a.price_begin_time,
		c.performance_province_name,
		c.performance_city_name,
		b.location_code,
		c.shop_name as location_name,  -- 地点名称
		a.shop_code,
		a.shop_name,
		a.source_type_code,
		b.product_code,
		regexp_replace(b.product_name,'\n|\t|\r|\,|\"|\\\\n','') as product_name, 		
		a.price_date,
		a.price 
	from 
		(select * from csx_ods.csx_ods_csx_price_prod_market_research_price_di
		where substr(price_begin_time,1,10) >= '2024-04-29'
		) a 
		left join 
		(
		select * from csx_ods.csx_ods_csx_price_prod_market_research_product_df 
		where sdt='${sdt_yes}'  
		) b on a.product_id =b.id 
		left join (select * from csx_dim.csx_dim_shop where sdt='current') c on c.shop_code=b.location_code
		left join (select * from csx_dim.csx_dim_basic_goods where sdt='current') d on d.goods_code=b.product_code
	group by 
		a.price_begin_time,
		c.performance_province_name,
		c.performance_city_name,
		b.location_code,
		c.shop_name,
		a.shop_code,
		a.shop_name,
		a.source_type_code,
		b.product_code,
		regexp_replace(b.product_name,'\n|\t|\r|\,|\"|\\\\n',''), 		
		a.price_date,
		a.price
	)a 
)a 	where rank_price = 1


;

---- 通用市调 
drop table if exists csx_analyse_tmp.ty;
create table if not exists csx_analyse_tmp.ty as  
select 	* 
from 
(select 
	a.*,
	row_number() over (partition by location_code,product_code order by price_begin_time desc) as rank_price	
from
	(
	select 
	    a.price_begin_time,
		c.performance_province_name,
		c.performance_city_name,
		b.location_code,
		c.shop_name as location_name,  -- 地点名称
		a.shop_code,
		a.shop_name,
		a.source_type_code,
		b.product_code,
		regexp_replace(b.product_name,'\n|\t|\r|\,|\"|\\\\n','') as product_name, 		
		a.price_date,
		a.price 
	from 
		(select * from csx_ods.csx_ods_csx_price_prod_market_research_not_yh_price_effective_di
		where substr(price_begin_time,1,10) >= '2024-04-29'
			and (shop_code like '%w%' or shop_code like '%W%')
		) a 
		left join 
		(
		select * from csx_ods.csx_ods_csx_price_prod_market_research_product_df 
		where sdt='${sdt_yes}'  
		) b on a.product_id =b.id 
		left join (select * from csx_dim.csx_dim_shop where sdt='current') c on c.shop_code=b.location_code
		left join (select * from csx_dim.csx_dim_basic_goods where sdt='current') d on d.goods_code=b.product_code
	group by 
	    a.price_begin_time,
		c.performance_province_name,
		c.performance_city_name,
		b.location_code,
		c.shop_name,
		a.shop_code,
		a.shop_name,
		a.source_type_code,
		b.product_code,
		regexp_replace(b.product_name,'\n|\t|\r|\,|\"|\\\\n',''), 		
		a.price_date,
		a.price
	)a 
)a 	where rank_price = 1;



select 
	a.*,
	b.price as yh_price,
	c.price as ty_price
from 
(select * from csx_analyse_tmp.csx_analyse_tmp_shidiao_goods_bp
)a 
left join 
(select * from csx_analyse_tmp.yh) b on b.location_code = a.inventory_dc_code and b.product_code = a.goods_code
left join
(select * from csx_analyse_tmp.ty) c on c.location_code = a.inventory_dc_code and c.product_code = a.goods_code
