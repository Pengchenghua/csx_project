--商品明细
drop table csx_analyse_tmp.csx_analyse_tmp_pingu_goods;
create table csx_analyse_tmp.csx_analyse_tmp_pingu_goods
as
select
	a.performance_province_name,
	a.inventory_dc_code,
	c.classify_large_name,
	c.classify_middle_name,
	--c.classify_small_name,
	c.category_small_name,
	c.spu_goods_name,
	c.brand_name,
	a.goods_code,
	c.goods_name,
	c.unit_name,
	c.standard,
	d.goods_status_name,
	case when d.stock_attribute_code='1' then '是' else '否' end as is_beihuo_goods,
	sum(a.sale_amt) sale_amt,
	sum(a.sale_qty) sale_qty,
	sum(a.profit)/abs(sum(a.sale_amt)) as profit_rate,
	count(distinct a.customer_code) customer_cnt,
	count(distinct a.sdt) as day_cnt,
	count(a.goods_code) as goods_cnt,
	-- 
	sum(case when a.sdt between '20230701' and '20230731' then a.sale_amt end) sale_amt,
	sum(case when a.sdt between '20230701' and '20230731' then a.sale_qty end) sale_qty,
	sum(case when a.sdt between '20230701' and '20230731' then a.profit end)/abs(sum(case when a.sdt between '20230701' and '20230731' then a.sale_amt end)) as profit_rate,
	count(distinct case when a.sdt between '20230701' and '20230731' then a.customer_code end) customer_cnt,
	count(distinct case when a.sdt between '20230701' and '20230731' then a.sdt end) as day_cnt,
	count(case when a.sdt between '20230701' and '20230731' then a.goods_code end) as goods_cnt
from 
	(
	select 
		performance_province_name,sdt,goods_code,customer_code,inventory_dc_code,order_code,sale_amt,sale_qty,profit
	from 
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt between '20230501' and '20230731'
		and channel_code in ('1','7','9')
		and business_type_code =1 -- 仅日配 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
		and delivery_type_code in (1) -- 剔除直送和自提 配送类型编码：1-配送 2-直送 3-自提 4-直通 11-同城配送 12-快递配送 13-一件代发
		--and order_channel_code not in (4,5,6) -- 剔除调价返利和价格补救 订单来源渠道: 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
		and inventory_dc_code in('W0A2')
	)a  
	left join
		(
		select 
			customer_code,customer_name
		from 
			csx_dim.csx_dim_crm_customer_info
		where 
			sdt = 'current'
		) b on b.customer_code=a.customer_code		
	left join   --商品表
		(
		select 
			goods_code,goods_name,unit_name,classify_large_name,classify_middle_name,classify_small_name,brand_name,standard,category_small_name,spu_goods_name
		from 
			csx_dim.csx_dim_basic_goods
		where 
			sdt ='current'
		) c on a.goods_code=c.goods_code
	left join
		(
		select
			dc_code,goods_code,goods_status_name,stock_attribute_code,stock_attribute_name --1存储 2货到即配
		from
			csx_dim.csx_dim_basic_dc_goods
		where 
			sdt = 'current'
		) d on d.dc_code=a.inventory_dc_code and d.goods_code=a.goods_code
group by 
	a.performance_province_name,
	a.inventory_dc_code,
	c.classify_large_name,
	c.classify_middle_name,
	--c.classify_small_name,
	c.category_small_name,
	c.spu_goods_name,
	c.brand_name,
	a.goods_code,
	c.goods_name,
	c.unit_name,
	c.standard,
	d.goods_status_name,
	case when d.stock_attribute_code='1' then '是' else '否' end
;
select * from csx_analyse_tmp.csx_analyse_tmp_pingu_goods
;


-- 客户明细
drop table csx_analyse_tmp.csx_analyse_tmp_pingu_customer;
create table csx_analyse_tmp.csx_analyse_tmp_pingu_customer
as
select
	a.performance_province_name,
	a.inventory_dc_code,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	a.customer_code,
	b.customer_name,
	a.goods_code,
	c.goods_name,
	sum(a.sale_amt) sale_amt,
	sum(a.sale_qty) sale_qty,
	count(distinct a.sdt) as day_cnt,
	count(a.goods_code) goods_cnt,
	sum(a.profit) as profit,
	--
	sum(case when a.sdt between '20230701' and '20230731' then a.sale_amt end) sale_amt,
	sum(case when a.sdt between '20230701' and '20230731' then a.sale_qty end) sale_qty,
	count(distinct case when a.sdt between '20230701' and '20230731' then a.sdt end) as day_cnt,
	count(case when a.sdt between '20230701' and '20230731' then a.goods_code end) goods_cnt,
	sum(case when a.sdt between '20230701' and '20230731' then a.profit end) as profit
from 
	(
	select 
		performance_province_name,inventory_dc_code,sdt,goods_code,customer_code,order_code,sale_amt,sale_qty,profit
	from 
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt between '20230501' and '20230731'
		and channel_code in ('1','7','9')
		and business_type_code =1 -- 仅日配 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
		and delivery_type_code in (1) -- 剔除直送和自提 配送类型编码：1-配送 2-直送 3-自提 4-直通 11-同城配送 12-快递配送 13-一件代发
		--and order_channel_code not in (4,5,6) -- 剔除调价返利和价格补救 订单来源渠道: 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
		and inventory_dc_code in('W0A2')
	)a 
	left join
		(
		select 
			customer_code,customer_name,first_category_name,second_category_name,third_category_name
		from 
			csx_dim.csx_dim_crm_customer_info
		where 
			sdt = 'current'
		) b on b.customer_code=a.customer_code
	left join   --商品表
		(
		select 
			goods_code,goods_name,unit_name,classify_large_name,classify_middle_name,classify_small_name,category_small_name,spu_goods_name
		from 
			csx_dim.csx_dim_basic_goods
		where 
			sdt ='current'
		) c on a.goods_code=c.goods_code
group by 
	a.performance_province_name,
	a.inventory_dc_code,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	a.customer_code,
	b.customer_name,
	a.goods_code,
	c.goods_name
;
select * from csx_analyse_tmp.csx_analyse_tmp_pingu_customer

