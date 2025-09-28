-- 销售临时表
drop table csx_analyse_tmp.csx_analyse_tmp_sale_pingu_00;
create table csx_analyse_tmp.csx_analyse_tmp_sale_pingu_00
as
select 
	a.*,b.customer_name,c.classify_large_name,c.classify_middle_name,c.category_small_name,c.spu_goods_name,c.brand_name,c.goods_name,c.unit_name,c.standard,d.goods_status_name,
	d.stock_attribute_code,b.first_category_name,b.second_category_name,b.third_category_name,e.performance_province_name
from
	(
	select 
		sdt,goods_code,customer_code,inventory_dc_code,order_code,sale_amt,sale_qty,profit
	from 
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt between '${start_day}' and '${end_day}'
		and channel_code in ('1','7','9')
		and business_type_code =1
		and delivery_type_code in (1) -- 剔除直送和自提 配送类型编码：1-配送 2-直送 3-自提 4-直通 11-同城配送 12-快递配送 13-一件代发
		-- and order_channel_code not in (5) -- 剔除调价返利和价格补救 订单来源渠道: 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
		--and order_channel_detail_code in (11,12) -- 11系统手工单 12小程序大宗单 25客户返利 26价格补救 27客户调价
		and inventory_dc_code in('W0BR')
		--and((inventory_dc_code in('W0R9') and delivery_type_code not in (2,3)) 
		--	or (inventory_dc_code in('W0A5')))
	) a 
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
	left join
		(
		select
			shop_code,performance_province_name
		from
			csx_dim.csx_dim_shop
		where
			sdt='current'
		) e on e.shop_code=a.inventory_dc_code
;
--商品明细
drop table csx_analyse_tmp.csx_analyse_tmp_pingu_goods;
create table csx_analyse_tmp.csx_analyse_tmp_pingu_goods
as
select
	a.performance_province_name,
	a.inventory_dc_code,
	a.classify_large_name,
	a.classify_middle_name,
	--a.classify_small_name,
	a.category_small_name,
	a.spu_goods_name,
	a.brand_name,
	a.goods_code,
	a.goods_name,
	a.unit_name,
	a.standard,
	a.goods_status_name,
	case when a.stock_attribute_code='1' then '是' else '否' end as is_beihuo_goods,
	sum(a.sale_amt) sale_amt,
	sum(a.sale_qty) sale_qty,
	sum(a.profit)/abs(sum(a.sale_amt)) as profit_rate,
	count(distinct a.customer_code) customer_cnt,
	count(distinct a.sdt) as day_cnt,
	count(a.goods_code) as goods_cnt,
	--
	sum(case when a.sdt between '${first_start_day}' and '${first_end_day}' then a.sale_amt end) sale_amt,
	sum(case when a.sdt between '${first_start_day}' and '${first_end_day}' then a.sale_qty end) sale_qty,
	sum(case when a.sdt between '${first_start_day}' and '${first_end_day}' then a.profit end)/abs(sum(case when a.sdt between '${first_start_day}' and '${first_end_day}' then a.sale_amt end)) as profit_rate,
	count(distinct case when a.sdt between '${first_start_day}' and '${first_end_day}' then a.customer_code end) customer_cnt,
	count(distinct case when a.sdt between '${first_start_day}' and '${first_end_day}' then a.sdt end) as day_cnt,
	count(case when a.sdt between '${first_start_day}' and '${first_end_day}' then a.goods_code end) as goods_cnt,
	--
	sum(case when a.sdt between '${second_start_day}' and '${second_end_day}' then a.sale_amt end) sale_amt,
	sum(case when a.sdt between '${second_start_day}' and '${second_end_day}' then a.sale_qty end) sale_qty,
	sum(case when a.sdt between '${second_start_day}' and '${second_end_day}' then a.profit end)/abs(sum(case when a.sdt between '${second_start_day}' and '${second_end_day}' then a.sale_amt end)) as profit_rate,
	count(distinct case when a.sdt between '${second_start_day}' and '${second_end_day}' then a.customer_code end) customer_cnt,
	count(distinct case when a.sdt between '${second_start_day}' and '${second_end_day}' then a.sdt end) as day_cnt,
	count(case when a.sdt between '${second_start_day}' and '${second_end_day}' then a.goods_code end) as goods_cnt,
	--
	sum(case when a.sdt between '${third_start_day}' and '${third_end_day}' then a.sale_amt end) sale_amt,
	sum(case when a.sdt between '${third_start_day}' and '${third_end_day}' then a.sale_qty end) sale_qty,
	sum(case when a.sdt between '${third_start_day}' and '${third_end_day}' then a.profit end)/abs(sum(case when a.sdt between '${third_start_day}' and '${third_end_day}' then a.sale_amt end)) as profit_rate,
	count(distinct case when a.sdt between '${third_start_day}' and '${third_end_day}' then a.customer_code end) customer_cnt,
	count(distinct case when a.sdt between '${third_start_day}' and '${third_end_day}' then a.sdt end) as day_cnt,
	count(case when a.sdt between '${third_start_day}' and '${third_end_day}' then a.goods_code end) as goods_cnt
from 
	csx_analyse_tmp.csx_analyse_tmp_sale_pingu_00 a 
group by 
	a.performance_province_name,
	a.inventory_dc_code,
	a.classify_large_name,
	a.classify_middle_name,
	--a.classify_small_name,
	a.category_small_name,
	a.spu_goods_name,
	a.brand_name,
	a.goods_code,
	a.goods_name,
	a.unit_name,
	a.standard,
	a.goods_status_name,
	case when a.stock_attribute_code='1' then '是' else '否' end
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
	a.first_category_name,
	a.second_category_name,
	a.third_category_name,
	a.customer_code,
	a.customer_name,
	a.goods_code,
	a.goods_name,
	sum(a.sale_amt) sale_amt,
	sum(a.sale_qty) sale_qty,
	count(distinct a.sdt) as day_cnt,
	count(a.goods_code) goods_cnt,
	sum(a.profit) as profit,
	--
	sum(case when a.sdt between '${first_start_day}' and '${first_end_day}' then a.sale_amt end) sale_amt,
	sum(case when a.sdt between '${first_start_day}' and '${first_end_day}' then a.sale_qty end) sale_qty,
	count(distinct case when a.sdt between '${first_start_day}' and '${first_end_day}' then a.sdt end) as day_cnt,
	count(case when a.sdt between '${first_start_day}' and '${first_end_day}' then a.goods_code end) goods_cnt,
	sum(case when a.sdt between '${first_start_day}' and '${first_end_day}' then a.profit end) as profit,
	--
	sum(case when a.sdt between '${second_start_day}' and '${second_end_day}' then a.sale_amt end) sale_amt,
	sum(case when a.sdt between '${second_start_day}' and '${second_end_day}' then a.sale_qty end) sale_qty,
	count(distinct case when a.sdt between '${second_start_day}' and '${second_end_day}' then a.sdt end) as day_cnt,
	count(case when a.sdt between '${second_start_day}' and '${second_end_day}' then a.goods_code end) goods_cnt,
	sum(case when a.sdt between '${second_start_day}' and '${second_end_day}' then a.profit end) as profit,
	--
	sum(case when a.sdt between '${third_start_day}' and '${third_end_day}' then a.sale_amt end) sale_amt,
	sum(case when a.sdt between '${third_start_day}' and '${third_end_day}' then a.sale_qty end) sale_qty,
	count(distinct case when a.sdt between '${third_start_day}' and '${third_end_day}' then a.sdt end) as day_cnt,
	count(case when a.sdt between '${third_start_day}' and '${third_end_day}' then a.goods_code end) goods_cnt,
	sum(case when a.sdt between '${third_start_day}' and '${third_end_day}' then a.profit end) as profit
from 
	csx_analyse_tmp.csx_analyse_tmp_sale_pingu_00 a 
group by 
	a.performance_province_name,
	a.inventory_dc_code,
	a.first_category_name,
	a.second_category_name,
	a.third_category_name,
	a.customer_code,
	a.customer_name,
	a.goods_code,
	a.goods_name
;
select * from csx_analyse_tmp.csx_analyse_tmp_pingu_customer
