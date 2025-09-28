-- 销售临时表
drop table csx_analyse_tmp.csx_analyse_tmp_sale_beijing_00;
create table csx_analyse_tmp.csx_analyse_tmp_sale_beijing_00
as
select
	performance_province_name,sdt,order_channel_detail_name,order_code,business_type_name,operation_mode_name,inventory_dc_code,delivery_type_name,
	customer_code,customer_name,a.goods_code,goods_bar_code,goods_name,brand_name,division_name,
	c.category_large_name,c.category_middle_name,c.category_small_name,
	classify_large_name,classify_middle_name,classify_small_name,unit_name,is_factory_goods_flag,
	zs_flag,price_source,
	cost_price,sale_price,sale_qty,sale_amt,	
	sale_cost,profit,profit_rate,row_number()over() as rn
from
	(
	select 
		performance_province_name,sdt,order_channel_detail_name,order_code,business_type_name,operation_mode_name,inventory_dc_code,delivery_type_name,
		customer_code,customer_name,goods_code,goods_bar_code,goods_name,brand_name,division_name,
		classify_large_name,classify_middle_name,classify_small_name,unit_name,is_factory_goods_flag,
		case when direct_delivery_type=0 then '普通'
			when direct_delivery_type=1 then '直送1'
			when direct_delivery_type=2 then '直送2'
			when direct_delivery_type=11 then '临时加单'
			when direct_delivery_type=12 then '紧急补货'
		end as zs_flag,
		'' as price_source,
		cost_price,sale_price,sale_qty,sale_amt,	
		sale_cost,profit,profit/abs(sale_amt) as profit_rate
	from 
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt between '20230801' and '20230930'
		and channel_code in ('1','7','9')
		and business_type_code =1
		and inventory_dc_code in('W0A3')
	) a 
	left join   --商品表
		(
		select 
			goods_code,category_large_name,category_middle_name,category_small_name
		from 
			csx_dim.csx_dim_basic_goods
		where 
			sdt ='current'
		) c on a.goods_code=c.goods_code
;
select * from csx_analyse_tmp.csx_analyse_tmp_sale_beijing_00 where rn<=490000;
select * from csx_analyse_tmp.csx_analyse_tmp_sale_beijing_00 where rn>490000;