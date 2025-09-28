drop table csx_analyse_tmp.csx_analyse_tmp_goods_name_cy;
create table csx_analyse_tmp.csx_analyse_tmp_goods_name_cy
as
select	
	c.province_name,c.city_name,a.dc_code,c.shop_name,a.goods_code,a.goods_name,a.regionalized_goods_name,a.unit,a.spec,a.goods_status_name,a.division_name,
	b.classify_large_name,b.classify_middle_name,b.classify_small_name,a.brand_name
from
	(
	select
		dc_code,goods_code,
		regexp_replace(goods_name,'\n|\t|\r','') as goods_name,
		regexp_replace(regionalized_goods_name,'\n|\t|\r','') as regionalized_goods_name,
		regexp_replace(unit,'\n|\t|\r','') as unit,
		regexp_replace(spec,'\n|\t|\r','') as spec,
		goods_status_name,division_name,
		coalesce(regexp_replace(brand_name,'\n|\t|\r',''),'') as brand_name
	from
		csx_dim.csx_dim_basic_dc_goods
	where
		sdt='current'
		and dc_code in ('W0R9','W0A5','W0N0','W0AS','W0A8','W0F4','W0L3','WB56','W0AH','W0G9','WA96','WB67','W0K6','W0BK','W0A7','W0X2','W0Z9','W0A6','W0Q2','W0A3',
			'W0Q9','W0P8','W0A2','W0BR','W0BH')
		and substr(goods_status_name,1,1) in('B','K')
	) a 
	left join
		(
		select
			goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name
		from
			csx_dim.csx_dim_basic_goods
		where
			sdt='current'
		) b on b.goods_code=a.goods_code
	left join
		(
		select
			shop_code,shop_name,province_name,city_name
		from
			csx_dim.csx_dim_shop
		where
			sdt='current'
		group by 
			shop_code,shop_name,province_name,city_name
		) c on c.shop_code=a.dc_code
	join
		(
		select 
			inventory_dc_code,goods_code,sum(sale_amt) as sale_amt
		from 
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20230201' 
			and sdt<='20230420'
			and channel_code in ('1', '7', '9')
			and business_type_code=1
			and order_channel_code not in (4,6)
		group by 
			inventory_dc_code,goods_code
		having
			sum(sale_amt)>0
		) d on d.inventory_dc_code=a.dc_code and d.goods_code=a.goods_code
;
select * from csx_analyse_tmp.csx_analyse_tmp_goods_name_cy