
drop table csx_analyse_tmp.csx_analyse_tmp_dc_product_pool_00;
create table csx_analyse_tmp.csx_analyse_tmp_dc_product_pool_00
as		
select
	c.performance_province_name,
	c.performance_city_name,
	a.inventory_dc_code,
	c.shop_name,
	a.product_code,
	b.goods_name,
	b.unit_name,
	a.base_product_status,
	case a.base_product_status
		when 0 then '正常'
		when 3 then '停售'
		when 6 then '退场'
		when 7 then '停购'
	end as base_product_status_name,
	a.sync_customer_product_flag,
	case a.sync_customer_product_flag
		when 0 then '否' when 1 then '是'
	end as sync_customer_product_flag_name,
	case when a.base_product_tag=1 then '是' when a.base_product_tag=0 then '否' end as base_product_tag_name
from	
	(
	select
		*
	from
		csx_ods.csx_ods_b2b_mall_prod_yszx_dc_product_pool_df
	where
		sdt='20230820' and base_product_tag=1
	) a 
	left join   --商品表
		(
		select 
			goods_code,goods_name,unit_name,classify_large_name,classify_middle_name,classify_small_name,brand_name,standard,category_small_name,spu_goods_name,goods_bar_code
		from 
			csx_dim.csx_dim_basic_goods
		where 
			sdt ='current'
		) b on a.product_code=b.goods_code	
	left join
		(
		select
			shop_code,shop_name,performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name
		from
			csx_dim.csx_dim_shop
		where
			sdt='current'
		) c on c.shop_code=a.inventory_dc_code
;

select * from csx_analyse_tmp.csx_analyse_tmp_dc_product_pool_00