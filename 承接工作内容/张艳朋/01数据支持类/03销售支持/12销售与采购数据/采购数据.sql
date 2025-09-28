--====================================================================================================================================
-- 采购
insert overwrite directory '/tmp/zhangyanpeng/20210113_wms_entry' row format delimited fields terminated by '\t'

select
	mon,a.province_name,receive_location_code,shop_name,a.source_type_name,a.business_type_name,a.goods_code,a.goods_name,brand_name,
	classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name,
	receive_qty,amount,avg_receive_amount,f.avg_sales_price,a.supplier_code,a.vendor_name
from
	(
	select
		a.mon,e.province_name,a.receive_location_code,e.shop_name,c.source_type_name,g.business_type_name,a.goods_code,b.goods_name,b.brand_name,
		b.classify_large_code,b.classify_large_name,b.classify_middle_code,b.classify_middle_name,b.classify_small_code,b.classify_small_name,
		a.supplier_code,d.vendor_name,
		sum(a.receive_qty) as receive_qty,
		sum(amount) as amount,
		sum(amount)/sum(a.receive_qty) as avg_receive_amount
	from
		(
		select
			substr(sdt,1,6)mon,entry_type,business_type,receive_location_code,goods_code,supplier_code,order_code,
			receive_qty,price*receive_qty as amount
		from
			csx_dw.dws_wms_r_d_entry_order_all_detail
		where
			sdt>='20200101' and sdt<='20201231'
			and (entry_type like 'P%' or business_type in ('ZN01','ZN02','ZN03','ZC01'))
		) as a
		left join --商品信息
			(
			select
				goods_id,goods_name,unit_name,brand_name,classify_large_code,classify_large_name,classify_middle_code,
				classify_middle_name,classify_small_code,classify_small_name
			from
				csx_dw.dws_basic_w_a_csx_product_m
			where
				sdt = 'current'
			) as b on b.goods_id=a.goods_code
		left join -- 是否城市服务商
			(
			select
				received_order_code,source_type,source_type_name
			from
				csx_dw.dws_scm_r_d_header_item_price
			where
				sdt>= '20190101'
				and super_class !='2'
			group by 
				received_order_code,source_type,source_type_name
			) as c on c.received_order_code=a.order_code
		left join -- 供应商信息
			(
			select
				vendor_id,vendor_name
			from
				csx_dw.dws_basic_w_a_csx_supplier_m
			where
				sdt = 'current'
			group by
				vendor_id,vendor_name
			)as d on regexp_replace(a.supplier_code,'^0*','') = d.vendor_id
		left join -- DC信息
			(
			select
				shop_id,shop_name,province_code,province_name
			from
				csx_dw.shop_m
			where
				sdt = 'current' 
			group by 
				shop_id,shop_name,province_code,province_name
			)e on a.receive_location_code = e.shop_id
		left join -- 业务类型码表
			(
			select
				business_type_code,business_type_name
			from
				csx_dw.dws_wms_w_a_business_type
			--where
			--	sdt = 'current' 
			group by 
				business_type_code,business_type_name
			)g on g.business_type_code = a.business_type
	where
		c.source_type !=4 or c.source_type is null
	group by 
		a.mon,e.province_name,a.receive_location_code,e.shop_name,c.source_type_name,g.business_type_name,a.goods_code,b.goods_name,b.brand_name,
		b.classify_large_code,b.classify_large_name,b.classify_middle_code,b.classify_middle_name,b.classify_small_code,b.classify_small_name,
		a.supplier_code,d.vendor_name
	) a	
	left join
		(
		select
			province_name,goods_code,
			sum(sales_value) as sales_value,
			sum(sales_qty) as sales_qty,
			sum(sales_value)/sum(sales_qty) as avg_sales_price
		from
			csx_dw.dws_sale_r_d_detail
		where
			sdt>='20200101' and sdt<='20201231'
			and channel_code in ('1','2','7','9')
			and business_type_code !='4'
		group by 
			province_name,goods_code
		) as f on f.province_name=a.province_name and f.goods_code=a.goods_code
				