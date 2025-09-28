

select
	a.customer_code,a.customer_name,a.order_code,a.order_remarks,b.sale_amt
from
	(
	select 
		a.customer_code,c.customer_name,a.order_code,a.order_remarks
	from
		(
		select 
			distinct customer_code,order_code,inventory_dc_code,order_remarks
		from 
			csx_dwd.csx_dwd_oms_sale_order_detail_di 
		where 
			sdt>='20221001' 
			and sdt<='20230430' 
			and delivery_type_code=2 
			and order_remarks !='' 
		) a 
		join
			(
			select
				*
			from
				csx_dim.csx_dim_shop
			where
				sdt='current'
				and performance_province_name='上海松江'
			) b on b.shop_code=a.inventory_dc_code
		left join	
			(
			select
				customer_code,customer_name
			from
				csx_dim.csx_dim_crm_customer_info
			where
				sdt='current'
			) c on c.customer_code=a.customer_code
	group by 
		a.customer_code,c.customer_name,a.order_code,a.order_remarks
	) a 
	left join	
		(
		select
			order_code,sum(sale_amt) as sale_amt
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20220901' and sdt<='20230522'
			and channel_code in('1','7','9')
		group by 
			order_code
		) b on b.order_code=a.order_code
			