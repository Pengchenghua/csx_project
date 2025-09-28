		select 
			performance_province_name,customer_code,customer_name,business_type_name,delivery_type_name,
			sum(sale_amt) as sale_amt,sum(profit) as profit
		from 
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20230401' and sdt<='20230426'
			and channel_code in('1','7','9')
			-- and business_type_code=1
			-- and order_channel_code not in(4,6) -- 剔除调价返利
			and customer_code in('128637','129910','129874','129974')
		group by 
            performance_province_name,customer_code,customer_name,business_type_name,delivery_type_name