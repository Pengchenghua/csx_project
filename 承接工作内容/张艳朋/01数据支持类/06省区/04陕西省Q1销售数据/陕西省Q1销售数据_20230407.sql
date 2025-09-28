		select 
			performance_province_name,performance_city_name,business_type_name,customer_code,customer_name,
			if(refund_order_flag=1,'逆向单','正向单') as refund_order_flag_name,
			case when order_channel_code =4 then '返利' when order_channel_code =6 then '调价' else '非调价返利' end as order_channel_name,
			sum(sale_amt) as sale_amt,sum(profit) as profit,sum(profit)/abs(sum(sale_amt)) as profit_rate
		from 
			-- csx_dw.dws_sale_r_d_detail 
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20230101' 
			and sdt<='20230331' 
			and business_type_code in(1)
			and performance_province_name='陕西省'
		group by 
            performance_province_name,performance_city_name,business_type_name,customer_code,customer_name,
			if(refund_order_flag=1,'逆向单','正向单'),
			case when order_channel_code =4 then '返利' when order_channel_code =6 then '调价' else '非调价返利' end