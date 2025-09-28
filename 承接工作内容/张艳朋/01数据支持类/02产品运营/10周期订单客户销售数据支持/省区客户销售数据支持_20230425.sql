-- ==================================================================================			
		select 
			performance_province_name,performance_city_name,count(distinct customer_code) as customer_cnt,
			sum(sale_amt) as sale_amt
		from 
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20230301' and sdt<='20230331'
			and channel_code in('1','7','9')
			and business_type_code=1
			and order_channel_code not in(4,6) -- 剔除调价返利
		group by 
            performance_province_name,performance_city_name
			
		select 
			performance_province_name,performance_city_name,count(distinct customer_code) as customer_cnt,
			sum(sale_amt) as sale_amt
		from 
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20230101' and sdt<='20230331'
			and channel_code in('1','7','9')
			and business_type_code=1
			and order_channel_code not in(4,6) -- 剔除调价返利
		group by 
            performance_province_name,performance_city_name
-- ==================================================================================			
		select 
			performance_province_name,performance_city_name,customer_code,
			sum(sale_amt) as sale_amt
		from 
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20230301' and sdt<='20230331'
			and channel_code in('1','7','9')
			and business_type_code=1
			and order_channel_code not in(4,6) -- 剔除调价返利
			and customer_code in('126377','223402','178615','104924','123443','123490','123870','123920','106226','120428','129131','124175','130721','129092','125686','121054','113698','105750','121466','123028','121308','224221','224423','113574','129481','105703','129471','129818','129811','129806','129796','129794','127054')
		group by 
            performance_province_name,performance_city_name,customer_code
			
		select 
			performance_province_name,performance_city_name,customer_code,
			sum(sale_amt) as sale_amt
		from 
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20230101' and sdt<='20230331'
			and channel_code in('1','7','9')
			and business_type_code=1
			and order_channel_code not in(4,6) -- 剔除调价返利
			and customer_code in('126377','223402','178615','104924','123443','123490','123870','123920','106226','120428','129131','124175','130721','129092','125686','121054','113698','105750','121466','123028','121308','224221','224423','113574','129481','105703','129471','129818','129811','129806','129796','129794','127054')
		group by 
            performance_province_name,performance_city_name,customer_code