
	select
		substr(sdt,1,6) as smonth,
		performance_province_name,
		performance_city_name,
		business_type_name,
		sum(sale_amt) as sale_amt
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20220901' and sdt<='20230131'
		and channel_code in('1','7','9')
		and business_type_code in (1,2,4)
	group by 
		substr(sdt,1,6),
		performance_province_name,
		performance_city_name,
		business_type_name		