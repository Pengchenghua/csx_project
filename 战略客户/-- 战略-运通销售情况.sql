-- 战略-运通销售情况
	select 
	    substr(sdt,1,6) smonth,
		performance_region_name,
		performance_province_name,
		performance_city_name,
		customer_name,
		customer_code,
		classify_large_name,
		classify_middle_name,
		sum(sum(sale_amt))over(partition by performance_city_name,substr(sdt,1,6) ) sale_all,
		sum(sale_amt) sale_amt,
		sum(profit) profit  
	from csx_dws.csx_dws_sale_detail_di
	where sdt>='20230101'
	 and sdt <='20240331'
     and (customer_name like '%运通%' or customer_name like '%和奥%' or customer_name like '%兴奥%')
	group by substr(sdt,1,6) ,
		performance_region_name,
		performance_province_name,
		performance_city_name,
		customer_name,
		customer_code,
		classify_large_name,
		classify_middle_name
		
		
		;
		
		
		
-- 战略-运通销售情况--转基因油
	select 
	    substr(sdt,1,6) smonth,
		performance_region_name,
		performance_province_name,
		performance_city_name,
		customer_name,
		customer_code,
		classify_large_name,
		classify_middle_name,
		goods_name,
		goods_code,
		sum(sum(sale_amt))over(partition by performance_city_name,substr(sdt,1,6) ) sale_all,
		sum(sale_amt) sale_amt,
		sum(profit) profit  
	from csx_dws.csx_dws_sale_detail_di
	where sdt>='20240101'
	 and sdt <='20240331'
     and (customer_name like '%运通%' or customer_name like '%和奥%' or customer_name like '%兴奥%')
     and goods_name like '%转基因%'
	group by substr(sdt,1,6) ,
		performance_region_name,
		performance_province_name,
		performance_city_name,
		customer_name,
		customer_code,
		classify_large_name,
		goods_name,
		goods_code,
		classify_middle_name
		;
		
		
		
				