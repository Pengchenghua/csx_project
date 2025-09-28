select
	substr(order_time,1,7) as smonth,province_name,city_group_name,count(*),sum(sales_value)
from
	csx_dw.dws_sale_r_d_detail
where
	substr(order_time,1,7)>='2022-01'
	and channel_code in('1','7','9')
	and business_type_code in('1')
	and return_flag <>'X'
	and sdt<='20220526'
group by
	substr(order_time,1,7),province_name,city_group_name