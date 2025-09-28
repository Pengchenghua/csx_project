select 
	count(*) 
from 
	(	
	select
		performance_province_name,inventory_dc_code,goods_code,sum(sale_amt) as sale_amt
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20230223' and sdt<='20230522'
		and channel_code in('1','7','9')
		and inventory_dc_code in('W0R9','W0A5','W0N0','W0W7','W0X6','W0N1','W0AS','W0A8','W0F4','W0L3','WB56','W0AH','W0G9','WA96','WB67','W0K6','W0BK','W0A7','W0X2',
		'W0Z9','W0A6','W0Q2','W0A3','W0Q9','W0P8','W0A2','W0BR','W0BH')
	group by 
		performance_province_name,inventory_dc_code,goods_code
	having
		sum(sale_amt)>0 
	) a 