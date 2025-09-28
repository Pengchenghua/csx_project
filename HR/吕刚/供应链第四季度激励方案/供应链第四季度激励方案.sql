--供应链第四季度激励方案
select 
	concat('20231001-','20231231') as sdt_s,
	a.performance_region_name,
	a.performance_province_name,
	sum(a.profit) as profit -- 不含税
from 
	(
	select 
		* 
	from 
		csx_dws.csx_dws_sale_detail_di 
	where 
		sdt>='20231001' 
		and sdt<='20231231'
		and channel_code in('1','7','9')
		and business_type_code in (1) 
	) a 
	join (select * from csx_dim.csx_dim_shop where sdt='current' and shop_low_profit_flag !=1 ) b on a.inventory_dc_code=b.shop_code 
group by 
	a.performance_region_name,
	a.performance_province_name
