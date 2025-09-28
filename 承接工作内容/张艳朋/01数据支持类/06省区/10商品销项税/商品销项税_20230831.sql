select 
	goods_code,
	goods_name,
	goods_bar_code,
	concat(tax_code,'-',tax_rate,'%'),
	goods_status_name 
from 
	csx_dim.csx_dim_basic_dc_goods 
where 
	sdt='current' 
	and dc_code='W0P8' 
	and shop_special_goods_status='0' 
	and business_type='0' -- 自营
