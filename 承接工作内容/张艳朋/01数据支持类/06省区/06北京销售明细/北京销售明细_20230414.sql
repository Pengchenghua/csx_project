drop table if exists csx_analyse_tmp.csx_analyse_tmp_beijing_sale;
create table csx_analyse_tmp.csx_analyse_tmp_beijing_sale
as
select 
	*
from 
	csx_dws.csx_dws_sale_detail_di
where 
	sdt>='20200101' and sdt<='20230413'
	and channel_code in('1','7','9')
	and performance_province_name='北京市'
	and to_date(order_time) between '2023-01-01' and '2023-03-31'
	and inventory_dc_code='W0A3'
	and delivery_type_code=1
	and business_type_code=1
;

select * from csx_analyse_tmp.csx_analyse_tmp_beijing_sale