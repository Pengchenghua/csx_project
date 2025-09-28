
drop table if exists csx_analyse_tmp.csx_analyse_tmp_sale_bj_cq;
create table csx_analyse_tmp.csx_analyse_tmp_sale_bj_cq
as
select 
	performance_province_name,inventory_dc_code,business_type_name,
	sum(sale_amt) as sale_amt,
	sum(profit) as profit
from 
	csx_dws.csx_dws_sale_detail_di
where 
	sdt>='20230301' and sdt<='20230328'
	and channel_code in('1','7','9','2')
	and business_type_code in (1,2,4,6,9)
	and performance_province_name in ('北京市','重庆市')
group by 
	performance_province_name,inventory_dc_code,business_type_name
;
select * from csx_analyse_tmp.csx_analyse_tmp_sale_bj_cq
	