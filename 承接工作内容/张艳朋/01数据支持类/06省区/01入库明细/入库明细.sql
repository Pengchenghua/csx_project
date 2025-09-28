drop table if exists csx_analyse_tmp.csx_analyse_tmp_goods_purchase_price;
create table csx_analyse_tmp.csx_analyse_tmp_goods_purchase_price
as			
select 
	province_name,city_name,dc_code,dc_name,business_division_code,business_division_name,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,
	classify_small_code,classify_small_name,goods_code,goods_name,
	sum(received_qty) as received_qty,
	sum(received_amt) as received_amt
from 
	csx_report.csx_report_wms_category_goods_purchase_price_df 
where 
	sdt between '20230403' and '20230409' 
	and province_name in ('北京','福建','重庆','安徽','四川') 
group by 
	province_name,city_name,dc_code,dc_name,business_division_code,business_division_name,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,
	classify_small_code,classify_small_name,goods_code,goods_name
;
select * from csx_analyse_tmp.csx_analyse_tmp_goods_purchase_price;