drop table if exists csx_analyse_tmp.csx_analyse_tmp_huadong_sale;
create table csx_analyse_tmp.csx_analyse_tmp_huadong_sale
as
select 
	company_code,inventory_dc_code,performance_province_name,performance_city_name,sdt,goods_code,goods_name,business_division_name,purchase_group_code,purchase_group_name,
	classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name,
	sum(sale_amt) as sale_amt,
	sum(sale_amt_no_tax) as sale_amt_no_tax,
	sum(sale_cost) as sale_cost,
	sum(sale_cost_no_tax) as sale_cost_no_tax,
	sum(profit) as profit,
	sum(profit_no_tax) as profit_no_tax
from 
	csx_dws.csx_dws_sale_detail_di
where 
	sdt>='20210101' and sdt<='20211231'
	and channel_code in('1','7','9')
	and company_code='2131'
group by 
	company_code,inventory_dc_code,performance_province_name,performance_city_name,sdt,goods_code,goods_name,business_division_name,purchase_group_code,purchase_group_name,
	classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name
;
select * from csx_analyse_tmp.csx_analyse_tmp_huadong_sale
;

drop table if exists csx_analyse_tmp.csx_analyse_tmp_huadong_sale_02;
create table csx_analyse_tmp.csx_analyse_tmp_huadong_sale_02
as
select 
	company_code,inventory_dc_code,performance_province_name,performance_city_name,sdt,goods_code,goods_name,business_division_name,purchase_group_code,purchase_group_name,
	classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name,
	sum(sale_amt) as sale_amt,
	sum(sale_amt_no_tax) as sale_amt_no_tax,
	sum(sale_cost) as sale_cost,
	sum(sale_cost_no_tax) as sale_cost_no_tax,
	sum(profit) as profit,
	sum(profit_no_tax) as profit_no_tax
from 
	csx_dws.csx_dws_sale_detail_di
where 
	sdt>='20210101' and sdt<='20220131'
	and channel_code in('1','7','9','2')
	and company_code='2131'
group by 
	company_code,inventory_dc_code,performance_province_name,performance_city_name,sdt,goods_code,goods_name,business_division_name,purchase_group_code,purchase_group_name,
	classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name
;
select * from csx_analyse_tmp.csx_analyse_tmp_huadong_sale_02
	