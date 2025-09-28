2-4月 客户 签收 销售额 日配

drop table if exists csx_analyse_tmp.csx_analyse_tmp_rp_customer_sale;
create table csx_analyse_tmp.csx_analyse_tmp_rp_customer_sale
as

	select
		performance_region_name,performance_province_name,substr(sdt,1,6) as sale_month,customer_code,customer_name,sum(sale_amt) as sale_amt,sum(profit) as profit
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20230201' and sdt<='20230430'
		and channel_code in('1','7','9')
		and business_type_code in(1)
	group by 
		performance_region_name,performance_province_name,substr(sdt,1,6),customer_code,customer_name
select * from csx_analyse_tmp.csx_analyse_tmp_rp_customer_sale