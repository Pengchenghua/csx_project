-- =============================================================================================================================================================================

drop table if exists csx_analyse_tmp.csx_analyse_tmp_pl_cy_00;
create table csx_analyse_tmp.csx_analyse_tmp_pl_cy_00
as 
select 
	substr(a.sdt,1,6) as month,
	a.performance_region_name,
	a.performance_province_name,
	a.business_type_name,
	c.classify_large_name,
	c.classify_middle_name,
	a.customer_code,
	e.customer_name,
	e.first_category_name,
	e.second_category_name,
	e.third_category_name,
	a.inventory_dc_code,
	nvl(sum(a.sale_amt),0) as all_sale_amt,
	nvl(sum(a.profit),0) as all_profit,
	nvl(sum(case when a.delivery_type_code=2 then a.sale_amt end),0) as zs_sale_amt,
	nvl(sum(case when a.delivery_type_code=2 then a.profit end),0) as zs_profit,
	nvl(sum(case when a.order_channel_code=6 then a.sale_amt end),0) as tj_sale_amt,
	nvl(sum(case when a.order_channel_code=6 then a.profit end),0) as tj_profit,
	nvl(sum(case when a.order_channel_code=4 then a.sale_amt end),0) as fl_sale_amt,
	nvl(sum(case when a.order_channel_code=4 then a.profit end),0) as fl_profit,
	nvl(sum(case when a.refund_order_flag=1 then a.sale_amt end),0) as td_sale_amt,
	nvl(sum(case when a.refund_order_flag=1 then a.profit end),0) as td_profit
from 
	(
	select 
		* 
	from 
		csx_dws.csx_dws_sale_detail_di 
	where 
		sdt>='20230101' and sdt<='20230930'
		and channel_code in('1','7','9')
		and business_type_code in (1) 
	) a 
	join (select * from csx_dim.csx_dim_shop where sdt='current' and shop_low_profit_flag !=1 ) b on a.inventory_dc_code=b.shop_code 
	left join (select * from csx_dim.csx_dim_basic_goods where sdt='current') c on a.goods_code=c.goods_code 
	left join (select * from csx_dws.csx_dws_crm_customer_business_active_di where sdt='current') d on a.customer_code=d.customer_code and a.business_type_code=d.business_type_code 
	left join (select * from csx_dim.csx_dim_crm_customer_info where sdt='current') e on a.customer_code=e.customer_code 
	left join (select * from csx_dim.csx_dim_basic_date) f on f.calday=a.sdt
group by 
	substr(a.sdt,1,6),
	a.performance_region_name,
	a.performance_province_name,
	a.business_type_name,
	c.classify_large_name,
	c.classify_middle_name,
	a.customer_code,
	e.customer_name,
	e.first_category_name,
	e.second_category_name,
	e.third_category_name,
	a.inventory_dc_code
;
select * from csx_analyse_tmp.csx_analyse_tmp_pl_cy_00