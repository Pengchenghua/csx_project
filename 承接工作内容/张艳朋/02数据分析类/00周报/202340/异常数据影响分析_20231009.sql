-- 明细
drop table if exists csx_analyse_tmp.csx_analyse_tmp_profit_week_detail;
create table csx_analyse_tmp.csx_analyse_tmp_profit_week_detail
as
select 
	substr(a.sdt,1,6) as month,
	f.csx_week,
	a.performance_region_code,
	a.performance_region_name,
	a.performance_province_code,
	a.performance_province_name,
	a.performance_city_code,
	a.performance_city_name,
	a.business_type_code,
	a.business_type_name,
	c.classify_large_code,
	c.classify_large_name,
	c.classify_middle_code,
	c.classify_middle_name,
	a.customer_code,
	e.customer_name,
	d.first_business_sale_date,
	e.first_category_name,
	e.second_category_name,
	e.third_category_name,
	a.inventory_dc_code,
	(case when b.shop_code is not null then '是' else '否' end) as if_zs_dc,
	nvl(sum(a.profit),0) as all_profit,
	nvl(sum(a.sale_amt),0) as all_sale_amt,
	nvl(sum(case when a.delivery_type_code=2 then a.profit end),0) as zs_profit,
	nvl(sum(case when a.delivery_type_code=2 then a.sale_amt end),0) as zs_sale_amt,
	nvl(sum(case when a.order_channel_code=6 then a.profit end),0) as tj_profit,
	nvl(sum(case when a.order_channel_code=6 then a.sale_amt end),0) as tj_sale_amt,
	nvl(sum(case when a.order_channel_code=4 then a.profit end),0) as fl_profit,
	nvl(sum(case when a.order_channel_code=4 then a.sale_amt end),0) as fl_sale_amt,
	nvl(sum(case when a.refund_order_flag=1 then a.profit end),0) as td_profit,
	nvl(sum(case when a.refund_order_flag=1 then a.sale_amt end),0) as td_sale_amt,
	nvl(sum(case when a.channel_code in (1,7,9) and a.business_type_code=3 then a.profit end),0) as cq_profit,
	nvl(sum(case when a.channel_code in (1,7,9) and a.business_type_code=3 then a.sale_amt end),0) as cq_sale_amt 
from 
	(
	select 
		* 
	from 
		csx_dws.csx_dws_sale_detail_di 
	where 
		sdt>='20230901' and sdt<='20231008'
		and channel_code in('1','7','9')
		and business_type_code in (1) 
		and performance_region_name in ('华北大区')
	) a 
	left join (select * from csx_dim.csx_dim_shop where sdt='current' and shop_low_profit_flag=1 ) b on a.inventory_dc_code=b.shop_code 
	left join (select * from csx_dim.csx_dim_basic_goods where sdt='current') c on a.goods_code=c.goods_code 
	left join (select * from csx_dws.csx_dws_crm_customer_business_active_di where sdt='current') d on a.customer_code=d.customer_code and a.business_type_code=d.business_type_code 
	left join (select * from csx_dim.csx_dim_crm_customer_info where sdt='current') e on a.customer_code=e.customer_code 
	left join (select * from csx_dim.csx_dim_basic_date) f on f.calday=a.sdt
where
	b.shop_code is null
group by 
	substr(a.sdt,1,6),
	f.csx_week,
	a.performance_region_code,
	a.performance_region_name,
	a.performance_province_code,
	a.performance_province_name,
	a.performance_city_code,
	a.performance_city_name,
	a.business_type_code,
	a.business_type_name,
	c.classify_large_code,
	c.classify_large_name,
	c.classify_middle_code,
	c.classify_middle_name,
	a.customer_code,
	e.customer_name,
	e.first_category_name,
	e.second_category_name,
	e.third_category_name,
	a.inventory_dc_code,
	(case when b.shop_code is not null then '是' else '否' end),
	d.first_business_sale_date 
;
select * from csx_analyse_tmp.csx_analyse_tmp_profit_week_detail;


-- 明细
drop table if exists csx_analyse_tmp.csx_analyse_tmp_profit_week_detail_2;
create table csx_analyse_tmp.csx_analyse_tmp_profit_week_detail_2
as
select 
	substr(a.sdt,1,6) as month,
	f.csx_week,
	a.performance_region_code,
	a.performance_region_name,
	a.performance_province_code,
	a.performance_province_name,
	a.performance_city_code,
	a.performance_city_name,
	a.business_type_code,
	a.business_type_name,
	c.classify_large_code,
	c.classify_large_name,
	c.classify_middle_code,
	c.classify_middle_name,
	a.customer_code,
	e.customer_name,
	d.first_business_sale_date,
	e.first_category_name,
	e.second_category_name,
	e.third_category_name,
	a.inventory_dc_code,
	(case when b.shop_code is not null then '是' else '否' end) as if_zs_dc,
	nvl(sum(a.profit),0) as all_profit,
	nvl(sum(a.sale_amt),0) as all_sale_amt,
	nvl(sum(case when a.delivery_type_code=2 then a.profit end),0) as zs_profit,
	nvl(sum(case when a.delivery_type_code=2 then a.sale_amt end),0) as zs_sale_amt,
	nvl(sum(case when a.order_channel_code=6 then a.profit end),0) as tj_profit,
	nvl(sum(case when a.order_channel_code=6 then a.sale_amt end),0) as tj_sale_amt,
	nvl(sum(case when a.order_channel_code=4 then a.profit end),0) as fl_profit,
	nvl(sum(case when a.order_channel_code=4 then a.sale_amt end),0) as fl_sale_amt,
	nvl(sum(case when a.refund_order_flag=1 then a.profit end),0) as td_profit,
	nvl(sum(case when a.refund_order_flag=1 then a.sale_amt end),0) as td_sale_amt,
	nvl(sum(case when a.channel_code in (1,7,9) and a.business_type_code=3 then a.profit end),0) as cq_profit,
	nvl(sum(case when a.channel_code in (1,7,9) and a.business_type_code=3 then a.sale_amt end),0) as cq_sale_amt 
from 
	(
	select 
		* 
	from 
		csx_dws.csx_dws_sale_detail_di 
	where 
		((sdt>='20230901' and sdt<='20230930') or (sdt>='20230801' and sdt<='20230831'))
		and channel_code in('1','7','9')
		and business_type_code in (1) 
		and performance_region_name in ('华北大区')
	) a 
	left join (select * from csx_dim.csx_dim_shop where sdt='current' and shop_low_profit_flag=1 ) b on a.inventory_dc_code=b.shop_code 
	left join (select * from csx_dim.csx_dim_basic_goods where sdt='current') c on a.goods_code=c.goods_code 
	left join (select * from csx_dws.csx_dws_crm_customer_business_active_di where sdt='current') d on a.customer_code=d.customer_code and a.business_type_code=d.business_type_code 
	left join (select * from csx_dim.csx_dim_crm_customer_info where sdt='current') e on a.customer_code=e.customer_code 
	left join (select * from csx_dim.csx_dim_basic_date) f on f.calday=a.sdt
where
	b.shop_code is null
group by 
	substr(a.sdt,1,6),
	f.csx_week,
	a.performance_region_code,
	a.performance_region_name,
	a.performance_province_code,
	a.performance_province_name,
	a.performance_city_code,
	a.performance_city_name,
	a.business_type_code,
	a.business_type_name,
	c.classify_large_code,
	c.classify_large_name,
	c.classify_middle_code,
	c.classify_middle_name,
	a.customer_code,
	e.customer_name,
	e.first_category_name,
	e.second_category_name,
	e.third_category_name,
	a.inventory_dc_code,
	(case when b.shop_code is not null then '是' else '否' end),
	d.first_business_sale_date 
;
select * from csx_analyse_tmp.csx_analyse_tmp_profit_week_detail_2;

-- 直送单据
--	select 
--		customer_code,customer_name,direct_delivery_type,sum(sale_amt),sum(profit)
--	from 
--		csx_dws.csx_dws_sale_detail_di 
--	where 
--		sdt>='20230812' and sdt<='20230818'
--		and channel_code in('1','7','9')
--		and business_type_code in (1) 
--		and performance_region_name in ('华北大区')
--		and performance_province_name in ('北京市')
--		and delivery_type_code=2
--		and inventory_dc_code not in ('WB26')
--		and customer_code in ('223283','232083','130239','128359')
--	group by 
--		customer_code,customer_name,direct_delivery_type
-- 131187 131162

drop table if exists csx_analyse_tmp.csx_analyse_tmp_zhisong_beijing_00;
create table csx_analyse_tmp.csx_analyse_tmp_zhisong_beijing_00
as
select 
	performance_province_name,inventory_dc_code,customer_code,customer_name,direct_delivery_type,
	case when direct_delivery_type=0 then '普通'
		when direct_delivery_type=1 then 'RD'
		when direct_delivery_type=2 then 'ZZ'
		when direct_delivery_type=11 then '临时加单'
		when direct_delivery_type=12 then '紧急补货' 
	end as direct_delivery_type_name,
	classify_middle_name,
	sum(sale_amt),sum(profit)
from 
	csx_dws.csx_dws_sale_detail_di 
where 
	sdt>='20230930' and sdt<='20231006'
	and channel_code in('1','7','9')
	and business_type_code in (1) 
	and performance_province_name in ('北京市')
	and delivery_type_code=2
	and inventory_dc_code not in ('WB26')
	-- and customer_code in ('130998','130949','130992')
group by 
	performance_province_name,inventory_dc_code,customer_code,customer_name,direct_delivery_type,
	case when direct_delivery_type=0 then '普通'
		when direct_delivery_type=1 then 'RD'
		when direct_delivery_type=2 then 'ZZ'
		when direct_delivery_type=11 then '临时加单'
		when direct_delivery_type=12 then '紧急补货' 
	end,
	classify_middle_name
;
select * from csx_analyse_tmp.csx_analyse_tmp_zhisong_beijing_00
