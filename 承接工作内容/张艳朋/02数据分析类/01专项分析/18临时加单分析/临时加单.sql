--临时加单对于省区毛利率的影响
--写结论：占比 影响 哪些省区有问题 临时加单里面哪些品类占比高
--到商品维度
--
--
--调价问题最大 紧急补货最差：北京和四川  
--毛利影响 占比 北京和四川哪些品类 哪些商品 
--到商品维度
drop table csx_analyse_tmp.csx_analyse_tmp_lsjd_customer_order_sale;
create table csx_analyse_tmp.csx_analyse_tmp_lsjd_customer_order_sale
as
select
	a.performance_region_name,a.performance_province_name,a.customer_code,d.customer_name,d.first_category_name,d.second_category_name,e.customer_large_level,
	f.classify_large_name,f.classify_middle_name,a.delivery_type_name,
	sum(a.sale_amt) as sale_amt,
	sum(a.profit) as profit,
	sum(if(c.order_code is not null,a.sale_amt,0)) as lsjd_sale_amt,
	sum(if(c.order_code is not null,a.profit,0)) as lsjd_profit
from
	(
	select 
		performance_region_name,performance_province_name,customer_code,inventory_dc_code,order_code,goods_code,sale_amt,profit,delivery_type_name
	from 
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt between '20230701' and '20230731'
		and channel_code in ('1','7','9')
		and business_type_code =1 
	) a 
	join (select distinct shop_code from csx_dim.csx_dim_shop where sdt='current' and shop_low_profit_flag!=1 )b on a.inventory_dc_code = b.shop_code
	left join
		(
		select
			order_code,
			replenishment_relation_order_code, -- 补货单原单号
			additional_order_flag, -- 加单标识0-否1-是
			delivery_flag -- 配送方式类型:0-P(普通) 1-R(融单)、2-Z(过账)
		from
			csx_dwd.csx_dwd_oms_sale_order_detail_di
		where
			sdt>='20230101'
			and additional_order_flag=1
		group by 
			order_code,replenishment_relation_order_code,additional_order_flag,delivery_flag
		) c on c.order_code=a.order_code
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) d on d.customer_code=a.customer_code
	left join
		(
		select
			distinct customer_no,customer_large_level
		from 
			csx_analyse.csx_analyse_report_sale_customer_level_mf
		where
			month='202307' 
			and tag=1 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
		) e on e.customer_no=a.customer_code
	left join
		(
		select
			goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name
		from
			csx_dim.csx_dim_basic_goods
		where
			sdt='current'
		) f on f.goods_code=a.goods_code
group by 
	a.performance_region_name,a.performance_province_name,a.customer_code,d.customer_name,d.first_category_name,d.second_category_name,e.customer_large_level,
	f.classify_large_name,f.classify_middle_name,a.delivery_type_name
;		
select * from csx_analyse_tmp.csx_analyse_tmp_lsjd_customer_order_sale;

-- 客户清单
drop table csx_analyse_tmp.csx_analyse_tmp_lsjd_customer_list;
create table csx_analyse_tmp.csx_analyse_tmp_lsjd_customer_list
as
select
	performance_region_name,performance_province_name,customer_code,customer_name,first_category_name,second_category_name,customer_large_level,
	sum(sale_amt) as sale_amt,
	sum(profit) as profit,
	sum(profit)/abs(sum(sale_amt)) as profit_rate,
	sum(lsjd_sale_amt) as lsjd_sale_amt,
	sum(lsjd_profit) as lsjd_profit,
	sum(lsjd_profit)/abs(sum(lsjd_sale_amt)) as lsjd_profit_rate,
	sum(lsjd_sale_amt)/sum(sale_amt) as lsjd_rate
from
	csx_analyse_tmp.csx_analyse_tmp_lsjd_customer_order_sale
group by 
	performance_region_name,performance_province_name,customer_code,customer_name,first_category_name,second_category_name,customer_large_level
;
select * from csx_analyse_tmp.csx_analyse_tmp_lsjd_customer_list;


drop table csx_analyse_tmp.csx_analyse_tmp_lsjd_customer_order_sale_2;
create table csx_analyse_tmp.csx_analyse_tmp_lsjd_customer_order_sale_2
as
select
	a.performance_region_name,a.performance_province_name,a.customer_code,d.customer_name,d.first_category_name,d.second_category_name,d.third_category_name,e.customer_large_level,
	f.classify_large_name,f.classify_middle_name,a.goods_code,f.goods_name,
	sum(a.sale_amt) as sale_amt,
	sum(a.profit) as profit,
	sum(if(c.order_code is not null,a.sale_amt,0)) as lsjd_sale_amt,
	sum(if(c.order_code is not null,a.profit,0)) as lsjd_profit
from
	(
	select 
		performance_region_name,performance_province_name,customer_code,inventory_dc_code,order_code,goods_code,sale_amt,profit,delivery_type_name
	from 
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt between '20230701' and '20230731'
		and channel_code in ('1','7','9')
		and business_type_code =1 
	) a 
	join (select distinct shop_code from csx_dim.csx_dim_shop where sdt='current' and shop_low_profit_flag!=1 )b on a.inventory_dc_code = b.shop_code
	left join
		(
		select
			order_code,
			replenishment_relation_order_code, -- 补货单原单号
			additional_order_flag, -- 加单标识0-否1-是
			delivery_flag -- 配送方式类型:0-P(普通) 1-R(融单)、2-Z(过账)
		from
			csx_dwd.csx_dwd_oms_sale_order_detail_di
		where
			sdt>='20230101'
			and additional_order_flag=1
		group by 
			order_code,replenishment_relation_order_code,additional_order_flag,delivery_flag
		) c on c.order_code=a.order_code
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) d on d.customer_code=a.customer_code
	left join
		(
		select
			distinct customer_no,customer_large_level
		from 
			csx_analyse.csx_analyse_report_sale_customer_level_mf
		where
			month='202307' 
			and tag=1 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
		) e on e.customer_no=a.customer_code
	left join
		(
		select
			goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name
		from
			csx_dim.csx_dim_basic_goods
		where
			sdt='current'
		) f on f.goods_code=a.goods_code
group by 
	a.performance_region_name,a.performance_province_name,a.customer_code,d.customer_name,d.first_category_name,d.second_category_name,d.third_category_name,e.customer_large_level,
	f.classify_large_name,f.classify_middle_name,a.goods_code,f.goods_name
;		
select * from csx_analyse_tmp.csx_analyse_tmp_lsjd_customer_order_sale_2;


	select 
		performance_region_name,performance_province_name,customer_code,order_code,goods_code,sale_amt,profit,delivery_type_name
	from 
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt between '20230701' and '20230731'
		and channel_code in ('1','7','9')
		and business_type_code =1 
		and inventory_dc_code !='WB26'
		and customer_code='229356'
		and goods_code='1555304'

