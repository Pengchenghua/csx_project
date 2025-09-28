-- 月份 大区 省区 城市 客户 二级行业 日配首次履约日期 新老客  是否调价 是否返利 物流模式 商品编码 商品名称 管理中类 管理小类 仓 是否直送仓 成本 售价 销售额 毛利额 毛利率 销售数量 

drop table if exists csx_analyse_tmp.csx_analyse_tmp_profit_city;
create table csx_analyse_tmp.csx_analyse_tmp_profit_city
as 

	select 
		substr(a.sdt,1,6) smonth,a.performance_region_name,a.performance_province_name,a.performance_city_name,
	 	a.customer_code,d.customer_name,d.second_category_name,e.first_sales_date,
		if(substr(a.sdt,1,6)=substr(e.first_sales_date,1,6),'是','否') as xinlaok,
		if(a.order_channel_code=6 ,'是','否') tiaojia_type,
		if(a.order_channel_code=4 ,'是','否') fanli_type,
		a.delivery_type_name,a.goods_code,b.goods_name,b.classify_middle_name,b.classify_small_name,
		-- a.inventory_dc_code,
		if(c.shop_code is null,'否','是') types,
		sum(sale_amt)as sales_value,
		sum(profit)as profit,		
		sum(if(order_channel_detail_code=26,0,sale_qty)) as sale_qty,
		sum(sale_cost) as sale_cost
	from 
		(
		select 
			* 
	    from 
			csx_dws.csx_dws_sale_detail_di 
	    where 
			(sdt between '20230501' and '20230513' or sdt between '20230601' and '20230613')
			and channel_code in('1','7','9')
			and business_type_code in ('1') 
		) a
		join 
			( 
	        select 
				distinct shop_code 
			from 
				csx_dim.csx_dim_shop 
			where 
				sdt='current' 
				and shop_low_profit_flag !=1  
			)c on a.inventory_dc_code = c.shop_code
		left join 
			(
			select 
				*  
			from 
				csx_dim.csx_dim_basic_goods 
			where 
				sdt = 'current'
			) b on b.goods_code = a.goods_code 
		left join  -- 首单日期
			(
			select 
				customer_code,business_type_code,min(first_business_sale_date) first_sales_date
			from 
				csx_dws.csx_dws_crm_customer_business_active_di
			where 
				sdt ='current' and 	business_type_code in (1)
			group by 
				customer_code,business_type_code
			)e on e.customer_code=a.customer_code and e.business_type_code=a.business_type_code
		left join  
			(
			select
				customer_code,customer_name,second_category_name,third_category_name
			from  
				csx_dim.csx_dim_crm_customer_info 
			where 
				sdt='current'
			)d on d.customer_code=a.customer_code
    group by
		substr(a.sdt,1,6),a.performance_region_name,a.performance_province_name,a.performance_city_name,
	 	a.customer_code,d.customer_name,d.second_category_name,e.first_sales_date,
		if(substr(a.sdt,1,6)=substr(e.first_sales_date,1,6),'是','否'),
		if(a.order_channel_code=6 ,'是','否'),
		if(a.order_channel_code=4 ,'是','否'),
		a.delivery_type_name,a.goods_code,b.goods_name,b.classify_middle_name,b.classify_small_name,
		-- a.inventory_dc_code,
		if(c.shop_code is null,'否','是')
;
select * from csx_analyse_tmp.csx_analyse_tmp_profit_city where smonth='202306'


-- =============================================================================================================================================================================

drop table if exists csx_analyse_tmp.csx_analyse_tmp_profit_city2;
create table csx_analyse_tmp.csx_analyse_tmp_profit_city2
as 
select
	a.smonth,a.performance_region_name,a.performance_province_name,a.performance_city_name,
	a.customer_code,a.customer_name,a.second_category_name,a.first_sales_date,
	a.xinlaok,a.tiaojia_type,a.fanli_type,a.delivery_type_name,a.goods_code,a.goods_name,a.classify_middle_name,a.classify_small_name,
	a.types,b.price_type_new1,
	sum(sale_amt)as sale_amt,
	sum(profit)as profit,		
	sum(sale_qty) as sale_qty,
	sum(sale_cost) as sale_cost
from
	(
	select 
		substr(a.sdt,1,6) smonth,a.performance_region_name,a.performance_province_name,a.performance_city_name,
	 	a.customer_code,d.customer_name,d.second_category_name,e.first_sales_date,
		if(substr(a.sdt,1,6)=substr(e.first_sales_date,1,6),'是','否') as xinlaok,
		if(a.order_channel_code=6 ,'是','否') tiaojia_type,
		if(a.order_channel_code=4 ,'是','否') fanli_type,
		a.delivery_type_name,a.order_code,a.goods_code,b.goods_name,b.classify_middle_name,b.classify_small_name,
		a.inventory_dc_code,
		if(c.shop_code is null,'是','否') types,
		sum(sale_amt)as sale_amt,
		sum(profit)as profit,		
		sum(if(order_channel_detail_code=26,0,sale_qty)) as sale_qty,
		sum(sale_cost) as sale_cost
	from 
		(
		select 
			* 
	    from 
			csx_dws.csx_dws_sale_detail_di 
	    where 
			sdt between '20230601' and '20230613'
			and channel_code in('1','7','9')
			and business_type_code in (1) 
		) a
		join 
			( 
	        select 
				distinct shop_code 
			from 
				csx_dim.csx_dim_shop 
			where 
				sdt='current' 
				and shop_low_profit_flag !=1  
			)c on a.inventory_dc_code = c.shop_code
		join 
			(
			select 
				*  
			from 
				csx_dim.csx_dim_basic_goods 
			where 
				sdt = 'current'
				and classify_middle_name in ('蔬菜','猪肉','水产')
			) b on b.goods_code = a.goods_code 
		left join  -- 首单日期
			(
			select 
				customer_code,business_type_code,min(first_business_sale_date) first_sales_date
			from 
				csx_dws.csx_dws_crm_customer_business_active_di
			where 
				sdt ='current' and 	business_type_code in (1)
			group by 
				customer_code,business_type_code
			)e on e.customer_code=a.customer_code and e.business_type_code=a.business_type_code
		left join  
			(
			select
				customer_code,customer_name,second_category_name,third_category_name
			from  
				csx_dim.csx_dim_crm_customer_info 
			where 
				sdt='current'
			)d on d.customer_code=a.customer_code
    group by
		substr(a.sdt,1,6),a.performance_region_name,a.performance_province_name,a.performance_city_name,
	 	a.customer_code,d.customer_name,d.second_category_name,e.first_sales_date,
		if(substr(a.sdt,1,6)=substr(e.first_sales_date,1,6),'是','否'),
		if(a.order_channel_code=6 ,'是','否'),
		if(a.order_channel_code=4 ,'是','否'),
		a.delivery_type_name,a.order_code,a.goods_code,b.goods_name,b.classify_middle_name,b.classify_small_name,
		a.inventory_dc_code,
		if(c.shop_code is null,'是','否')
	) a 
	left join
		(
		select
			customer_code,inventory_dc_code,order_code,goods_code,min(price_type_new1) as price_type_new1
		from
			csx_analyse_tmp.csx_analyse_tmp_cus_price_guide_order_final_supple_last
		where
			price_type_new1 is not null
		group by 
			customer_code,inventory_dc_code,order_code,goods_code
		) b on b.customer_code=a.customer_code and b.inventory_dc_code=a.inventory_dc_code and b.order_code=a.order_code and b.goods_code=a.goods_code
group by 
	a.smonth,a.performance_region_name,a.performance_province_name,a.performance_city_name,
	a.customer_code,a.customer_name,a.second_category_name,a.first_sales_date,
	a.xinlaok,a.tiaojia_type,a.fanli_type,a.delivery_type_name,a.goods_code,a.goods_name,a.classify_middle_name,a.classify_small_name,
	a.types,b.price_type_new1
;
select * from csx_analyse_tmp.csx_analyse_tmp_profit_city2