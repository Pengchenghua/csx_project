-- =======================================================================================================================================================================		
--临时-明细
drop table if exists csx_analyse_tmp.csx_analyse_tmp_zhisong_beijing_04;
create table csx_analyse_tmp.csx_analyse_tmp_zhisong_beijing_04
as
select
	*
from
	(
	select 
		substr(a.sdt,1,6) as smonth,a.performance_province_name,a.inventory_dc_code,a.classify_middle_name,
		a.direct_delivery_type_name,
		sum(a.sale_amt) as sale_amt,
		sum(a.profit) as profit,
		sum(sale_amt_no_tax) as sale_amt_no_tax,
		sum(profit_no_tax) as profit_no_tax,
		if(order_hour_flag='22点至3点' and a.direct_delivery_type_name='普通','临时加单',direct_delivery_type_name) as direct_delivery_type_name_flag,
		if(a.classify_large_name in ('肉禽水产','蔬菜水果','干货加工'),a.classify_large_name,'食百') as classify_large_name_flag,
		if(d.shop_code is null,'否','是') types,business_type_name,a.customer_code,a.customer_name
	from 
		(
		select
			performance_province_name,inventory_dc_code,customer_code,customer_name,classify_large_name,classify_middle_name,order_time,direct_delivery_type,
			sale_amt,profit,order_code,goods_code,sdt,
			case when direct_delivery_type=0 or direct_delivery_type is null then '普通'
				when direct_delivery_type=1 then 'RD'
				when direct_delivery_type=2 then 'ZZ'
				when direct_delivery_type=11 then '临时加单'
				when direct_delivery_type=12 then '紧急补货' 
			end as direct_delivery_type_name,
			date_format(order_time,'HH') as order_hour,
			if(date_format(order_time,'HH')>='22' or date_format(order_time,'HH')<='02','22点至3点',concat(date_format(order_time,'HH'),'点')) as order_hour_flag,
			sale_amt_no_tax,profit_no_tax,business_type_name
		from
			csx_dws.csx_dws_sale_detail_di 
		where 
			sdt>='20230401' and sdt<='20230930'
			and channel_code in('1','7','9')
			and business_type_code in (1,2) 
			and delivery_type_code=2
		) a 
		left join
			(
			select
				calday,quarter_of_year,csx_week,csx_week_begin,csx_week_end,month_of_year
			from
				csx_dim.csx_dim_basic_date
			) b on b.calday=a.sdt
		left join   --商品表
			(
			select 
				goods_code,goods_name,unit_name,classify_large_name,classify_middle_name,classify_small_name,brand_name,standard,category_small_name,spu_goods_name
			from 
				csx_dim.csx_dim_basic_goods
			where 
				sdt ='current'
			) c on a.goods_code=c.goods_code
		left join 
			(
			select 
				* 
			from 
				csx_dim.csx_dim_shop 
			where 
				sdt='current' and shop_low_profit_flag=1 
			) d on a.inventory_dc_code=d.shop_code 
	group by 
		substr(a.sdt,1,6),a.performance_province_name,a.inventory_dc_code,a.classify_middle_name,
		a.direct_delivery_type_name,
		if(order_hour_flag='22点至3点' and a.direct_delivery_type_name='普通','临时加单',direct_delivery_type_name),
		if(a.classify_large_name in ('肉禽水产','蔬菜水果','干货加工'),a.classify_large_name,'食百'),
		if(d.shop_code is null,'否','是'),business_type_name,a.customer_code,a.customer_name
	) a 
;
select * from csx_analyse_tmp.csx_analyse_tmp_zhisong_beijing_04
;