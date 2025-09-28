-- 直送
drop table if exists csx_analyse_tmp.csx_analyse_tmp_zhisong_beijing_daily_00;
create table csx_analyse_tmp.csx_analyse_tmp_zhisong_beijing_daily_00
as
select 
	b.month_of_year,b.csx_week,a.sdt,a.performance_province_name,a.inventory_dc_code,
	if(a.classify_large_name in ('肉禽水产','蔬菜水果','干货加工'),a.classify_large_name,'食百') as classify_large_name_flag,
	sum(a.sale_amt) as sale_amt,
	sum(a.profit) as profit,
	sum(case when a.delivery_type_code=2 then a.sale_amt else 0 end) as zs_sale_amt,
	sum(case when a.delivery_type_code=2 then a.profit else 0 end) as zs_profit
from 
	(
	select
		performance_province_name,inventory_dc_code,customer_code,customer_name,classify_large_name,classify_middle_name,order_time,direct_delivery_type,
		sale_amt,profit,order_code,goods_code,sdt,delivery_type_code
	from
		csx_dws.csx_dws_sale_detail_di 
	where 
		sdt>='20230901' and sdt<=regexp_replace(to_date(date_sub(now(),1)),'-','')
		and channel_code in('1','7','9')
		and business_type_code in (1) 
		and performance_province_name in ('北京市')
		-- and delivery_type_code=2
		and inventory_dc_code not in ('WB26') -- 剔除直送仓
	) a 
	left join
		(
		select
			calday,quarter_of_year,csx_week,csx_week_begin,csx_week_end,month_of_year
		from
			csx_dim.csx_dim_basic_date
		) b on b.calday=a.sdt
group by 
	b.month_of_year,b.csx_week,a.sdt,a.performance_province_name,a.inventory_dc_code,
	if(a.classify_large_name in ('肉禽水产','蔬菜水果','干货加工'),a.classify_large_name,'食百') 
;
select * from csx_analyse_tmp.csx_analyse_tmp_zhisong_beijing_daily_00
;