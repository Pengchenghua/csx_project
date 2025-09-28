
select
	b.quarter_of_year as `季度`,
	a.performance_region_name as `大区`,
	a.performance_province_name as `省份`,
	sum(sale_amt)/10000 as `销售额_B+BBC`,
	sum(profit)/10000 as `毛利额_B+BBC`,
	
	sum(case when business_type_code in(2,6) then sale_amt else 0 end)/10000 as `销售额_福利BBC`,
	sum(case when business_type_code in(2,6) then profit else 0 end)/10000 as `毛利额_福利BBC`,

	sum(case when business_type_code not in(4) then sale_amt else 0 end)/10000 as `销售额_自营`,
	sum(case when business_type_code not in(4) then profit else 0 end)/10000 as `毛利额_自营`
from
	(
	select 
		sdt,performance_region_name,performance_province_name,business_type_code,sale_amt,profit	
	from 
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt between '20220101' and '20230630'
		and channel_code in('1','7','9')
	) a 
	left join
		(
		select
			calday,quarter_of_year,csx_week,csx_week_begin,csx_week_end
		from
			csx_dim.csx_dim_basic_date
		) b on b.calday=a.sdt
group by 
	b.quarter_of_year,
	a.performance_region_name,
	a.performance_province_name	