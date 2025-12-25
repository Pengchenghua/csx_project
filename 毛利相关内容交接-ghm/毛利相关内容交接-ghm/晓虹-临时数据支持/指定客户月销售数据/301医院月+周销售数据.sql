select
	substr(a.sdt,1,6) as smonth,
	case when a.sdt>='20251101' then c.week_range else '-' end  week_smonth,
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	a.customer_code,
	a.customer_name,
	sum(sale_amt)/10000 as sale_amt,
	sum(profit)/10000 as profit,
	sum(profit)/abs(sum(sale_amt)) profit_rate
from 
	(select *
	from 
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt >= '20250801' and sdt <= '20251127'
		and business_type_code=1  
		and shipper_code='YHCSX' 
		and customer_code 
		in ('252181'
			,'151497'
			,'252189'
			,'252182'
			,'252183'
			,'254511'
			,'254080'
			,'252191'
			,'258912'
			,'257481'
			,'252186'
			,'252193'
			,'252195'
			,'252185'
			,'252999'
			,'256641')
	) a	
	left join -- 周信息
	(
	select
		calday,csx_week,csx_week_begin,csx_week_end,concat(csx_week,'(',csx_week_begin,'-',csx_week_end,')') as week_range
	from
		csx_dim.csx_dim_basic_date
	) c on c.calday=a.sdt
	left join 
    (select
        code as type,
        max(name) as name,
        max(extra) as extra 
    from csx_dim.csx_dim_basic_topic_dict_df
    where parent_code = 'direct_delivery_type' 
    group by code 
    ) h 
    on a.direct_delivery_type=h.type 
    where h.extra='采购参与'	
group by 
	substr(a.sdt,1,6),
	case when a.sdt>='20251101' then c.week_range else '-' end ,
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	a.customer_code,
	a.customer_name;