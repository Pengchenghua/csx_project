	select
		substr(sdt,1,6) as smonth,
		performance_province_name,
		performance_city_name,
		count(distinct case when business_type_code=1 then order_code else null end) as rp_order_cnt,
		count(distinct case when business_type_code=1 then goods_code else null end) as rp_goods_code_cnt,
		count(distinct case when business_type_code=2 then order_code else null end) as fl_order_cnt,
		count(distinct case when business_type_code=2 then goods_code else null end) as fl_goods_code_cnt,
		count(distinct case when business_type_code=5 then order_code else null end) as dz_order_cnt,
		count(distinct case when business_type_code=5 then goods_code else null end) as dz_goods_code_cnt
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20220901' and sdt<='20221031'
		and channel_code in('1','7','9')
	group by 
		substr(sdt,1,6),
		performance_province_name,
		performance_city_name	

;


	select
		substr(sdt,1,6) as smonth,
		performance_province_name,
		performance_city_name,
		count(distinct case when business_type_code=1 then order_code else null end) as rp_order_cnt,
		count(case when business_type_code=1 then goods_code else null end) as rp_goods_code_cnt,
		count(distinct case when business_type_code=2 then order_code else null end) as fl_order_cnt,
		count(case when business_type_code=2 then goods_code else null end) as fl_goods_code_cnt,
		count(distinct case when business_type_code=5 then order_code else null end) as dz_order_cnt,
		count(case when business_type_code=5 then goods_code else null end) as dz_goods_code_cnt,
		sum(case when business_type_code=1 then sale_amt else 0 end) as rp_sale_amt,
		sum(case when business_type_code=2 then sale_amt else 0 end) as fl_sale_amt
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20220901' and sdt<='20221031'
		and channel_code in('1','7','9')
	group by 
		substr(sdt,1,6),
		performance_province_name,
		performance_city_name				