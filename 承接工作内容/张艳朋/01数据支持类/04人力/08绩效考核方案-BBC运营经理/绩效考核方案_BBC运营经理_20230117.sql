select
	performance_province_name,performance_city_name,if(is_new_customer=1,'新客户','老客户') as customer_flag,credit_pay_type_name,
	case when order_source=0 then '自营' when order_source=1 then 'bbc' when order_source=2 then '京东' when order_source=3 then '永辉生活订单' when order_source=4 then '联营' end as order_source_name,
	sum(sale_amt) as sale_amt,sum(profit) as profit
from
	-- csx_dw.ads_bbc_s_d_customer_summary
	csx_ads.csx_ads_bbc_customer_summary_1d
where
	sdt>='20221001' and sdt<='20221231'
group by 
	1,2,3,4,5
;

select
	performance_province_name,performance_city_name,
	count(distinct case when is_new_customer=1 then customer_code else null end) as new_customer_cnt,
	count(distinct case when is_new_customer !=1 then customer_code else null end) as old_customer_cnt
from
	-- csx_dw.ads_bbc_s_m_customer_summary
	csx_ads.csx_ads_bbc_customer_summary_1m
where
	month>='202210' and month<='202212'
group by 
	performance_province_name,performance_city_name	