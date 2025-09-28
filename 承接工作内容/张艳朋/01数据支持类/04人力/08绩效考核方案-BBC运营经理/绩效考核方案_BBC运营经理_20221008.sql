select
	province_name,city_name,if(is_new_customer=1,'新客户','老客户') as customer_flag,credit_pay_type_name,
	case when order_source=1 then 'bbc' when order_source=2 then '京东' when order_source=3 then '永辉生活订单' when order_source=4 then '联营' end as order_source_name,
	sum(sales_value) as sales_value,sum(profit) as profit
from
	csx_dw.ads_bbc_s_d_customer_summary
where
	sdt>='20220901' and sdt<='20220930'
group by 
	province_name,city_name,if(is_new_customer=1,'新客户','老客户'),credit_pay_type_name,
	case when order_source=1 then 'bbc' when order_source=2 then '京东' when order_source=3 then '永辉生活订单'  when order_source=4 then '联营' end
;

select
	province_name,city_name,
	count(distinct case when is_new_customer=1 then customer_no else null end) as new_customer_cnt,
	count(distinct case when is_new_customer !=1 then customer_no else null end) as old_customer_cnt
from
	csx_dw.ads_bbc_s_d_customer_summary
where
	sdt>='20220801' and sdt<='20220831'
group by 
	province_name,city_name

;

select
	province_name,city_name,
	count(distinct case when is_new_customer=1 then customer_no else null end) as new_customer_cnt,
	count(distinct case when is_new_customer !=1 then customer_no else null end) as old_customer_cnt
from
	csx_dw.ads_bbc_s_m_customer_summary
where
	month='202209'
group by 
	province_name,city_name	