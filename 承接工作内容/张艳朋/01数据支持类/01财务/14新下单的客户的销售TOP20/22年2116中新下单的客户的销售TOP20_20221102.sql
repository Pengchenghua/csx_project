	
select
	b.performance_province_name,
	b.performance_city_name,
	a.customer_code,
	b.customer_name,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	a.sale_amt,
	a.profit
from
	(
	select
		customer_code,sum(sale_amt) as sale_amt,sum(profit) as profit,row_number()over(order by sum(sale_amt) desc) as rn
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20220101' and sdt<='20221101'
		and channel_code in('1','7','9')
		and sign_company_code='2116'
	group by 
		customer_code
	) a 
	left join
		(
		select
			customer_code,customer_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) b on b.customer_code=a.customer_code
where
	rn<=20
