	
select
	a.customer_code,
	b.customer_name,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	b.performance_region_name,
	b.performance_province_name,
	b.performance_city_name,
	a.delivery_type_name,
	a.order_channel_name,
	c.quarter_of_year,
	a.channel_name,
	a.business_type_name,
	sum(a.sale_amt) as sale_amt,
	sum(a.sale_amt_no_tax) as sale_amt_no_tax,
	sum(a.profit) as profit,
	sum(a.profit_no_tax) as profit_no_tax
from
	(
	select
		sdt,customer_code,delivery_type_name,order_channel_code,
		case order_channel_code
			when 1 then 'B端'
			when 2 then 'M端'
			when 3 then 'BBC'
			when 4 then '调价返利'
			when -1 then 'SAP' 
		end as order_channel_name,
		channel_name,business_type_name,	
		sale_amt,sale_amt_no_tax,profit,profit_no_tax
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20210101' and sdt<='20221031'
		and channel_code in('1','7','9')
	) a 
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) b on b.customer_code=a.customer_code
	left join
		(
		select
			calday,quarter_of_year
		from
			csx_dim.csx_dim_basic_date
		) c on c.calday=a.sdt
group by 
	a.customer_code,
	b.customer_name,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	b.performance_region_name,
	b.performance_province_name,
	b.performance_city_name,
	a.delivery_type_name,
	a.order_channel_name,
	c.quarter_of_year,
	a.channel_name,
	a.business_type_name	
