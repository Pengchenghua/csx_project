	select
		b.province_name,
		a.customer_no,
		b.customer_name,
		a.customer_name,
		a.sales_value,
		a.excluding_tax_sales,
		a.excluding_tax_profit
	from
		(
		select 
			customer_no,
			count(distinct substr(sdt,1,6)) as customer_name,
			sum(sales_value) as sales_value,
			sum(excluding_tax_sales) as excluding_tax_sales,
			sum(excluding_tax_profit) as excluding_tax_profit
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20190101' and sdt<='20220831'
			and channel_code in('1','7','9')
			and customer_no in ('128371','120054','105156','127082','128587','125137','105181','106423','118007','105182','117108','114387','105164','105575')
		group by 
			customer_no
		) a
		left join
			(
			select
				customer_no,customer_name,province_name
			from
				csx_dw.dws_crm_w_a_customer 
			where
				sdt='current'
			) b on b.customer_no=a.customer_no