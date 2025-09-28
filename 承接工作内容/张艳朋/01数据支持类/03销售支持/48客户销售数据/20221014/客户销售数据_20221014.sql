	select
		b.province_name,
		a.customer_no,
		b.customer_name,
		a.business_type_name,
		a.sales_value,
		b.contact_person,
		b.contact_phone,
		b.customer_address_full,
		b.work_no,
		b.sales_name
	from
		(
		select 
			customer_no,
			business_type_name,
			sum(sales_value) as sales_value
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20190101' and sdt<='20211231'
			and channel_code in('1','7','9')
		group by 
			customer_no,business_type_name
		) a
		join
			(
			select
				customer_no,customer_name,province_name,contact_person,contact_phone,customer_address_full,work_no,sales_name
			from
				csx_dw.dws_crm_w_a_customer 
			where
				sdt='current'
				and province_name='北京市'
			) b on b.customer_no=a.customer_no
		left join
			(
			select 
				distinct customer_no
			from 
				csx_dw.dws_sale_r_d_detail
			where 
				sdt>='20220101' and sdt<='20221013'
				and channel_code in('1','7','9')
			) c on c.customer_no=a.customer_no
	where
		c.customer_no is null