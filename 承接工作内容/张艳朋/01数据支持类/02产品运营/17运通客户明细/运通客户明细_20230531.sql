
	select
		a.performance_region_name,a.performance_province_name,a.performance_city_name,a.customer_code,a.customer_name,
		b.sub_customer_code,b.sub_customer_name,
		a.first_category_name,a.second_category_name,a.third_category_name,
		a.sales_user_number,a.sales_user_name,create_time
	from
		(
		select
			performance_region_name,performance_province_name,performance_city_name,customer_code,customer_name,first_category_name,second_category_name,third_category_name,
			sales_user_number,sales_user_name,create_time
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current' 
			and customer_name like '%运通%'
		) a 
		left join
			(
			select
				customer_code,sub_customer_code,sub_customer_name
			from
				csx_dim.csx_dim_csms_yszx_customer_relation
			where
				sdt='current'
			) b on b.customer_code=a.customer_code
			
			
	select
		a.*,
		b.sub_customer_code,b.sub_customer_name
	from
		(
		select
			*
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current' 
			and customer_name like '%运通%'
		) a 
		left join
			(
			select
				customer_code,sub_customer_code,sub_customer_name
			from
				csx_dim.csx_dim_csms_yszx_customer_relation
			where
				sdt='current'
			) b on b.customer_code=a.customer_code