
	select
		a.performance_province_name,
		a.business_type_name,
		a.customer_code,
		a.customer_name,
		a.sub_customer_code,
		a.sub_customer_name,
		b.sales_user_number,
		b.sales_user_name,
		coalesce(c.rp_service_user_work_no_new,c.fl_service_user_work_no_new,bbc_service_user_work_no_new) as service_user_work_no_new,
		coalesce(c.rp_service_user_name_new,c.fl_service_user_name_new,c.bbc_service_user_name_new) as service_user_name_new,
		b.customer_address_full,
		a.sale_amt,
		a.profit
	from
		(
		select
			performance_province_name,
			business_type_name,
			customer_code,
			customer_name,
			sub_customer_code,
			sub_customer_name,
			sum(sale_amt) as sale_amt,
			sum(profit) as profit
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20221001' and sdt<='20221212'
			and channel_code in('1','7','9')
			and business_type_code=1
			and performance_province_name='北京市'
		group by 
			performance_province_name,
			business_type_name,
			customer_code,
			customer_name,
			sub_customer_code,
			sub_customer_name
		)a 
		left join
			(
			select
				customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
				sales_user_number,sales_user_name,customer_address_full
			from
				csx_dim.csx_dim_crm_customer_info
			where
				sdt='current'
			) b on b.customer_code=a.customer_code
		left join
			(
			select distinct 
				customer_no,customer_name,
				rp_service_user_work_no_new,rp_service_user_name_new,
				fl_service_user_work_no_new,fl_service_user_name_new,
				bbc_service_user_work_no_new,bbc_service_user_name_new
			from
				csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
			where
				sdt='20221212'
			) c on c.customer_no=a.customer_code 