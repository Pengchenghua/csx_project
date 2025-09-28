
-- ===============================================================================================================================================================

	select
		a.performance_province_name,a.business_type_name,
		if(b.quarter_of_year=c.quarter_of_year,'新客','老客') as customer_flag,
		count(distinct a.customer_code) as customer_cnt,
		sum(sale_amt) as sale_amt
	from
		(
		select
			sdt,performance_province_name,business_type_name,customer_code,sale_amt
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20230101' and sdt<='20230331'
			and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
			and business_type_code in(1,2,4,6)
			and performance_province_name !='平台-B'
		) a 
		left join
			(
			select
				calday,quarter_of_year
			from
				csx_dim.csx_dim_basic_date
			) b on b.calday=a.sdt
		left join
			(
			select 
				a.customer_code,a.first_business_sale_date,b.quarter_of_year,a.business_type_name
			from
				(
				select
					customer_code,first_business_sale_date,business_type_name
				from
					csx_dws.csx_dws_crm_customer_business_active_di
				where 
					sdt='current'
					and business_type_code in(1,2,4,6) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
				) a 
				left join
					(
					select
						calday,quarter_of_year
					from
						csx_dim.csx_dim_basic_date
					) b on b.calday=a.first_business_sale_date		
			) c on c.customer_code=a.customer_code and c.business_type_name=a.business_type_name	
	group by 
		a.performance_province_name,a.business_type_name,
		if(b.quarter_of_year=c.quarter_of_year,'新客','老客')	
;

	select
		a.performance_province_name,
		if(b.quarter_of_year=c.quarter_of_year,'新客','老客') as customer_flag,
		count(distinct a.customer_code) as customer_cnt
	from
		(
		select
			sdt,performance_province_name,customer_code,sale_amt
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20230101' and sdt<='20230331'
			and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
			and business_type_code in(1,2,4,6)
			and performance_province_name !='平台-B'
		) a 
		left join
			(
			select
				calday,quarter_of_year
			from
				csx_dim.csx_dim_basic_date
			) b on b.calday=a.sdt
		left join
			(
			select 
				a.customer_code,a.first_business_sale_date,b.quarter_of_year
			from
				(
				select
					customer_code,min(first_business_sale_date) as first_business_sale_date
				from
					csx_dws.csx_dws_crm_customer_business_active_di
				where 
					sdt='current'
					and business_type_code in(1,2,4,6) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
				group by 
					customer_code
				) a 
				left join
					(
					select
						calday,quarter_of_year
					from
						csx_dim.csx_dim_basic_date
					) b on b.calday=a.first_business_sale_date		
			) c on c.customer_code=a.customer_code
	group by 
		a.performance_province_name,
		if(b.quarter_of_year=c.quarter_of_year,'新客','老客')
;
-- 验数
		select
			count(distinct customer_code),sum(sale_amt) as sale_amt
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20230101' and sdt<='20230331'
			and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
			and business_type_code in(1,2,4,6)
			and performance_province_name !='平台-B'		