-- 区分业务类型
select
	b.performance_province_name,
	substr(a.sdt,1,4) as syear,
	a.business_type_name,
	count(distinct a.customer_code) as customer_cnt
from
	(
	select
		sdt,customer_code,channel_name,business_type_name,sale_amt,sale_amt_no_tax,profit,profit_no_tax
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20210101' and sdt<='20221231'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
	) a 
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
group by 
	b.performance_province_name,
	substr(a.sdt,1,4),
	a.business_type_name
;
-- 不区分业务类型
select
	b.performance_province_name,
	substr(a.sdt,1,4) as syear,
	count(distinct a.customer_code) as customer_cnt
from
	(
	select
		sdt,customer_code,channel_name,business_type_name,sale_amt,sale_amt_no_tax,profit,profit_no_tax
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20210101' and sdt<='20221231'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
	) a 
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
group by 
	b.performance_province_name,
	substr(a.sdt,1,4)
;
-- 总+合计
select
	b.performance_province_name,
	count(distinct a.customer_code) as customer_cnt
from
	(
	select
		sdt,customer_code,channel_name,business_type_name,sale_amt,sale_amt_no_tax,profit,profit_no_tax
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20210101' and sdt<='20221231'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
	) a 
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
group by 
	b.performance_province_name
;
-- 区分业务类型+总
select
	b.performance_province_name,
	a.business_type_name,
	count(distinct a.customer_code) as customer_cnt
from
	(
	select
		sdt,customer_code,channel_name,business_type_name,sale_amt,sale_amt_no_tax,profit,profit_no_tax
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20210101' and sdt<='20221231'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
	) a 
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
group by 
	b.performance_province_name,
	a.business_type_name
;
-- 22年Q对比
select
	b.performance_province_name,
	c.quarter_of_year,
	a.business_type_name,
	count(distinct a.customer_code) as customer_cnt
from
	(
	select
		sdt,customer_code,channel_name,business_type_name,sale_amt,sale_amt_no_tax,profit,profit_no_tax
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20220101' and sdt<='20221231'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
	) a 
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
		select
			calday,quarter_of_year
		from
			csx_dim.csx_dim_basic_date
		) c on c.calday=a.sdt
group by 
	b.performance_province_name,
	c.quarter_of_year,
	a.business_type_name
;
-- 日配
select
	b.performance_province_name,
	c.quarter_of_year,
	if(d.first_sale_quarter_of_year=c.quarter_of_year,'新客','老客') as customer_flag_q,
	count(distinct a.customer_code) as customer_cnt,
	sum(a.sale_amt) as sale_amt
from
	(
	select
		sdt,customer_code,channel_name,business_type_name,sale_amt,sale_amt_no_tax,profit,profit_no_tax
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20220101' and sdt<='20221231'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		and business_type_code=1
	) a 
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
		select
			calday,quarter_of_year
		from
			csx_dim.csx_dim_basic_date
		) c on c.calday=a.sdt
	left join
		(
		select
			t1.customer_code,t1.first_business_sale_date,t1.first_sale_yearmonth,t2.quarter_of_year as first_sale_quarter_of_year
		from
			(
			select
				customer_code,first_business_sale_date,substr(first_business_sale_date,1,6) as first_sale_yearmonth
			from
				csx_dws.csx_dws_crm_customer_business_active_di
			where
				sdt='current'
				and business_type_code=1
			) t1
			left join
				(
				select
					calday,quarter_of_year
				from
					csx_dim.csx_dim_basic_date
				) t2 on t2.calday=t1.first_business_sale_date				
		) d on d.customer_code=a.customer_code
group by 
	b.performance_province_name,
	c.quarter_of_year,
	if(d.first_sale_quarter_of_year=c.quarter_of_year,'新客','老客')
;
-- 日配
select
	b.performance_province_name,
	c.quarter_of_year,
	count(distinct a.customer_code) as customer_cnt,
	sum(a.sale_amt) as sale_amt
from
	(
	select
		sdt,customer_code,channel_name,business_type_name,sale_amt,sale_amt_no_tax,profit,profit_no_tax
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20220101' and sdt<='20221231'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		and business_type_code !=4
	) a 
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
		select
			calday,quarter_of_year
		from
			csx_dim.csx_dim_basic_date
		) c on c.calday=a.sdt
	left join
		(
		select
			t1.customer_code,t1.first_business_sale_date,t1.first_sale_yearmonth,t2.quarter_of_year as first_sale_quarter_of_year
		from
			(
			select
				customer_code,first_business_sale_date,substr(first_business_sale_date,1,6) as first_sale_yearmonth
			from
				csx_dws.csx_dws_crm_customer_business_active_di
			where
				sdt='current'
				and business_type_code=1
			) t1
			left join
				(
				select
					calday,quarter_of_year
				from
					csx_dim.csx_dim_basic_date
				) t2 on t2.calday=t1.first_business_sale_date				
		) d on d.customer_code=a.customer_code
group by 
	b.performance_province_name,
	c.quarter_of_year
;


select * from csx_analyse_tmp.csx_analyse_tmp_finance_customer_sale_20230130

-- 验数
	select
		sum(sale_amt)
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20210101' and sdt<='20221231'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
-- SKU数		
	select
		substr(sdt,1,4) as syear,
		customer_code,
		count(distinct goods_code)
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20210101' and sdt<='20221231'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		and business_type_code=1
		and customer_code in ('114244','118602','121061','114548','119032','118022','113809','113758','104493','106287','106775','123086','122817','113870','104872','123311','120459','105164','106898','118376','124524','126377','123622')
	group by 
		substr(sdt,1,4),
		customer_code	
;
-- SKU数		
	select
		substr(sdt,1,4) as syear,
		customer_code,
		count(distinct goods_code)
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20210101' and sdt<='20221231'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		and business_type_code=1
		and customer_code in ('115906')
	group by 
		substr(sdt,1,4),
		customer_code		
