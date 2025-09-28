-- 教育增长复盘：
-- 核心是要得出结论：1. 目前教育增长全国如何，哪些子行业哪些省区不错，哪些不行；2. 教育客户如何，从老客增量，留存，新开数，毛利率绝对值及爬坡情况如何；各省区哪些不错，哪些不行 之类的

-- 
-- 五大行业销售额趋势
-- 
-- 一页客户 
-- 一页销售额 
-- 一页商机
-- 大商机 大数+转化 只看教育行业
-- ================================================================================================================================================================================
-- 教育行业整体新客、老客
-- 哪个省区新客多、老客流失多

-- 5个季度商机数量、转化
-- Q1-大商机，转化
-- 只看教育行业
	
select 
	d.performance_region_name,d.performance_province_name,b.quarter_of_year,
	if(b.quarter_of_year=c.quarter_of_year,'新客','老客') as customer_flag,d.second_category_name,d.third_category_name,
	count(distinct a.customer_code) as customer_cnt,
	sum(a.sale_amt) as sale_amt,
	sum(a.profit) as profit
from
	(
	select
		sdt,customer_code,sale_amt,profit
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20220101' and sdt<='20230331'
		and channel_code in('1','7','9')
		and business_type_code in(1)
		-- and order_channel_code not in (4,6) -- 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
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
				customer_code,first_business_sale_date
			from
				csx_dws.csx_dws_crm_customer_business_active_di
			where 
				sdt='current'
				and business_type_code=1 -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
			) a 
			left join
				(
				select
					calday,quarter_of_year
				from
					csx_dim.csx_dim_basic_date
				) b on b.calday=a.first_business_sale_date		
		) c on c.customer_code=a.customer_code
	join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
			and second_category_code='305' -- 客户二级分类：教育
		) d on d.customer_code=a.customer_code	
group by 
	d.performance_region_name,d.performance_province_name,b.quarter_of_year,
	if(b.quarter_of_year=c.quarter_of_year,'新客','老客'),d.second_category_name,d.third_category_name
;

-- ===============================================================================================================================================================================
-- 20224下单在20231未下单	

select
	d.performance_region_name,d.performance_province_name,d.performance_city_name,
	count(distinct a.customer_code) as customer_cnt
from
	(
	select
		customer_code
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20221001' and sdt<='20221231'
		and channel_code in('1','7','9')
		and business_type_code in(1)
		and order_channel_code not in (4,6) -- 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
	group by 
		customer_code
	) a 
	left join
		(
		select
			customer_code
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20230101' and sdt<='20230331'
			and channel_code in('1','7','9')
			and business_type_code in(1)
			and order_channel_code not in (4,6) -- 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
		group by 
			customer_code
		) b on b.customer_code=a.customer_code
	join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
			and second_category_code='305' -- 客户二级分类：教育
		) d on d.customer_code=a.customer_code	
where
	b.customer_code is null
group by  
	d.performance_region_name,d.performance_province_name,d.performance_city_name
;

-- ===============================================================================================================================================================================
-- 20224下单在20231未下单明细

select
	d.performance_region_name,d.performance_province_name,d.performance_city_name,a.customer_code,d.customer_name,first_category_name,second_category_name,third_category_name,
	a.sale_amt,a.profit,a.profit_rate
from
	(
	select
		customer_code,sum(sale_amt) as sale_amt,sum(profit) as profit,if(sum(sale_amt)=0,0,sum(profit)/abs(sum(sale_amt))) as profit_rate
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20221001' and sdt<='20221231'
		and channel_code in('1','7','9')
		and business_type_code in(1)
		and order_channel_code not in (4,6) -- 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
	group by 
		customer_code
	) a 
	left join
		(
		select
			customer_code
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20230101' and sdt<='20230331'
			and channel_code in('1','7','9')
			and business_type_code in(1)
			and order_channel_code not in (4,6) -- 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
		group by 
			customer_code
		) b on b.customer_code=a.customer_code
	join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
			and second_category_code='305' -- 客户二级分类：教育
		) d on d.customer_code=a.customer_code	
where
	b.customer_code is null
;
-- ===============================================================================================================================================================================
-- 商机
select
	a.quarter_of_year,a.business_sign_month,a.business_number,a.customer_id,a.customer_code,a.customer_name,
	a.performance_region_name,a.performance_province_name,a.performance_city_name,
	a.business_stage,a.contract_cycle,a.estimate_contract_amount,a.gross_profit_rate,a.business_sign_date,a.business_sign_date_2,a.first_business_sign_date,
	a.num,a.next_sign_date,
	sum(case when b.sdt>=a.business_sign_date and b.sdt<a.next_sign_date then b.sale_amt else null end) as sale_amt,
	sum(case when b.sdt>=a.business_sign_date and b.sdt<a.next_sign_date then b.profit else null end) as profit,
	count(distinct case when b.sdt>=a.business_sign_date and b.sdt<a.next_sign_date then b.sdt else null end) as sdt_cnt,
	min(case when b.sdt>=a.business_sign_date and b.sdt<a.next_sign_date then b.sdt else null end) as min_sdt
from
	(
	select
		a.business_sign_month,a.business_number,a.customer_id,a.customer_code,a.customer_name,
		d.performance_region_name,d.performance_province_name,d.performance_city_name,
		a.business_stage,a.contract_cycle,a.estimate_contract_amount,a.gross_profit_rate,
		a.business_sign_date,a.business_sign_date_2,a.first_business_sign_date,
		row_number() over(partition by a.customer_code order by a.business_sign_time) num,
		regexp_replace(to_date(lead(a.business_sign_time,1,'9999-12-31')over(partition by a.customer_code order by a.business_sign_time)),'-','') as next_sign_date,
		b.quarter_of_year
	from 
		(
		select
			business_sign_time,
			regexp_replace(substr(to_date(business_sign_time),1,7),'-','') business_sign_month,business_number,customer_id,customer_code,customer_name,
			business_stage,contract_cycle,estimate_contract_amount,gross_profit_rate,
			regexp_replace(to_date(business_sign_time),'-','') business_sign_date,
			to_date(business_sign_time) as business_sign_date_2,
			regexp_replace(to_date(first_business_sign_time),'-','') first_business_sign_date
			-- row_number() over(partition by concat(customer_code) order by business_sign_time) num --商机顺序
		from 
			csx_dim.csx_dim_crm_business_info
		where 
			sdt='current'
			and channel_code in('1','7','9')
			and business_type_code in(1) -- 日配业务
			and status=1  -- 是否有效 0.无效 1.有效 (status=0,'停止跟进')
			and business_stage=5
			and regexp_replace(to_date(business_sign_time),'-','') between '20220101' and '20230331'
		)a
		left join
			(
			select
				calday,quarter_of_year
			from
				csx_dim.csx_dim_basic_date
			) b on b.calday=a.business_sign_date
		join
			(
			select
				customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
				sales_user_number,sales_user_name,customer_address_full
			from
				csx_dim.csx_dim_crm_customer_info
			where
				sdt='current'
				and second_category_code='305' -- 客户二级分类：教育
			) d on d.customer_code=a.customer_code	
	) a 
	left join 
		(
		select 
			sdt,customer_code,
			sum(sale_amt) as sale_amt,
			sum(profit) as profit,
			sum(profit)/abs(sum(sale_amt)) as profit_rate
		from 	
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20220101' and sdt<='20230331'
			and channel_code in('1','7','9')
			and business_type_code in(1)
			and order_channel_code not in (4,6) -- 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
		group by 
			sdt,customer_code
		)b on a.customer_code=b.customer_code
group by 
	a.quarter_of_year,a.business_sign_month,a.business_number,a.customer_id,a.customer_code,a.customer_name,
	a.performance_region_name,a.performance_province_name,a.performance_city_name,
	a.business_stage,a.contract_cycle,a.estimate_contract_amount,a.gross_profit_rate,a.business_sign_date,a.business_sign_date_2,a.first_business_sign_date,
	a.num,a.next_sign_date
;

