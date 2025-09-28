-- 1、北京省区2021年及2022年各业务类型的销售额、毛利率、客户数
select
	substr(sdt,1,4) as syear,
	performance_province_name,
	business_type_name,
	sum(sale_amt) as sale_amt,
	sum(profit)/abs(sum(sale_amt)) as profit_rate,
	count(distinct customer_code) as customer_cnt
from
	csx_dws.csx_dws_sale_detail_di
where 
	sdt>='20210101' and sdt<='20221231'
	and channel_code in('1','7','9')
	and performance_province_name='北京市'
group by 
	substr(sdt,1,4),
	performance_province_name,
	business_type_name
;

-- 2、行业（二级分类）的毛利额、销售额、客户数——分业务类型
select
	a.syear,
	a.performance_province_name,
	a.business_type_name,
	b.first_category_name,
	b.second_category_name,
	sum(a.sale_amt) as sale_amt,
	sum(a.profit) as profit,
	count(distinct a.customer_code) as customer_cnt
from
	(
	select
		substr(sdt,1,4) as syear,
		performance_province_name,
		business_type_name,
		customer_code,
		sum(sale_amt) as sale_amt,
		sum(profit) as profit
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20210101' and sdt<='20221231'
		and channel_code in('1','7','9')
		and performance_province_name='北京市'
	group by 
		substr(sdt,1,4),
		performance_province_name,
		business_type_name,
		customer_code
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
group by 
	a.syear,
	a.performance_province_name,
	a.business_type_name,
	b.first_category_name,
	b.second_category_name
;

-- 3、新签行业（二级分类）客户数、新签客户的销售额金额
select
	a.syear,
	a.performance_province_name,
	a.first_category_name,
	a.second_category_name,
	sum(b.sale_amt_21) as sale_amt_21,
	sum(b.sale_amt_22) as sale_amt_22,
	count(distinct a.customer_code) as customer_cnt
from
	(
	select
		customer_code,sign_time,performance_province_name,first_category_name,second_category_name,year(sign_time) as syear
	from
		csx_dim.csx_dim_crm_customer_info
	where
		sdt='current'
		and performance_province_name='北京市'
		and to_date(sign_time) between '2021-01-01' and '2022-12-31'
	) a 
	left join
		(
		select 
			customer_code,
			sum(sale_amt)as sale_amt,
			sum(profit) as profit,
			sum(profit)/abs(sum(sale_amt)) as profit_rate,
			sum(case when substr(sdt,1,4)='2021' then sale_amt else 0 end) as sale_amt_21,
			sum(case when substr(sdt,1,4)='2022' then sale_amt else 0 end) as sale_amt_22
		from 
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20210101' and sdt<='20221231'
			and channel_code in('1','7','9')
			and performance_province_name='北京市'
		group by 
			customer_code
		) b on b.customer_code=a.customer_code
group by 
	a.syear,
	a.performance_province_name,
	a.first_category_name,
	a.second_category_name	
		
-- 4、各业务类型彩食鲜主体履约客户数、客户金额
select
	a.syear,
	a.performance_province_name,
	a.business_type_name,
	b.company_belong_name,
	sum(a.sale_amt) as sale_amt,
	count(distinct a.customer_code) as customer_cnt
from
	(
	select
		substr(sdt,1,4) as syear,
		performance_province_name,
		business_type_name,
		customer_code,
		sum(sale_amt) as sale_amt,
		sum(profit) as profit
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20210101' and sdt<='20221231'
		and channel_code in('1','7','9')
		and performance_province_name='北京市'
	group by 
		substr(sdt,1,4),
		performance_province_name,
		business_type_name,
		customer_code
	)a 
	join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,company_code,company_name,company_belong,
			case when company_belong=1 then '彩食鲜' when company_belong=2 then '永辉' end as company_belong_name
		from
			csx_dim.csx_dim_crm_customer_company
		where
			sdt='current'
			and company_belong=1 -- 公司归属(1 彩食鲜 2 永辉)
		) b on b.customer_code=a.customer_code
group by 
	a.syear,
	a.performance_province_name,
	a.business_type_name,
	b.company_belong_name
-- 5、各业务类型永辉主体履约客户数、客户金额
select
	a.syear,
	a.performance_province_name,
	a.business_type_name,
	b.company_belong_name,
	sum(a.sale_amt) as sale_amt,
	count(distinct a.customer_code) as customer_cnt
from
	(
	select
		substr(sdt,1,4) as syear,
		performance_province_name,
		business_type_name,
		customer_code,
		sum(sale_amt) as sale_amt,
		sum(profit) as profit
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20210101' and sdt<='20221231'
		and channel_code in('1','7','9')
		and performance_province_name='北京市'
	group by 
		substr(sdt,1,4),
		performance_province_name,
		business_type_name,
		customer_code
	)a 
	join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,company_code,company_name,company_belong,
			case when company_belong=1 then '彩食鲜' when company_belong=2 then '永辉' end as company_belong_name
		from
			csx_dim.csx_dim_crm_customer_company
		where
			sdt='current'
			and company_belong=2 -- 公司归属(1 彩食鲜 2 永辉)
		) b on b.customer_code=a.customer_code
group by 
	a.syear,
	a.performance_province_name,
	a.business_type_name,
	b.company_belong_name