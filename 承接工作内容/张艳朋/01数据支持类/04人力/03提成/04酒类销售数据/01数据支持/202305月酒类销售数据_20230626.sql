-- 5月			
select
	concat('20230501','-','20230531') as qj,
	a.performance_province_name,b.sales_user_number,b.sales_user_name,
	c.classify_middle_name,c.classify_small_name,a.goods_code,c.goods_name,
	sum(a.sale_amt) as sale_amt,
	sum(a.profit) as profit
from 
	(
	select
		performance_province_name,goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name,sdt,customer_code,sale_amt,profit
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20230501' and sdt<='20230531'
		and channel_code in('1','7','9')
		-- and goods_code not in ('8718','8708','8649','840509')
		and classify_middle_name='酒'
	) a 
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='20230531'
		) b on b.customer_code=a.customer_code
	left join
		(
		select
			goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name
		from
			csx_dim.csx_dim_basic_goods
		where
			sdt='current'
		) c on c.goods_code=a.goods_code
group by 
	a.performance_province_name,b.sales_user_number,b.sales_user_name,
	c.classify_middle_name,c.classify_small_name,a.goods_code,c.goods_name
	
	
-- 4月			
select
	concat('20230401','-','20230430') as qj,
	a.performance_province_name,b.sales_user_number,b.sales_user_name,
	c.classify_middle_name,c.classify_small_name,a.goods_code,c.goods_name,
	sum(a.sale_amt) as sale_amt,
	sum(a.profit) as profit
from 
	(
	select
		performance_province_name,goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name,sdt,customer_code,sale_amt,profit
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20230401' and sdt<='20230430'
		and channel_code in('1','7','9')
		-- and goods_code not in ('8718','8708','8649','840509')
		and classify_middle_name='酒'
	) a 
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='20230430'
		) b on b.customer_code=a.customer_code
	left join
		(
		select
			goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name
		from
			csx_dim.csx_dim_basic_goods
		where
			sdt='current'
		) c on c.goods_code=a.goods_code
group by 
	a.performance_province_name,b.sales_user_number,b.sales_user_name,
	c.classify_middle_name,c.classify_small_name,a.goods_code,c.goods_name