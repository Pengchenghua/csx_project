
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_sale_classify;
create table csx_analyse_tmp.csx_analyse_tmp_finance_sale_classify
as
select
	a.performance_province_name,a.business_type_name,c.classify_large_name,c.classify_middle_name,c.classify_small_name,
	sum(sale_amt) as sale_amt,
	sum(sale_amt_no_tax) as sale_amt_no_tax,
	sum(profit_no_tax) as profit_no_tax,
	substr(sdt,1,4) as syear
from
	(
	select
		sdt,performance_province_name,business_type_name,customer_code,goods_code,sale_amt,sale_amt_no_tax,profit_no_tax
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20200101' and sdt<='20211231'
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
			goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name
		from
			csx_dim.csx_dim_basic_goods
		where
			sdt='current'
		) c on c.goods_code=a.goods_code
group by 
	a.performance_province_name,a.business_type_name,c.classify_large_name,c.classify_middle_name,c.classify_small_name,substr(sdt,1,4)
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_sale_classify
;


drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_sale_customer;
create table csx_analyse_tmp.csx_analyse_tmp_finance_sale_customer
as
select
	a.performance_province_name,a.customer_code,b.customer_name,substr(a.sdt,1,4) as syear,a.business_type_name,
	b.first_category_name,b.second_category_name,
	sum(sale_amt) as sale_amt,
	sum(sale_amt_no_tax) as sale_amt_no_tax,
	sum(profit) as profit,
	sum(profit_no_tax) as profit_no_tax
from
	(
	select
		sdt,performance_province_name,business_type_name,customer_code,goods_code,sale_amt,sale_amt_no_tax,profit_no_tax,profit
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20200101' and sdt<='20211231'
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
			goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name
		from
			csx_dim.csx_dim_basic_goods
		where
			sdt='current'
		) c on c.goods_code=a.goods_code
group by 
	a.performance_province_name,a.customer_code,b.customer_name,substr(a.sdt,1,4),a.business_type_name,
	b.first_category_name,b.second_category_name
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_sale_customer

