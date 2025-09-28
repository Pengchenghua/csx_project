drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_customer_sale_20230130;
create table csx_analyse_tmp.csx_analyse_tmp_finance_customer_sale_20230130
as
select
	b.performance_province_name,
	b.first_category_name,
	b.second_category_name,
	count(distinct a.customer_code) as customer_cnt,
	sum(a.sale_amt) as sale_amt,
	sum(a.sale_amt_no_tax) as sale_amt_no_tax,
	sum(a.profit) as profit,
	sum(a.profit_no_tax) as profit_no_tax
from
	(
	select
		sdt,customer_code,channel_name,business_type_name,sale_amt,sale_amt_no_tax,profit,profit_no_tax
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20220101' and sdt<='20221231'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		and business_type_code=1 -- 日配
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
			-- and second_category_name in ('部队','教育')
		) b on b.customer_code=a.customer_code
group by 
	b.performance_province_name,
	b.first_category_name,
	b.second_category_name
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
