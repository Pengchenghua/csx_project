-- ====================================================================================================================================================
drop table if exists csx_analyse_tmp.csx_analyse_tmp_liucun_20230104;
create table csx_analyse_tmp.csx_analyse_tmp_liucun_20230104
as
select 
	c.performance_province_name,
	a.new_smonth,
	c.second_category_name,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01'))) diff_month,
	count(distinct b.customer_code) counts,
	sum(b.sale_amt) as sale_amt,
	sum(b.sale_amt_no_tax) sale_amt_no_tax,
	sum(profit_no_tax) as profit_no_tax,
	sum(days_cnt) as days_cnt
from
	(
	select 
		customer_code,substr(first_business_sale_date,1,6) as new_smonth
	from 
		-- csx_dw.dws_crm_w_a_customer_active
		csx_dws.csx_dws_crm_customer_business_active_di
	where 
		sdt = 'current' 
		and business_type_code=1
		and substr(first_business_sale_date,1,6) between '201901' and '202212'
	group by 
		customer_code,substr(first_business_sale_date,1,6)
	)a
	join
		(
		select 
			customer_code,substr(sdt,1,6) smonth,
			sum(sale_amt) as sale_amt,
			sum(sale_amt_no_tax) sale_amt_no_tax,
			sum(profit_no_tax) as profit_no_tax,
			count(distinct sdt) as days_cnt
		from 
			-- csx_dw.dws_sale_r_d_detail
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20190101' and sdt<='20221231'
			and channel_code in('1','7','9')
		group by 
			customer_code,substr(sdt,1,6)
		)b on a.customer_code=b.customer_code
	join
		(
		select 
			customer_code,customer_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			performance_province_name
		from 
			-- csx_dw.dws_crm_w_a_customer
			csx_dim.csx_dim_crm_customer_info
		where 
			sdt = 'current'
			and channel_code in('1','7','9')
			and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
		)c on c.customer_code=a.customer_code	
group by
	c.performance_province_name, 
	a.new_smonth,
	c.second_category_name,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),
	concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01')))
;
select * from csx_analyse_tmp.csx_analyse_tmp_liucun_20230104