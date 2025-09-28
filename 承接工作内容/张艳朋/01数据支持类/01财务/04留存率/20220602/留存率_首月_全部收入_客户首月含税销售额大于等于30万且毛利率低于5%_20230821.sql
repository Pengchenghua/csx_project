-- ====================================================================================================================================================
drop table if exists csx_analyse_tmp.csx_analyse_tmp_low_profit_customer;
create table csx_analyse_tmp.csx_analyse_tmp_low_profit_customer
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
		customer_code,
		substr(min(sdt),1,6) as new_smonth
	from 
		-- csx_dw.dws_sale_r_d_detail
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt >= '20190101' and sdt<=regexp_replace(date_sub(current_date(),1),'-','')
		and channel_code in('1','7','9')
		and ((business_type_code=1 and order_channel_code not in(4,6)) or business_type_code in (2,6))
	group by 
		customer_code
	having
		min(sdt)>='20190101' and min(sdt)<=regexp_replace(date_sub(current_date(),1),'-','')
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
			sdt>='20190101' and sdt<=regexp_replace(date_sub(current_date(),1),'-','')
			and channel_code in('1','7','9')
			and business_type_code in (1,2,6)
		group by 
			customer_code,substr(sdt,1,6)
		having
			sum(sale_amt) >= 300000 and sum(profit)/abs(sum(sale_amt)) < 0.05		
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
select * from csx_analyse_tmp.csx_analyse_tmp_low_profit_customer

--明细
--================================================================================================================================================================================

drop table if exists csx_analyse_tmp.csx_analyse_tmp_low_profit_customer_detail;
create table csx_analyse_tmp.csx_analyse_tmp_low_profit_customer_detail
as 
select 
	c.performance_province_name,
	c.performance_city_name,
	a.new_smonth,
	d.smonth,
	a.customer_code,
	c.customer_name,
	c.first_category_name,
	c.second_category_name,
	c.third_category_name,
	d.classify_large_name,
	d.classify_middle_name,
	d.classify_small_name,
	c.sales_user_number,
	c.sales_user_name,
	d.sale_amt,
	d.profit,
	d.profit_rate,
	d.sale_amt_no_tax,
	d.profit_no_tax,
	d.profit_no_tax_rate,
	e.months_cnt
from
	(
	select
		customer_code,
		substr(min(sdt),1,6) as new_smonth
	from 
		-- csx_dw.dws_sale_r_d_detail
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt >= '20190101' and sdt<=regexp_replace(date_sub(current_date(),1),'-','')
		and channel_code in('1','7','9')
		and ((business_type_code=1 and order_channel_code not in(4,6)) or business_type_code in (2,6))
	group by 
		customer_code
	having
		min(sdt)>='20190101' and min(sdt)<=regexp_replace(date_sub(current_date(),1),'-','')
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
			sdt>='20190101' and sdt<=regexp_replace(date_sub(current_date(),1),'-','')
			and channel_code in('1','7','9')
			and business_type_code in (1,2,6) 
		group by 
			customer_code,substr(sdt,1,6)
		having
			sum(sale_amt) >= 300000 and sum(profit)/abs(sum(sale_amt)) < 0.05		
		)b on a.customer_code=b.customer_code
	join
		(
		select 
			customer_code,customer_name,sales_user_number,sales_user_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			performance_province_name,performance_city_name
		from 
			-- csx_dw.dws_crm_w_a_customer
			csx_dim.csx_dim_crm_customer_info
		where 
			sdt = 'current'
			and channel_code in('1','7','9')
			and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
		)c on c.customer_code=a.customer_code
	join
		(
		select 
			customer_code,substr(sdt,1,6) smonth,classify_large_name,classify_middle_name,classify_small_name,
			sum(sale_amt) as sale_amt,
			sum(profit) as profit,
			sum(profit)/abs(sum(sale_amt)) as profit_rate,
			sum(sale_amt_no_tax) sale_amt_no_tax,
			sum(profit_no_tax) as profit_no_tax,
			sum(profit_no_tax)/abs(sum(sale_amt_no_tax)) as profit_no_tax_rate,
			count(distinct sdt) as days_cnt
		from 
			-- csx_dw.dws_sale_r_d_detail
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20190101' and sdt<=regexp_replace(date_sub(current_date(),1),'-','')
			and channel_code in('1','7','9')
			and business_type_code in (1,2,6) 
		group by 
			customer_code,substr(sdt,1,6),classify_large_name,classify_middle_name,classify_small_name
		)d on d.customer_code=b.customer_code and d.smonth=b.smonth
	left join
		(
		select
			customer_code,count(distinct substr(sdt,1,6)) as months_cnt
		from 
			-- csx_dw.dws_sale_r_d_detail
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20190101' and sdt<=regexp_replace(date_sub(current_date(),1),'-','')
			and channel_code in('1','7','9')
			and business_type_code in (1,2,6) 
		group by 
			customer_code
		) e on e.customer_code=a.customer_code
;
select * from csx_analyse_tmp.csx_analyse_tmp_low_profit_customer_detail
			
		