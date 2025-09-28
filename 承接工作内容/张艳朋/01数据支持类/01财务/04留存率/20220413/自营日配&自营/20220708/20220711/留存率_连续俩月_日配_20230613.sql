-- ====================================================================================================================================================
--连续两个月履约日配业务
drop table if exists csx_analyse_tmp.csx_analyse_tmp_ripei_lianxu_months;
create table csx_analyse_tmp.csx_analyse_tmp_ripei_lianxu_months
as
select
	customer_code,next_month
from
	(
	select
		customer_code,
		s_month,
		lead(s_month,1,0) over (partition by customer_code order by s_month) as next_month,
		row_number() over(partition by customer_code order by s_month) as rn
	from
		(
		select
			customer_code,substr(sdt,1,6) as s_month,sum(sale_amt_no_tax) as sale_amt_no_tax
		from
			-- csx_dw.dws_sale_r_d_detail 
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20190101' and sdt<='20230430'
			and channel_code in('1','7','9')			
			and business_type_code=1
		group by 
			customer_code,substr(sdt,1,6)
		) t1
	) t1
where
	rn=1
	and substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(s_month,'01'),'yyyyMMdd')),1),'-',''),1,6)=next_month
group by 
	customer_code,next_month
;

--日配_所有客户
drop table if exists csx_analyse_tmp.csx_analyse_tmp_ripei_all_customer;
create table csx_analyse_tmp.csx_analyse_tmp_ripei_all_customer
as
select 
	c.performance_province_name,
	a.new_smonth,
	c.second_category_name,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01'))) diff_month,
	count(distinct b.customer_code) counts,
	sum(b.sale_amt_no_tax) sale_amt_no_tax,
	sum(profit_no_tax) as profit_no_tax,
	sum(days_cnt) as days_cnt
from
	(
	select 
		customer_code,substr(next_month,1,6) new_smonth
	from
		csx_analyse_tmp.csx_analyse_tmp_ripei_lianxu_months
	where 
		substr(next_month,1,6)>='202101'
	)a
	join 	
		(
		select 
			customer_code,substr(sdt,1,6) smonth,
			sum(sale_amt_no_tax) sale_amt_no_tax,
			sum(profit_no_tax) as profit_no_tax,
			count(distinct sdt) as days_cnt
		from 
			-- csx_dw.dws_sale_r_d_detail
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20190101' and sdt<='20230430'
			and channel_code in('1','7','9')
			and business_type_code in (1) -- 日配
		group by 
			customer_code,substr(sdt,1,6)
		)b on a.customer_code=b.customer_code
	join
		(
		select 
			customer_code,customer_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			performance_province_name
		from 
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
select * from csx_analyse_tmp.csx_analyse_tmp_ripei_all_customer

-- ====================================================================================================================================================

--日配_至少A、B类有两个月的客户
drop table if exists csx_analyse_tmp.csx_analyse_tmp_ripei_ab_two_months_customer;
create table csx_analyse_tmp.csx_analyse_tmp_ripei_ab_two_months_customer
as
select 
	c.performance_province_name,
	a.new_smonth,
	c.second_category_name,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01'))) diff_month,
	count(distinct b.customer_code) counts,
	sum(b.sale_amt_no_tax) sale_amt_no_tax,
	sum(profit_no_tax) as profit_no_tax,
	sum(days_cnt) as days_cnt
from
	(
	select 
		customer_code,substr(next_month,1,6) new_smonth
	from
		csx_analyse_tmp.csx_analyse_tmp_ripei_lianxu_months
	where 
		substr(next_month,1,6)>='202101'
	)a
	join
		(
		select 
			customer_code,substr(sdt,1,6) smonth,
			sum(sale_amt_no_tax) sale_amt_no_tax,
			sum(profit_no_tax) as profit_no_tax,
			count(distinct sdt) as days_cnt
		from 
			-- csx_dw.dws_sale_r_d_detail
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20190101' and sdt<='20230430'
			and channel_code in('1','7','9')
			and business_type_code in (1)
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
	join
		( --至少出现过两个月AB类的客户
		select
			customer_no,count(distinct month) as months_cnt
		from 
			-- csx_dw.report_sale_r_m_customer_level
			csx_analyse.csx_analyse_report_sale_customer_level_mf
		where
			month>='202101' and month<='202304'
			and customer_large_level in ('A','B')
			and tag=1 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
		group by 
			customer_no
		having
			count(distinct month)>=2
		) d on d.customer_no=a.customer_code
group by
	c.performance_province_name, 
	a.new_smonth,
	c.second_category_name,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),
	concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01')))
;
select * from csx_analyse_tmp.csx_analyse_tmp_ripei_ab_two_months_customer
--====================================================================================================================================================

--日配_所有客户_一直到M12未流失的客户
drop table if exists csx_analyse_tmp.csx_analyse_tmp_ripei_all_m12_non_loss_customer;
create table csx_analyse_tmp.csx_analyse_tmp_ripei_all_m12_non_loss_customer
as
select 
	c.performance_province_name,
	a.new_smonth,
	c.second_category_name,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01'))) diff_month,
	count(distinct b.customer_code) counts,
	sum(b.sale_amt_no_tax) sale_amt_no_tax,
	sum(b.profit_no_tax) as profit_no_tax,
	sum(b.days_cnt) as days_cnt
from
	(
	select 
		customer_code,substr(next_month,1,6) new_smonth,
		case when substr(next_month,1,6)<='202204' then substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),12),'-',''),1,6)
			else substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),cast(months_between('2023-04-01',to_date(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd'))))as int)),'-',''),1,6)
		end as 12_months
		--substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),cast(months_between('2022-06-01',to_date(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd'))))as int)),'-',''),1,6) as 12_months
	from
		csx_analyse_tmp.csx_analyse_tmp_ripei_lianxu_months
	where 
		substr(next_month,1,6)>='202101'
	)a
	join 	
		(
		select 
			customer_code,substr(sdt,1,6) smonth,
			sum(sale_amt_no_tax) sale_amt_no_tax,
			sum(profit_no_tax) as profit_no_tax,
			count(distinct sdt) as days_cnt
		from 
			-- csx_dw.dws_sale_r_d_detail
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20190101' and sdt<='20230430'
			and channel_code in('1','7','9')
			and business_type_code in (1) -- 日配
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
	join 	
		(
		select 
			customer_code,substr(max(sdt),1,6) max_month
		from 
			-- csx_dw.dws_sale_r_d_detail
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20190101' and sdt<='20230430'
			and channel_code in('1','7','9')
			and business_type_code in (1) -- 日配
		group by 
			customer_code
		)d on a.customer_code=d.customer_code
where
	d.max_month>=a.12_months
group by
	c.performance_province_name, 
	a.new_smonth,
	c.second_category_name,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),
	concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01')))
;
select * from csx_analyse_tmp.csx_analyse_tmp_ripei_all_m12_non_loss_customer
--====================================================================================================================================================
--日配_至少A、B类有两个月的客户_一直到M12未流失的客户
drop table if exists csx_analyse_tmp.csx_analyse_tmp_ripei_ab_two_months_m12_non_loss_customer;
create table csx_analyse_tmp.csx_analyse_tmp_ripei_ab_two_months_m12_non_loss_customer
as
select 
	c.performance_province_name,
	a.new_smonth,
	c.second_category_name,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01'))) diff_month,
	count(distinct b.customer_code) counts,
	sum(b.sale_amt_no_tax) sale_amt_no_tax,
	sum(b.profit_no_tax) as profit_no_tax,
	sum(b.days_cnt) as days_cnt
from
	(
	select 
		customer_code,substr(next_month,1,6) new_smonth,
		case when substr(next_month,1,6)<='202204' then substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),12),'-',''),1,6)
			else substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),cast(months_between('2023-04-01',to_date(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd'))))as int)),'-',''),1,6)
		end as 12_months
		--substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),cast(months_between('2022-06-01',to_date(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd'))))as int)),'-',''),1,6) as 12_months
	from
		csx_analyse_tmp.csx_analyse_tmp_ripei_lianxu_months
	where 
		substr(next_month,1,6)>='202101'
	)a
	join 	
		(
		select 
			customer_code,substr(sdt,1,6) smonth,
			sum(sale_amt_no_tax) sale_amt_no_tax,
			sum(profit_no_tax) as profit_no_tax,
			count(distinct sdt) as days_cnt
		from 
			-- csx_dw.dws_sale_r_d_detail
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20190101' and sdt<='20230430'
			and channel_code in('1','7','9')
			and business_type_code in (1) -- 日配
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
	join 	
		(
		select 
			customer_code,substr(max(sdt),1,6) max_month
		from 
			-- csx_dw.dws_sale_r_d_detail
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20190101' and sdt<='20230430'
			and channel_code in('1','7','9')
			and business_type_code in (1) -- 日配
		group by 
			customer_code
		)d on a.customer_code=d.customer_code
	join
		( --至少出现过两个月AB类的客户
		select
			customer_no,count(distinct month) as months_cnt
		from 
			-- csx_dw.report_sale_r_m_customer_level
			csx_analyse.csx_analyse_report_sale_customer_level_mf
		where
			month>='202101' and month<='202304'
			and customer_large_level in ('A','B')
			and tag=1 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
		group by 
			customer_no
		having
			count(distinct month)>=2
		) e on e.customer_no=a.customer_code
where
	d.max_month>=a.12_months
group by
	c.performance_province_name, 
	a.new_smonth,
	c.second_category_name,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),
	concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01')))
;
select * from csx_analyse_tmp.csx_analyse_tmp_ripei_ab_two_months_m12_non_loss_customer

--====================================================================================================================================================

--日配_所有客户_一直到M12流失的客户
drop table if exists csx_analyse_tmp.csx_analyse_tmp_ripei_all_m12_loss_customer;
create table csx_analyse_tmp.csx_analyse_tmp_ripei_all_m12_loss_customer
as
select 
	c.performance_province_name,
	a.new_smonth,
	c.second_category_name,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01'))) diff_month,
	count(distinct b.customer_code) counts,
	sum(b.sale_amt_no_tax) sale_amt_no_tax,
	sum(b.profit_no_tax) as profit_no_tax,
	sum(b.days_cnt) as days_cnt
from
	(
	select 
		customer_code,substr(next_month,1,6) new_smonth,
		case when substr(next_month,1,6)<='202204' then substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),12),'-',''),1,6)
			else substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),cast(months_between('2023-04-01',to_date(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd'))))as int)),'-',''),1,6)
		end as 12_months
		--substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),cast(months_between('2022-06-01',to_date(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd'))))as int)),'-',''),1,6) as 12_months
	from
		csx_analyse_tmp.csx_analyse_tmp_ripei_lianxu_months
	where 
		substr(next_month,1,6)>='202101'
	)a
	join 	
		(
		select 
			customer_code,substr(sdt,1,6) smonth,
			sum(sale_amt_no_tax) sale_amt_no_tax,
			sum(profit_no_tax) as profit_no_tax,
			count(distinct sdt) as days_cnt
		from 
			-- csx_dw.dws_sale_r_d_detail
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20190101' and sdt<='20230430'
			and channel_code in('1','7','9')
			and business_type_code in (1) -- 日配
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
	left join 	
		(
		select 
			customer_code,substr(max(sdt),1,6) max_month
		from 
			-- csx_dw.dws_sale_r_d_detail
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20190101' and sdt<='20230430'
			and channel_code in('1','7','9')
			and business_type_code in (1) -- 日配
		group by 
			customer_code
		)d on a.customer_code=d.customer_code
where
	--d.customer_code is null
	d.max_month<a.12_months
group by
	c.performance_province_name, 
	a.new_smonth,
	c.second_category_name,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),
	concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01')))
;
select * from csx_analyse_tmp.csx_analyse_tmp_ripei_all_m12_loss_customer
--====================================================================================================================================================

--日配_至少A、B类有两个月的客户_一直到M12流失的客户
drop table if exists csx_analyse_tmp.csx_analyse_tmp_ripei_ab_two_months_m12_loss_customer;
create table csx_analyse_tmp.csx_analyse_tmp_ripei_ab_two_months_m12_loss_customer
as
select 
	c.performance_province_name,
	a.new_smonth,
	c.second_category_name,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01'))) diff_month,
	count(distinct b.customer_code) counts,
	sum(b.sale_amt_no_tax) sale_amt_no_tax,
	sum(b.profit_no_tax) as profit_no_tax,
	sum(b.days_cnt) as days_cnt
from
	(
	select 
		customer_code,substr(next_month,1,6) new_smonth,
		case when substr(next_month,1,6)<='202204' then substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),12),'-',''),1,6)
			else substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),cast(months_between('2023-04-01',to_date(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd'))))as int)),'-',''),1,6)
		end as 12_months
		--substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),cast(months_between('2022-06-01',to_date(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd'))))as int)),'-',''),1,6) as 12_months
	from
		csx_analyse_tmp.csx_analyse_tmp_ripei_lianxu_months
	where 
		substr(next_month,1,6)>='202101'
	)a
	join 	
		(
		select 
			customer_code,substr(sdt,1,6) smonth,
			sum(sale_amt_no_tax) sale_amt_no_tax,
			sum(profit_no_tax) as profit_no_tax,
			count(distinct sdt) as days_cnt
		from 
			-- csx_dw.dws_sale_r_d_detail
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20190101' and sdt<='20230430'
			and channel_code in('1','7','9')
			and business_type_code in (1) -- 日配
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
	left join 	
		(
		select 
			customer_code,substr(max(sdt),1,6) max_month
		from 
			-- csx_dw.dws_sale_r_d_detail
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20190101' and sdt<='20230430'
			and channel_code in('1','7','9')
			and business_type_code in (1) -- 日配
		group by 
			customer_code
		)d on a.customer_code=d.customer_code
	join
		( --至少出现过两个月AB类的客户
		select
			customer_no,count(distinct month) as months_cnt
		from 
			-- csx_dw.report_sale_r_m_customer_level
			csx_analyse.csx_analyse_report_sale_customer_level_mf
		where
			month>='202101' and month<='202304'
			and customer_large_level in ('A','B')
			and tag=1 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
		group by 
			customer_no
		having
			count(distinct month)>=2
		) e on e.customer_no=a.customer_code
where
	--d.customer_code is null
	d.max_month<a.12_months
group by
	c.performance_province_name, 
	a.new_smonth,
	c.second_category_name,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),
	concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01')))
;
select * from csx_analyse_tmp.csx_analyse_tmp_ripei_ab_two_months_m12_loss_customer

--====================================================================================================================================================

--日配_至少A、B类有两个月的客户_一直到M12流失的客户清单
drop table if exists csx_analyse_tmp.csx_analyse_tmp_ripei_ab_two_months_m12_loss_customer_list;
create table csx_analyse_tmp.csx_analyse_tmp_ripei_ab_two_months_m12_loss_customer_list
as
select distinct
	c.performance_province_name,
	a.customer_code,
	c.customer_name,
	a.new_smonth,
	-- a.12_months,
	c.sales_user_number,
	c.sales_user_name,
	c.first_category_name,
	c.second_category_name,
	c.third_category_name
from
	(
	select 
		customer_code,substr(next_month,1,6) new_smonth,
		case when substr(next_month,1,6)<='202204' then substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),12),'-',''),1,6)
			else substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),cast(months_between('2023-04-01',to_date(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd'))))as int)),'-',''),1,6)
		end as 12_months
		--substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),cast(months_between('2022-06-01',to_date(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd'))))as int)),'-',''),1,6)as 12_months
	from
		csx_analyse_tmp.csx_analyse_tmp_ripei_lianxu_months
	where 
		substr(next_month,1,6)>='202101'
	)a
	join 	
		(
		select 
			customer_code,substr(sdt,1,6) smonth,
			sum(sale_amt_no_tax) sale_amt_no_tax,
			sum(profit_no_tax) as profit_no_tax,
			count(distinct sdt) as days_cnt
		from 
			-- csx_dw.dws_sale_r_d_detail
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20190101' and sdt<='20230430'
			and channel_code in('1','7','9')
			and business_type_code in (1) -- 日配
		group by 
			customer_code,substr(sdt,1,6)
		)b on a.customer_code=b.customer_code
	join
		(
		select 
			customer_code,customer_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			performance_province_name,sales_user_number,sales_user_name
		from 
			-- csx_dw.dws_crm_w_a_customer
			csx_dim.csx_dim_crm_customer_info
		where 
			sdt = 'current'
			and channel_code in('1','7','9')
			and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
		)c on c.customer_code=a.customer_code
	left join 	
		(
		select 
			customer_code,substr(max(sdt),1,6) max_month
		from 
			-- csx_dw.dws_sale_r_d_detail
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20190101' and sdt<='20230430'
			and channel_code in('1','7','9')
			and business_type_code in (1) -- 日配
		group by 
			customer_code
		)d on a.customer_code=d.customer_code
	join
		( --至少出现过两个月AB类的客户
		select
			customer_no,count(distinct month) as months_cnt
		from 
			-- csx_dw.report_sale_r_m_customer_level
			csx_analyse.csx_analyse_report_sale_customer_level_mf
		where
			month>='202101' and month<='202304'
			and customer_large_level in ('A','B')
			and tag=1 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
		group by 
			customer_no
		having
			count(distinct month)>=2
		) e on e.customer_no=a.customer_code
where
	d.max_month<a.12_months
;
select * from csx_analyse_tmp.csx_analyse_tmp_ripei_ab_two_months_m12_loss_customer_list		