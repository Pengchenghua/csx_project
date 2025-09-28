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
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20190101' and sdt<='20231231'
			and channel_code in('1','7','9')			
		--	and business_type_code=1
			and business_type_code in (1,2,6)
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
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20190101' and sdt<='20231231'
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
			csx_analyse.csx_analyse_report_sale_customer_level_mf
		where
			month>='202101' and month<='202312'
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



--日配+福利+bbc_所有客户
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
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20190101' and sdt<='20231231'
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
union all 

;
select * from csx_analyse_tmp.csx_analyse_tmp_ripei_all_customer


-- 日配连续两个月

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
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20190101' and sdt<='20231231'
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
select * from csx_analyse_tmp.csx_analyse_tmp_ripei_ab_two_months_customer

-- 福利+BBC 留存


drop table if exists csx_analyse_tmp.csx_analyse_tmp_ripei_lianxu_bbc;
create table csx_analyse_tmp.csx_analyse_tmp_ripei_lianxu_bbc
as
select
	customer_code,next_month,min_sdt
from
	(
	select
		customer_code,
		s_month,
		min(s_month)over(partition by customer_code ) min_sdt,
		lead(s_month,1,0) over (partition by customer_code order by s_month) as next_month,
		row_number() over(partition by customer_code order by s_month) as rn
	from
		(
		select
			customer_code, substr(sdt,1,6) as s_month,sum(sale_amt_no_tax) as sale_amt_no_tax
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20190101' and sdt<='20231231'
			and channel_code in('1','7','9')			
		--	and business_type_code=1
			and business_type_code in (2,6)
		group by 
			customer_code ,substr(sdt,1,6)
		) t1
	) t1
where
	rn=1
--	and substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(s_month,'01'),'yyyyMMdd')),1),'-',''),1,6)=next_month
group by 
 	customer_code,next_month,min_sdt
;


select
	performance_province_name,
	second_category_name,
	new_smonth,
	diff_month,
	count(distinct customer_code) counts,
	sum(sale_amt_no_tax) sale_amt_no_tax,
	sum(profit_no_tax) as profit_no_tax,
	sum(days_cnt) as days_cnt
from
	(
	select 
		c.performance_province_name,
 		a.new_smonth,
		b.smonth,
		second_category_name,
		floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01'))) diff_month,
		b.customer_code,
	--	case when a.new_smonth>=d.first_month then 'B端转化' else '非B端转化' end as type_2,
		b.sale_amt_no_tax,
		b.profit_no_tax,days_cnt
	from
		(
		select 
			customer_code,substr(next_month,1,6)  new_smonth		-- 待验证日期取最早交易日期
		from
			csx_analyse_tmp.csx_analyse_tmp_ripei_lianxu_bbc
			where 
		    substr(min_sdt,1,6)>='202101'
		)a
		 join -- 每月日配业务履约金额大于0	
			(
			select 
				customer_code,substr(sdt,1,6) smonth,
				sum(sale_amt_no_tax) sale_amt_no_tax,
				sum(profit_no_tax) as profit_no_tax,
	            count(distinct sdt) as days_cnt
	from 
			  	csx_dws.csx_dws_sale_detail_di
			where 
				sdt>='20190101' and sdt<='20231231'
				and channel_code in('1','7','9')
				and business_type_code in('6','2') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
				--and sales_type !='fanli'
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
		)c on c.customer_code=b.customer_code
	
	) as a 
group by
	performance_province_name,
	new_smonth,
	diff_month,second_category_name
