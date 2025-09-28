-- ====================================================================================================================================================
--连续两个月履约日配业务
drop table if exists csx_tmp.tmp_ripei_lianxu_months;
create table csx_tmp.tmp_ripei_lianxu_months
as
select
	customer_no,next_month
from
	(
	select
		customer_no,
		s_month,
		lead(s_month,1,0) over (partition by customer_no order by s_month) as next_month,
		row_number() over(partition by customer_no order by s_month) as rn
	from
		(
		select
			customer_no,substr(sdt,1,6) as s_month,sum(excluding_tax_sales) as excluding_tax_sales
		from
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt>='20190101' and sdt<='20220630'
			and channel_code in('1','7','9')			
			and business_type_code='1'
		group by 
			customer_no,substr(sdt,1,6)
		) t1
	) t1
where
	rn=1
	and substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(s_month,'01'),'yyyyMMdd')),1),'-',''),1,6)=next_month
group by 
	customer_no,next_month
;

insert overwrite directory '/tmp/zhangyanpeng/20220413_01_09' row format delimited fields terminated by '\t' 
--日配_所有客户
select 
	c.province_name,
	a.new_smonth,
	c.second_category_name,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01'))) diff_month,
	count(distinct b.customer_no) counts,
	sum(b.excluding_tax_sales) excluding_tax_sales,
	sum(excluding_tax_profit) as excluding_tax_profit,
	sum(days_cnt) as days_cnt
from
	(
	select 
		customer_no,substr(next_month,1,6) new_smonth
	from
		csx_tmp.tmp_ripei_lianxu_months
	where 
		substr(next_month,1,6)>='202101'
	)a
	join 	
		(
		select 
			customer_no,substr(sdt,1,6) smonth,
			sum(excluding_tax_sales) excluding_tax_sales,
			sum(excluding_tax_profit) as excluding_tax_profit,
			count(distinct sdt) as days_cnt
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20190101' and sdt<='20220630'
			and channel_code in('1','7','9')
			and business_type_code in ('1') -- 日配
		group by 
			customer_no,substr(sdt,1,6)
		)b on a.customer_no=b.customer_no
	join
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			sales_region_code,sales_region_name,sales_province_code,province_name,city_group_code,city_group_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and channel_code in('1','7','9')
			and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
		)c on c.customer_no=a.customer_no	
group by
	c.province_name, 
	a.new_smonth,
	c.second_category_name,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),
	concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01')))
;


-- ====================================================================================================================================================

insert overwrite directory '/tmp/zhangyanpeng/20220413_01_10' row format delimited fields terminated by '\t' 
--日配_至少A、B类有两个月的客户
select 
	c.province_name,
	a.new_smonth,
	c.second_category_name,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01'))) diff_month,
	count(distinct b.customer_no) counts,
	sum(b.excluding_tax_sales) excluding_tax_sales,
	sum(excluding_tax_profit) as excluding_tax_profit,
	sum(days_cnt) as days_cnt
from
	(
	select 
		customer_no,substr(next_month,1,6) new_smonth
	from
		csx_tmp.tmp_ripei_lianxu_months
	where 
		substr(next_month,1,6)>='202101'
	)a
	join
		(
		select 
			customer_no,substr(sdt,1,6) smonth,
			sum(excluding_tax_sales) excluding_tax_sales,
			sum(excluding_tax_profit) as excluding_tax_profit,
			count(distinct sdt) as days_cnt
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20190101' and sdt<='20220630'
			and channel_code in('1','7','9')
			and business_type_code in ('1')
		group by 
			customer_no,substr(sdt,1,6)
		)b on a.customer_no=b.customer_no
	join
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			sales_region_code,sales_region_name,sales_province_code,province_name,city_group_code,city_group_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and channel_code in('1','7','9')
			and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
		)c on c.customer_no=a.customer_no	
	join
		( --至少出现过两个月AB类的客户
		select
			customer_no,count(distinct month) as months_cnt
		from 
			csx_dw.report_sale_r_m_customer_level
		where
			month>='202101' and month<='202206'
			and customer_large_level in ('A','B')
			and tag=1 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
		group by 
			customer_no
		having
			count(distinct month)>=2
		) d on d.customer_no=a.customer_no
group by
	c.province_name, 
	a.new_smonth,
	c.second_category_name,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),
	concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01')))
;

--====================================================================================================================================================


insert overwrite directory '/tmp/zhangyanpeng/20220413_01_11' row format delimited fields terminated by '\t' 
--日配_所有客户_一直到M12未流失的客户
select 
	c.province_name,
	a.new_smonth,
	c.second_category_name,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01'))) diff_month,
	count(distinct b.customer_no) counts,
	sum(b.excluding_tax_sales) excluding_tax_sales,
	sum(b.excluding_tax_profit) as excluding_tax_profit,
	sum(b.days_cnt) as days_cnt
from
	(
	select 
		customer_no,substr(next_month,1,6) new_smonth,
		--case when substr(next_month,1,6)<='202106' then substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),12),'-',''),1,6)
		--	else substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),cast(months_between('2022-06-01',to_date(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd'))))as int)),'-',''),1,6)
		--end as 12_months
		substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),cast(months_between('2022-06-01',to_date(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd'))))as int)),'-',''),1,6) as 12_months
	from
		csx_tmp.tmp_ripei_lianxu_months
	where 
		substr(next_month,1,6)>='202101'
	)a
	join 	
		(
		select 
			customer_no,substr(sdt,1,6) smonth,
			sum(excluding_tax_sales) excluding_tax_sales,
			sum(excluding_tax_profit) as excluding_tax_profit,
			count(distinct sdt) as days_cnt
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20190101' and sdt<='20220630'
			and channel_code in('1','7','9')
			and business_type_code in ('1') -- 日配
		group by 
			customer_no,substr(sdt,1,6)
		)b on a.customer_no=b.customer_no
	join
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			sales_region_code,sales_region_name,sales_province_code,province_name,city_group_code,city_group_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and channel_code in('1','7','9')
			and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
		)c on c.customer_no=a.customer_no
	join 	
		(
		select 
			customer_no,substr(sdt,1,6) smonth
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20190101' and sdt<='20220630'
			and channel_code in('1','7','9')
			and business_type_code in ('1') -- 日配
		group by 
			customer_no,substr(sdt,1,6)
		)d on a.customer_no=d.customer_no and a.12_months=d.smonth
group by
	c.province_name, 
	a.new_smonth,
	c.second_category_name,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),
	concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01')))
;

--====================================================================================================================================================


insert overwrite directory '/tmp/zhangyanpeng/20220413_01_12' row format delimited fields terminated by '\t' 
--日配_至少A、B类有两个月的客户_一直到M12未流失的客户
select 
	c.province_name,
	a.new_smonth,
	c.second_category_name,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01'))) diff_month,
	count(distinct b.customer_no) counts,
	sum(b.excluding_tax_sales) excluding_tax_sales,
	sum(b.excluding_tax_profit) as excluding_tax_profit,
	sum(b.days_cnt) as days_cnt
from
	(
	select 
		customer_no,substr(next_month,1,6) new_smonth,
		--case when substr(next_month,1,6)<='202106' then substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),12),'-',''),1,6)
		--	else substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),cast(months_between('2022-06-01',to_date(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd'))))as int)),'-',''),1,6)
		--end as 12_months
		substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),cast(months_between('2022-06-01',to_date(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd'))))as int)),'-',''),1,6) as 12_months
	from
		csx_tmp.tmp_ripei_lianxu_months
	where 
		substr(next_month,1,6)>='202101'
	)a
	join 	
		(
		select 
			customer_no,substr(sdt,1,6) smonth,
			sum(excluding_tax_sales) excluding_tax_sales,
			sum(excluding_tax_profit) as excluding_tax_profit,
			count(distinct sdt) as days_cnt
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20190101' and sdt<='20220630'
			and channel_code in('1','7','9')
			and business_type_code in ('1') -- 日配
		group by 
			customer_no,substr(sdt,1,6)
		)b on a.customer_no=b.customer_no
	join
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			sales_region_code,sales_region_name,sales_province_code,province_name,city_group_code,city_group_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and channel_code in('1','7','9')
			and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
		)c on c.customer_no=a.customer_no
	join 	
		(
		select 
			customer_no,substr(sdt,1,6) smonth
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20190101' and sdt<='20220630'
			and channel_code in('1','7','9')
			and business_type_code in ('1') -- 日配
		group by 
			customer_no,substr(sdt,1,6)
		)d on a.customer_no=d.customer_no and a.12_months=d.smonth
	join
		( --至少出现过两个月AB类的客户
		select
			customer_no,count(distinct month) as months_cnt
		from 
			csx_dw.report_sale_r_m_customer_level
		where
			month>='202101' and month<='202206'
			and customer_large_level in ('A','B')
			and tag=1 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
		group by 
			customer_no
		having
			count(distinct month)>=2
		) e on e.customer_no=a.customer_no
group by
	c.province_name, 
	a.new_smonth,
	c.second_category_name,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),
	concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01')))
;




--====================================================================================================================================================


insert overwrite directory '/tmp/zhangyanpeng/20220413_01_13' row format delimited fields terminated by '\t' 
--日配_所有客户_一直到M12流失的客户
select 
	c.province_name,
	a.new_smonth,
	c.second_category_name,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01'))) diff_month,
	count(distinct b.customer_no) counts,
	sum(b.excluding_tax_sales) excluding_tax_sales,
	sum(b.excluding_tax_profit) as excluding_tax_profit,
	sum(b.days_cnt) as days_cnt
from
	(
	select 
		customer_no,substr(next_month,1,6) new_smonth,
		--case when substr(next_month,1,6)<='202106' then substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),12),'-',''),1,6)
		--	else substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),cast(months_between('2022-06-01',to_date(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd'))))as int)),'-',''),1,6)
		--end as 12_months
		substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),cast(months_between('2022-06-01',to_date(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd'))))as int)),'-',''),1,6) as 12_months
	from
		csx_tmp.tmp_ripei_lianxu_months
	where 
		substr(next_month,1,6)>='202101'
	)a
	join 	
		(
		select 
			customer_no,substr(sdt,1,6) smonth,
			sum(excluding_tax_sales) excluding_tax_sales,
			sum(excluding_tax_profit) as excluding_tax_profit,
			count(distinct sdt) as days_cnt
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20190101' and sdt<='20220630'
			and channel_code in('1','7','9')
			and business_type_code in ('1') -- 日配
		group by 
			customer_no,substr(sdt,1,6)
		)b on a.customer_no=b.customer_no
	join
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			sales_region_code,sales_region_name,sales_province_code,province_name,city_group_code,city_group_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and channel_code in('1','7','9')
			and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
		)c on c.customer_no=a.customer_no
	left join 	
		(
		select 
			customer_no,substr(sdt,1,6) smonth
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20190101' and sdt<='20220630'
			and channel_code in('1','7','9')
			and business_type_code in ('1') -- 日配
		group by 
			customer_no,substr(sdt,1,6)
		)d on a.customer_no=d.customer_no and a.12_months=d.smonth
where
	d.customer_no is null
group by
	c.province_name, 
	a.new_smonth,
	c.second_category_name,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),
	concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01')))
;

--====================================================================================================================================================


insert overwrite directory '/tmp/zhangyanpeng/20220413_01_14' row format delimited fields terminated by '\t' 
--日配_至少A、B类有两个月的客户_一直到M12流失的客户
select 
	c.province_name,
	a.new_smonth,
	c.second_category_name,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01'))) diff_month,
	count(distinct b.customer_no) counts,
	sum(b.excluding_tax_sales) excluding_tax_sales,
	sum(b.excluding_tax_profit) as excluding_tax_profit,
	sum(b.days_cnt) as days_cnt
from
	(
	select 
		customer_no,substr(next_month,1,6) new_smonth,
		--case when substr(next_month,1,6)<='202106' then substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),12),'-',''),1,6)
		--	else substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),cast(months_between('2022-06-01',to_date(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd'))))as int)),'-',''),1,6)
		--end as 12_months
		substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),cast(months_between('2022-06-01',to_date(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd'))))as int)),'-',''),1,6) as 12_months
	from
		csx_tmp.tmp_ripei_lianxu_months
	where 
		substr(next_month,1,6)>='202101'
	)a
	join 	
		(
		select 
			customer_no,substr(sdt,1,6) smonth,
			sum(excluding_tax_sales) excluding_tax_sales,
			sum(excluding_tax_profit) as excluding_tax_profit,
			count(distinct sdt) as days_cnt
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20190101' and sdt<='20220630'
			and channel_code in('1','7','9')
			and business_type_code in ('1') -- 日配
		group by 
			customer_no,substr(sdt,1,6)
		)b on a.customer_no=b.customer_no
	join
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			sales_region_code,sales_region_name,sales_province_code,province_name,city_group_code,city_group_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and channel_code in('1','7','9')
			and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
		)c on c.customer_no=a.customer_no
	left join 	
		(
		select 
			customer_no,substr(sdt,1,6) smonth
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20190101' and sdt<='20220630'
			and channel_code in('1','7','9')
			and business_type_code in ('1') -- 日配
		group by 
			customer_no,substr(sdt,1,6)
		)d on a.customer_no=d.customer_no and a.12_months=d.smonth
	join
		( --至少出现过两个月AB类的客户
		select
			customer_no,count(distinct month) as months_cnt
		from 
			csx_dw.report_sale_r_m_customer_level
		where
			month>='202101' and month<='202206'
			and customer_large_level in ('A','B')
			and tag=1 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
		group by 
			customer_no
		having
			count(distinct month)>=2
		) e on e.customer_no=a.customer_no
where
	d.customer_no is null
group by
	c.province_name, 
	a.new_smonth,
	c.second_category_name,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),
	concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01')))
;


--====================================================================================================================================================


insert overwrite directory '/tmp/zhangyanpeng/20220413_01_15' row format delimited fields terminated by '\t' 
--日配_至少A、B类有两个月的客户_一直到M12流失的客户清单
select distinct
	c.province_name,
	a.customer_no,
	c.customer_name,
	a.new_smonth,
	--a.12_months,
	c.work_no,
	c.sales_name,
	c.first_category_name,
	c.second_category_name,
	c.third_category_name
from
	(
	select 
		customer_no,substr(next_month,1,6) new_smonth,
		--case when substr(next_month,1,6)<='202106' then substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),12),'-',''),1,6)
		--	else substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),cast(months_between('2022-06-01',to_date(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd'))))as int)),'-',''),1,6)
		--end as 12_months
		substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),cast(months_between('2022-06-01',to_date(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd'))))as int)),'-',''),1,6)as 12_months
	from
		csx_tmp.tmp_ripei_lianxu_months
	where 
		substr(next_month,1,6)>='202101'
	)a
	join 	
		(
		select 
			customer_no,substr(sdt,1,6) smonth,
			sum(excluding_tax_sales) excluding_tax_sales,
			sum(excluding_tax_profit) as excluding_tax_profit,
			count(distinct sdt) as days_cnt
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20190101' and sdt<='20220630'
			and channel_code in('1','7','9')
			and business_type_code in ('1') -- 日配
		group by 
			customer_no,substr(sdt,1,6)
		)b on a.customer_no=b.customer_no
	join
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			sales_region_code,sales_region_name,sales_province_code,province_name,city_group_code,city_group_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and channel_code in('1','7','9')
			and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
		)c on c.customer_no=a.customer_no
	left join 	
		(
		select 
			customer_no,substr(sdt,1,6) smonth
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20190101' and sdt<='20220630'
			and channel_code in('1','7','9')
			and business_type_code in ('1') -- 日配
		group by 
			customer_no,substr(sdt,1,6)
		)d on a.customer_no=d.customer_no and a.12_months=d.smonth
	join
		( --至少出现过两个月AB类的客户
		select
			customer_no,count(distinct month) as months_cnt
		from 
			csx_dw.report_sale_r_m_customer_level
		where
			month>='202101' and month<='202206'
			and customer_large_level in ('A','B')
			and tag=1 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
		group by 
			customer_no
		having
			count(distinct month)>=2
		) e on e.customer_no=a.customer_no
where
	d.customer_no is null
;		