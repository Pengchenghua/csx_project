-- ====================================================================================================================================================
-- 自营日配留存&arpu
--1、第一个月履约业务单有日配的算新客
--2、M0新客数为当月首次履约的自营日配业务的非一次性客户数
--3、M1-M24的老客数为以后每月履约的任何自营业务的客户数（多种单据类型只算一次）


--2、留存率
-- 客户最小成交日期 、首单日期 首单--首日

with cur_sale_value as 
(
select
	customer_no,
	min(sdt) as min_sdt,
	max(sdt) as max_sdt,
	sum(sales_value) as sales_value
from 
	csx_dw.dws_sale_r_d_detail 
where 
	sdt>='20190101' and sdt<='20210630' 
	and channel_code in('1','7','9')
	and business_type_code='1'
group by 
	customer_no
)

insert overwrite directory '/tmp/zhangyanpeng/20210812_01_01' row format delimited fields terminated by '\t' 
--结果表  
--判断各月新客，计算每月新客在之后各月是否有销售
select 
	c.sales_province_name,
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
		customer_no,substr(min_sdt,1,6) new_smonth
	from
		cur_sale_value
	where 
		substr(min_sdt,1,6)>='201901'
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
			sdt>='20190101' and sdt<='20210630'
			and channel_code in('1','7','9')
			and business_type_code in ('1') -- 日配
		group by 
			customer_no,substr(sdt,1,6)
		)b on a.customer_no=b.customer_no
	join
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			sales_region_code,sales_region_name,sales_province_code,sales_province_name,city_group_code,city_group_name
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
			customer_no,
			min(sdt) as min_sdt,
			max(sdt) as max_sdt,
			sum(sales_value) as sales_value
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt>='20190101' and sdt<='20210630' 
			and channel_code in('1','7','9')
			and business_type_code not in('4','6')
		group by 
			customer_no
		having
			sum(sales_value)/months_between(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),from_unixtime(unix_timestamp(min(sdt),'yyyyMMdd')))>=150000
		) d on d.customer_no=a.customer_no
group by
	c.sales_province_name, 
	a.new_smonth,
	c.second_category_name,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),
	concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01')))
;



-- ====================================================================================================================================================
-- 自营日配留存&arpu
--1、第一个月履约业务单有日配的算新客
--2、M0新客数为当月首次履约的自营日配业务的非一次性客户数
--3、M1-M24的老客数为以后每月履约的任何自营业务的客户数（多种单据类型只算一次）


--2、留存率
-- 客户最小成交日期 、首单日期 首单--首日

--with cur_sale_value_2 as 
--(
--select
--	customer_no,
--	min(sdt) as min_sdt,
--	max(sdt) as max_sdt,
--	sum(sales_value) as sales_value
--from 
--	csx_dw.dws_sale_r_d_detail 
--where 
--	sdt>='20190101' and sdt<='20210630' 
--	and channel_code in('1','7','9')
--	and business_type_code='1'
--group by 
--	customer_no
--)

insert overwrite directory '/tmp/zhangyanpeng/20210812_01_02' row format delimited fields terminated by '\t' 
--结果表  
--判断各月新客，计算每月新客在之后各月是否有销售
select 
	c.sales_province_name,
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
		customer_no,substr(min_sdt,1,6) new_smonth
	from
		cur_sale_value
	where 
		substr(min_sdt,1,6)>='201901'
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
			sdt>='20190101' and sdt<='20210630'
			and channel_code in('1','7','9')
			and business_type_code not in ('4','6') -- 
		group by 
			customer_no,substr(sdt,1,6)
		)b on a.customer_no=b.customer_no
	join
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			sales_region_code,sales_region_name,sales_province_code,sales_province_name,city_group_code,city_group_name
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
			customer_no,
			min(sdt) as min_sdt,
			max(sdt) as max_sdt,
			sum(sales_value) as sales_value
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt>='20190101' and sdt<='20210630' 
			and channel_code in('1','7','9')
			and business_type_code not in('4','6')
		group by 
			customer_no
		having
			sum(sales_value)/months_between(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),from_unixtime(unix_timestamp(min(sdt),'yyyyMMdd')))>=150000
		) d on d.customer_no=a.customer_no		
group by
	c.sales_province_name, 
	a.new_smonth,
	c.second_category_name,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),
	concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01')))
;


