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
	a.customer_no,
	b.next_sdt
from
	(
	select
		customer_no,
		substr(sdt,1,6) as s_sdt
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt>='20190101' and sdt<='20210630' 
		and business_type_code='1'
		and sales_type !='fanli'
	group by 
		customer_no,substr(sdt,1,6)
	) as a 
	join
		(
		select
			customer_no,
			min(s_month) as min_month,
			substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(min(s_month),'01'),'yyyyMMdd')),1),'-',''),1,6) as next_month,
			max(s_month) as max_month
		from
			(
			select
				customer_no,substr(sdt,1,6) as s_month,sum(sales_value) as sales_value
			from
				csx_dw.dws_sale_r_d_detail 
			where 
				sdt>='20190101' and sdt<='20210630' 
				and business_type_code='1'
				and sales_type !='fanli'
			group by 
				customer_no,substr(sdt,1,6)
			having
				sum(sales_value)>0
			) t1
		) as b on b.customer_no=a.customer_no and b.next_sdt=a.s_sdt
group by 
	a.customer_no,
	b.next_sdt	
)

insert overwrite directory '/tmp/zhangyanpeng/20210720_linshi_2' row format delimited fields terminated by '\t' 
--结果表  
--判断各月新客，计算每月新客在之后各月是否有销售
select 
	c.sales_province_name,
	a.new_smonth,
	c.second_category_name,
	floor(months_between(concat(substr(d.smonth,1,4),'-',substr(d.smonth,5,2),'-','01'),concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01'))) diff_month,
	count(distinct a.customer_no) counts,
	sum(d.sales_value) sales_value,
	avg(d.sales_value) avg_sales_value
from
	(
	select 
		customer_no,substr(next_sdt,1,6) new_smonth
	from
		cur_sale_value
	where 
		substr(next_sdt,1,6)>='201901'
		-- and substr(min_sdt,1,6)<substr(max_sdt,1,6)  --至少销售跨两月的客户
		-- and count_day>1  --销售天数大于1
	)a
	join -- 日配业务 每月履约金额大于0	
		(
		select
			customer_no,smonth,
			sum(sales_value) as sales_value
		from
			(
			select 
				customer_no,substr(sdt,1,6) smonth,business_type_name,
				sum(sales_value) sales_value
			from 
				csx_dw.dws_sale_r_d_detail
			where 
				sdt>='20190101' and sdt<='20210630'
				and channel_code in('1','7','9')
				and business_type_code='1'
				and sales_type !='fanli'
			group by 
				customer_no,substr(sdt,1,6),business_type_name
			having
				sum(sales_value)>0
			) t1
		group by 
			customer_no,smonth
		)b on a.customer_no=b.customer_no
	join -- 限制长期客户
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
	left join -- 自营业务的收入 不含BBC
		(
		select
			customer_no,substr(sdt,1,6) as smonth,
			sum(sales_value) as sales_value
		from
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20190101' and sdt<='20210630'
			and channel_code in('1','7','9')
			and business_type_code not in ('4','6') -- 自营业务的收入 不含BBC
			and sales_type !='fanli'
		group by 
			customer_no,substr(sdt,1,6)
		)d on d.customer_no=a.customer_no		
group by
	c.sales_province_name, 
	a.new_smonth,
	c.second_category_name,
	floor(months_between(concat(substr(d.smonth,1,4),'-',substr(d.smonth,5,2),'-','01'),
	concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01')))
;



-- ====================================================================================================================================================
-- B端全体留存&arpu
--1、第一个月履约业务单有日配的算新客
--2、M0新客数为当月首次履约的自营日配业务的非一次性客户数
--3、M1-M24的老客数为以后每月履约的任何自营业务的客户数（多种单据类型只算一次）


--2、留存率
-- 客户最小成交日期 、首单日期 首单--首日
with cur_sale_value_2 as 
(
select
	a.customer_no,
	b.next_sdt
from
	(
	select
		customer_no,
		substr(sdt,1,6) as s_sdt
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt>='20190101' and sdt<='20210630' 
		and channel_code in ('1','7','9')
		-- and business_type_code='1'
	group by 
		customer_no,substr(sdt,1,6)
	) as a 
	join
		(
		select
			customer_no,
			substr(min(sdt),1,6) as min_sdt,
			substr(regexp_replace(add_months(from_unixtime(unix_timestamp(min(sdt),'yyyyMMdd')),1),'-',''),1,6) as next_sdt,
			substr(max(sdt),1,6) as max_sdt
		from
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt>='20190101' and sdt<='20210630' 
			and channel_code in ('1','7','9')
			-- and business_type_code='1'
		group by 
			customer_no
		) as b on b.customer_no=a.customer_no and b.next_sdt=a.s_sdt
group by 
	a.customer_no,
	b.next_sdt
)

insert overwrite directory '/tmp/zhangyanpeng/20210715_2_linshi_3' row format delimited fields terminated by '\t' 
--结果表  
--判断各月新客，计算每月新客在之后各月是否有销售
select
	c.sales_province_name, 
	a.new_smonth,
	c.second_category_name,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01'))) diff_month,
	count(distinct b.customer_no) counts,
	sum(b.sales_value) sales_value,
	avg(b.sales_value) avg_sales_value
from
	(
	select 
		customer_no,substr(next_sdt,1,6) new_smonth
	from
		cur_sale_value_2
	where 
		substr(next_sdt,1,6)>='201901'
		-- and substr(min_sdt,1,6)<substr(max_sdt,1,6)  --至少销售跨两月的客户
		-- and count_day>1  --销售天数大于1
	)a
	join 	
		(
		select 
			customer_no,substr(sdt,1,6) smonth,
			sum(sales_value) sales_value
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20190101' and sdt<='20210630'
			and channel_code in('1','7','9')
			-- and business_type_code<>'4' -- 自营业务的收入
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
group by 
	c.sales_province_name, 
	a.new_smonth,
	c.second_category_name,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),
	concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01')))
;		


	