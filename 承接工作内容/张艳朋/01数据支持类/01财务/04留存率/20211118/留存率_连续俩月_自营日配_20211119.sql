-- ====================================================================================================================================================

drop table csx_tmp.tmp_self_two_consecutive_months;
create table csx_tmp.tmp_self_two_consecutive_months
as 
select
	customer_no,next_month,
	substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),5),'-',''),1,6) as four_months_after
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
			sdt>='20190101' and sdt<='20211031' 
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
	customer_no,next_month,
	substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),4),'-',''),1,6)
;

insert overwrite directory '/tmp/zhangyanpeng/1119_02_01' row format delimited fields terminated by '\t' 

select 
	c.sales_region_name,
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
		customer_no,substr(next_month,1,6) new_smonth
	from
		csx_tmp.tmp_self_two_consecutive_months
	where 
		substr(next_month,1,6)>='201901'
	)a
	join -- 每月日配业务履约金额大于0	
		(
		select 
			customer_no,substr(sdt,1,6) smonth,
			sum(excluding_tax_sales) excluding_tax_sales,
			sum(excluding_tax_profit) as excluding_tax_profit,
			count(distinct sdt) as days_cnt
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20190101' and sdt<='20211031'
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
group by
	c.sales_region_name,
	c.sales_province_name, 
	a.new_smonth,
	c.second_category_name,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),
	concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01')))
;

	