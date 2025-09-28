--1、第一个月履约业务单有日配的算新客
--2、新客剔除试配一次性客户标识
--3、新客之后履约月份发生任何业务单都不算流失



--2、留存率
-- 客户最小成交日期 、首单日期 首单--首日
drop table if exists csx_tmp.tmp_cust_sale_20210202;
create table csx_tmp.tmp_cust_sale_20210202
as 
select
	customer_no,min(sdt) as min_sdt,max(sdt) as max_sdt,count(distinct sdt) as count_day
from 
	csx_dw.dws_sale_r_d_detail 
where 
	sdt>='20190101' and sdt<'20210201' 
	--and (order_category_desc='NORMAL' or order_category_desc is null)
	--and business_type_code<>'4'
	and business_type_code='1'
	and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046','OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
group by 
	customer_no
;

--结果表  
--判断各月新客，计算每月新客在之后各月是否有销售
select 
	a.new_smonth,
	--b.smonth,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01'))) diff_month,
	count(distinct b.customer_no) counts
	--sum(b.sales_value) sales_value,
	--avg(b.sales_value) avg_sales_value
from
	(
	select 
		customer_no,substr(min_sdt,1,6) new_smonth
	from 
		csx_tmp.tmp_cust_sale_20210202
	where 
		substr(min_sdt,1,6)>='202001'
		and substr(min_sdt,1,6)<substr(max_sdt,1,6)  --至少销售跨两月的客户
		--and count_day>1  --销售天数大于1
	)a
	join 	
		(
		select 
			customer_no,substr(sdt,1,6) smonth,
			sum(sales_value) sales_value
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20200101' and sdt<'20210201'
			and channel_code in('1','7','9')
			and business_type_code='1'
		group by 
			customer_no,substr(sdt,1,6)
		)b on a.customer_no=b.customer_no
	join
		(
		select 
			regexp_replace(split(sign_time, ' ')[0], '-', '') as sign_date,*
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
		and channel_code in('1','7','9')
		--and cooperation_mode_code='01'
		)c on c.customer_no=a.customer_no		
group by 
	a.new_smonth,
	--b.smonth
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01')))
;	

