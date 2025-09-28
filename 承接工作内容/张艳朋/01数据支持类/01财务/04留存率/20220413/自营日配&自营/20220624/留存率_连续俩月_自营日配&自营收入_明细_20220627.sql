--客户信息
insert overwrite directory '/tmp/zhangyanpeng/20220624_01_01' row format delimited fields terminated by '\t' 
select
	c.province_name,
	a.customer_no,
	c.customer_name,
	a.new_smonth,
	c.work_no,
	c.sales_name,
	c.first_category_name,
	c.second_category_name,
	c.third_category_name,
	d.normal_first_order_date,
	d.normal_last_order_date
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
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			sales_region_code,sales_region_name,province_code,province_name,city_group_code,city_group_name
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
			customer_no,first_order_date,last_order_date,normal_first_order_date,normal_last_order_date
		from
			csx_dw.dws_crm_w_a_customer_active
		where
			sdt='current'
		) d on d.customer_no=a.customer_no
;


--每月履约
insert overwrite directory '/tmp/zhangyanpeng/20220624_01_02' row format delimited fields terminated by '\t' 
select 
	c.province_name,
	a.customer_no,
	c.customer_name,
	b.smonth,
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
			sdt>='20190101' and sdt<='20220626'
			and channel_code in('1','7','9')
			and business_type_code in ('1') -- 日配
		group by 
			customer_no,substr(sdt,1,6)
		)b on a.customer_no=b.customer_no
	join
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			sales_region_code,sales_region_name,province_code,province_name,city_group_code,city_group_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and channel_code in('1','7','9')
			and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
		)c on c.customer_no=a.customer_no	
group by
	c.province_name,
	a.customer_no,
	c.customer_name,
	b.smonth
;


--客户每月级别
insert overwrite directory '/tmp/zhangyanpeng/20220624_01_03' row format delimited fields terminated by '\t' 

select
	a.customer_no,a.month,a.customer_large_level
from 
	csx_dw.report_sale_r_m_customer_level a 
	join csx_tmp.tmp_ripei_lianxu_months b on b.customer_no=a.customer_no
where
	a.month='202206';

	
--客户最终级别	
insert overwrite directory '/tmp/zhangyanpeng/20220624_01_04' row format delimited fields terminated by '\t' 
select
	a.customer_no,a.month,a.customer_large_level
from
	(
	select
		a.customer_no,a.month,a.customer_large_level,row_number()over(partition by a.customer_no order by a.month desc) as rn
	from 
		csx_dw.report_sale_r_m_customer_level a 
		join csx_tmp.tmp_ripei_lianxu_months b on b.customer_no=a.customer_no
	) a 
where
	a.rn=1
;

--客户A类月数
insert overwrite directory '/tmp/zhangyanpeng/20220624_01_05' row format delimited fields terminated by '\t' 

select
	a.customer_no,count(distinct a.month) as months_cnt
from 
	csx_dw.report_sale_r_m_customer_level a 
	join csx_tmp.tmp_ripei_lianxu_months b on b.customer_no=a.customer_no
where
	month>='202101'
	and a.customer_large_level='A'
group by 
	a.customer_no
;


--客户履约月数
insert overwrite directory '/tmp/zhangyanpeng/20220624_01_06' row format delimited fields terminated by '\t' 
select 
	a.customer_no,
	count(distinct b.smonth) as months_cnt
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
			sdt>='20210101' and sdt<='20220626'
			and channel_code in('1','7','9')
			and business_type_code in ('1') -- 日配
		group by 
			customer_no,substr(sdt,1,6)
		)b on a.customer_no=b.customer_no
	join
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			sales_region_code,sales_region_name,province_code,province_name,city_group_code,city_group_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and channel_code in('1','7','9')
			and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
		)c on c.customer_no=a.customer_no	
group by
	a.customer_no
;

--客户AB类月数
insert overwrite directory '/tmp/zhangyanpeng/20220624_01_07' row format delimited fields terminated by '\t' 

select
	a.customer_no,count(distinct a.month) as months_cnt
from 
	csx_dw.report_sale_r_m_customer_level a 
	join csx_tmp.tmp_ripei_lianxu_months b on b.customer_no=a.customer_no
where
	month>='202101'
	and a.customer_large_level in ('A','B')
group by 
	a.customer_no
;	