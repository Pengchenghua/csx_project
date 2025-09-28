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
	where
		a.month>='202101' and a.month<='202206'
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
	month>='202101' and a.month<='202206'
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
			sdt>='20210101' and sdt<='20220630'
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
	month>='202101' and a.month<='202206'
	and a.customer_large_level in ('A','B')
group by 
	a.customer_no
;	


--================================================================================================================================================================================
select
	customer_no,substr(sdt,1,6) smonth,
	sum(sales_value) as sales_value,
	sum(excluding_tax_sales) excluding_tax_sales,
	sum(profit) as profit
	--sum(excluding_tax_profit) as excluding_tax_profit
from
	(
	select
		customer_no,sdt,substr(sdt,1,6) smonth,business_type_name,sales_value,excluding_tax_sales,profit,excluding_tax_profit
	from
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20210101' and sdt<='20220630'
		and channel_code in('1','7','9')
		and business_type_code in ('1') -- 日配
	) a 
	left join
		(
		select 
			calday,quarter_of_year,concat(substr(quarter_of_year,1,4),'Q',substr(quarter_of_year,5,1)) as quarter 
		from 
			csx_dw.dws_basic_w_a_date
		) b on b.calday=a.sdt
	left join
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			sales_region_code,sales_region_name,province_code,province_name,city_group_code,city_group_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			--and channel_code in('1','7','9')
			--and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
		) c on c.customer_no=a.customer_no
	left join
		(
		select distinct 
			customer_no,customer_large_level,customer_small_level,month
		from 
			csx_dw.report_sale_r_m_customer_level 
		where 
			1=1
		) d on d.customer_no=a.customer_no and d.month=a.smonth
	left join
		(
		select
			customer_no,normal_last_order_date
		from 
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt = 'current' 
		) e
			
		
group by 
	customer_no,substr(sdt,1,6)	
	
select 
	calday,quarter_of_year,concat(substr(quarter_of_year,1,4),'Q',substr(quarter_of_year,5,1)) as quarter 
from 
	csx_dw.dws_basic_w_a_date;
	
--================================================================================================================================================================================	
		select
			customer_no,first_order_date,
			case when substr(first_order_date,1,4) <='2019' then '19年及之前新客'
				when substr(first_order_date,1,4)='2020' then '20年新客'
				when substr(first_order_date,1,4)='2021' then '21年新客'
				when substr(first_order_date,1,4)='2022' then '22年新客'
				else '其他'
			end as customer_type
		from 
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt = 'current' 
			and first_order_date <='20220630'
--================================================================================================================================================================================			
--计算客户最早下单日期
drop table if exists csx_tmp.tmp_customer_first_order_date_20220711;
create table csx_tmp.tmp_customer_first_order_date_20220711	
as
select
	b.province_name,
	a.customer_no,
	a.b_bbc_first_order_date,
	case when substr(b_bbc_first_order_date,1,4) <='2019' then '19年及之前新客'
		when substr(b_bbc_first_order_date,1,4)='2020' then '20年新客'
		when substr(b_bbc_first_order_date,1,4)='2021' then '21年新客'
		when substr(b_bbc_first_order_date,1,4)='2022' then '22年新客'
		else '其他'
	end as b_bbc_customer_type,
	a.rp_first_order_date,
	case when substr(rp_first_order_date,1,4) <='2019' then '19年及之前新客'
		when substr(rp_first_order_date,1,4)='2020' then '20年新客'
		when substr(rp_first_order_date,1,4)='2021' then '21年新客'
		when substr(rp_first_order_date,1,4)='2022' then '22年新客'
		else '其他'
	end as rp_customer_type
from	
	(
	select
		customer_no,
		min(sdt) as b_bbc_first_order_date,
		min(case when business_type_code in ('1') then sdt end) as rp_first_order_date
	from
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20190101' and sdt<='20220630'
		and channel_code in('1','7','9')
		and sales_type <>'fanli'
	group by 
		customer_no
	) a 
	left join
		(
		select
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			sales_region_code,sales_region_name,province_code,province_name,city_group_code,city_group_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = '20220630'
		) b on b.customer_no=a.customer_no
;
--================================================================================================================================================================================
--b+bbc
select
	b.province_name,
	b.b_bbc_customer_type,
	a.syear,
	count(distinct case when b.b_bbc_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is null and f.customer_no is null then a.customer_no end) as `20年履约且22年、21年未履约的20年前新客户数`,
	count(distinct case when b.b_bbc_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is null then a.customer_no end) as `21年履约且22年未履约的20年前新客户数`,
	count(distinct case when b.b_bbc_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is not null then a.customer_no end) as `22年履约的20年前新客户数`,
	sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is null and f.customer_no is null then a.sales_value end) as `20年履约且22年、21年未履约的20年前新客户含税收入`,
	sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is null then a.sales_value end) as `21年履约且22年未履约的20年前新客户含税收入`,
	sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is not null then a.sales_value end) as `22年履约的20年前新客户含税收入`,

	sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is null and f.customer_no is null then a.excluding_tax_profit end)/
	abs(sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is null and f.customer_no is null then a.excluding_tax_sales end)) as `20年履约且22年、21年未履约的20年前新客户不含税毛利率`,
	
	sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is null then a.excluding_tax_profit end)/
	abs(sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is null then a.excluding_tax_sales end)) as `21年履约且22年未履约的20年前新客户不含税毛利率`,
	
	sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is not null then a.excluding_tax_profit end)/
	abs(sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is not null then a.excluding_tax_sales end)) as `22年履约的20年前新客户不含税毛利率`,
	
	sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is null and f.customer_no is null then a.excluding_tax_profit end) as `20年履约且22年、21年未履约的20年前新客户不含税毛利额`,
	sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is null and f.customer_no is null then a.excluding_tax_sales end) as `20年履约且22年、21年未履约的20年前新客户不含税销售额`,
	
	sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is null then a.excluding_tax_profit end)as `21年履约且22年未履约的20年前新客户不含税毛利额`,
	sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is null then a.excluding_tax_sales end) as `21年履约且22年未履约的20年前新客户不含税销售额`,
	
	sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is not null then a.excluding_tax_profit end)as `22年履约的20年前新客户不含税毛利额`,
	sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is not null then a.excluding_tax_sales end) as `22年履约的20年前新客户不含税销售额`,

	--20年新客
	count(distinct case when b.b_bbc_customer_type='20年新客' and d.customer_no is not null and e.customer_no is null and f.customer_no is null then a.customer_no end) as `21、22均未履约的20年新客户`,
	count(distinct case when b.b_bbc_customer_type='20年新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is null then a.customer_no end) as `21年履约且22年未履约的20年新客户`,
	count(distinct case when b.b_bbc_customer_type='20年新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is not null then a.customer_no end) as `22年履约的20年新客户`,
	sum(case when b.b_bbc_customer_type='20年新客' and d.customer_no is not null and e.customer_no is null and f.customer_no is null then a.sales_value end) as `21、22均未履约的20年新客户含税收入`,
	sum(case when b.b_bbc_customer_type='20年新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is null then a.sales_value end) as `21年履约且22年未履约的20年新客户含税收入`,
	sum(case when b.b_bbc_customer_type='20年新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is not null then a.sales_value end) as `22年履约的20年新客户含税收入`,

	sum(case when b.b_bbc_customer_type='20年新客' and d.customer_no is not null and e.customer_no is null and f.customer_no is null then a.excluding_tax_profit end)/
	abs(sum(case when b.b_bbc_customer_type='20年新客' and d.customer_no is not null and e.customer_no is null and f.customer_no is null then a.excluding_tax_sales end)) as `21、22均未履约的20年新客户不含税毛利率`,
	
	sum(case when b.b_bbc_customer_type='20年新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is null then a.excluding_tax_profit end)/
	abs(sum(case when b.b_bbc_customer_type='20年新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is null then a.excluding_tax_sales end)) as `21年履约且22年未履约的20年新客户不含税毛利率`,
	
	sum(case when b.b_bbc_customer_type='20年新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is not null then a.excluding_tax_profit end)/
	abs(sum(case when b.b_bbc_customer_type='20年新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is not null then a.excluding_tax_sales end)) as `22年履约的20年新客户不含税毛利率`,	


	sum(case when b.b_bbc_customer_type='20年新客' and d.customer_no is not null and e.customer_no is null and f.customer_no is null then a.excluding_tax_profit end) as `21、22均未履约的20年新客户不含税毛利额`,
	sum(case when b.b_bbc_customer_type='20年新客' and d.customer_no is not null and e.customer_no is null and f.customer_no is null then a.excluding_tax_sales end) as `21、22均未履约的20年新客户不含税销售额`,
	
	sum(case when b.b_bbc_customer_type='20年新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is null then a.excluding_tax_profit end) as `21年履约且22年未履约的20年新客户不含税毛利额`,
	sum(case when b.b_bbc_customer_type='20年新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is null then a.excluding_tax_sales end) as `21年履约且22年未履约的20年新客户不含税销售额`,
	
	sum(case when b.b_bbc_customer_type='20年新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is not null then a.excluding_tax_profit end) as `22年履约的20年新客户不含税毛利额`,
	sum(case when b.b_bbc_customer_type='20年新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is not null then a.excluding_tax_sales end) as `22年履约的20年新客户不含税销售额`,
	--21年新客
	count(distinct case when b.b_bbc_customer_type='21年新客' and e.customer_no is not null and f.customer_no is null then a.customer_no end) as `21年履约且22年未履约的21年新客户`,
	count(distinct case when b.b_bbc_customer_type='21年新客' and e.customer_no is not null and f.customer_no is not null then a.customer_no end) as `22年履约的21年新客户`,
	sum(case when b.b_bbc_customer_type='21年新客' and e.customer_no is not null and f.customer_no is null then a.sales_value end) as `21年履约且22年未履约的21年新客户含税收入`,
	sum(case when b.b_bbc_customer_type='21年新客' and e.customer_no is not null and f.customer_no is not null then a.sales_value end) as `22年履约的21年新客户含税收入`,

	sum(case when b.b_bbc_customer_type='21年新客' and e.customer_no is not null and f.customer_no is null then a.excluding_tax_profit end)/
	abs(sum(case when b.b_bbc_customer_type='21年新客' and e.customer_no is not null and f.customer_no is null then a.excluding_tax_sales end)) as `21年履约且22年未履约的21年新客户不含税毛利率`,
	
	sum(case when b.b_bbc_customer_type='21年新客' and e.customer_no is not null and f.customer_no is not null then a.excluding_tax_profit end)/
	abs(sum(case when b.b_bbc_customer_type='21年新客' and e.customer_no is not null and f.customer_no is not null then a.excluding_tax_sales end)) as `22年履约的21年新客户不含税毛利率`,


	sum(case when b.b_bbc_customer_type='21年新客' and e.customer_no is not null and f.customer_no is null then a.excluding_tax_profit end) as `21年履约且22年未履约的21年新客户不含税毛利额`,
	sum(case when b.b_bbc_customer_type='21年新客' and e.customer_no is not null and f.customer_no is null then a.excluding_tax_sales end) as `21年履约且22年未履约的21年新客户不含税销售额`,
	
	sum(case when b.b_bbc_customer_type='21年新客' and e.customer_no is not null and f.customer_no is not null then a.excluding_tax_profit end) as `22年履约的21年新客户不含税毛利额`,	
	sum(case when b.b_bbc_customer_type='21年新客' and e.customer_no is not null and f.customer_no is not null then a.excluding_tax_sales end) as `22年履约的21年新客户不含税销售额`,	
	--22年新客
	count(distinct case when b.b_bbc_customer_type='22年新客' and f.customer_no is not null then a.customer_no end) as `22年履约的22年新客户`,
	sum(case when b.b_bbc_customer_type='22年新客' and f.customer_no is not null then a.sales_value end) as `22年履约的22年新客户含税收入`,

	sum(case when b.b_bbc_customer_type='22年新客' and f.customer_no is not null then a.excluding_tax_profit end)/
	abs(sum(case when b.b_bbc_customer_type='22年新客' and f.customer_no is not null then a.excluding_tax_sales end)) as `22年履约的22年新客户不含税毛利率`,

	sum(case when b.b_bbc_customer_type='22年新客' and f.customer_no is not null then a.excluding_tax_profit end) as `22年履约的22年新客户不含税毛利额`,
	sum(case when b.b_bbc_customer_type='22年新客' and f.customer_no is not null then a.excluding_tax_sales end) as `22年履约的22年新客户不含税销售额`
	
	
from
	(--履约
	select
		customer_no,substr(sdt,1,4) as syear,
		sum(sales_value) as sales_value,
		sum(excluding_tax_sales) as excluding_tax_sales,
		sum(excluding_tax_profit) as excluding_tax_profit
	from
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20200101' and sdt<='20220630'
		and channel_code in('1','7','9')
	group by 
		customer_no,substr(sdt,1,4)
	) a
	join
		(
		select
			province_name,customer_no,b_bbc_first_order_date,b_bbc_customer_type
		from
			csx_tmp.tmp_customer_first_order_date_20220711
		where
			1=1
			--b_bbc_customer_type='19年及之前新客'
		) b on b.customer_no=a.customer_no
	left join --2020年履约
		(
		select
			customer_no,substr(sdt,1,4) as syear,
			sum(sales_value) as sales_value,
			sum(excluding_tax_sales) as excluding_tax_sales,
			sum(excluding_tax_profit) as excluding_tax_profit
		from
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20200101' and sdt<='20201231'
			and channel_code in('1','7','9')
		group by 
			customer_no,substr(sdt,1,4)
		) d on d.customer_no=a.customer_no
	left join --2021年履约
		(
		select
			customer_no,substr(sdt,1,4) as syear,
			sum(sales_value) as sales_value,
			sum(excluding_tax_sales) as excluding_tax_sales,
			sum(excluding_tax_profit) as excluding_tax_profit
		from
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20210101' and sdt<='20211231'
			and channel_code in('1','7','9')
		group by 
			customer_no,substr(sdt,1,4)
		) e on e.customer_no=a.customer_no
	left join --2022年履约
		(
		select
			customer_no,substr(sdt,1,4) as syear,
			sum(sales_value) as sales_value,
			sum(excluding_tax_sales) as excluding_tax_sales,
			sum(excluding_tax_profit) as excluding_tax_profit
		from
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20220101' and sdt<='20220630'
			and channel_code in('1','7','9')
		group by 
			customer_no,substr(sdt,1,4)
		) f on f.customer_no=a.customer_no
group by 
	b.province_name,b.b_bbc_customer_type,a.syear
;
		
--================================================================================================================================================================================
--日配
select
	b.province_name,
	b.rp_customer_type,
	a.syear,
	count(distinct case when b.rp_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is null and f.customer_no is null then a.customer_no end) as `20年履约且22年、21年未履约的20年前新客户数`,
	count(distinct case when b.rp_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is null then a.customer_no end) as `21年履约且22年未履约的20年前新客户数`,
	count(distinct case when b.rp_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is not null then a.customer_no end) as `22年履约的20年前新客户数`,
	sum(case when b.rp_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is null and f.customer_no is null then a.sales_value end) as `20年履约且22年、21年未履约的20年前新客户含税收入`,
	sum(case when b.rp_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is null then a.sales_value end) as `21年履约且22年未履约的20年前新客户含税收入`,
	sum(case when b.rp_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is not null then a.sales_value end) as `22年履约的20年前新客户含税收入`,

	sum(case when b.rp_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is null and f.customer_no is null then a.excluding_tax_profit end)/
	abs(sum(case when b.rp_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is null and f.customer_no is null then a.excluding_tax_sales end)) as `20年履约且22年、21年未履约的20年前新客户不含税毛利率`,
	
	sum(case when b.rp_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is null then a.excluding_tax_profit end)/
	abs(sum(case when b.rp_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is null then a.excluding_tax_sales end)) as `21年履约且22年未履约的20年前新客户不含税毛利率`,
	
	sum(case when b.rp_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is not null then a.excluding_tax_profit end)/
	abs(sum(case when b.rp_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is not null then a.excluding_tax_sales end)) as `22年履约的20年前新客户不含税毛利率`,
	

	sum(case when b.rp_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is null and f.customer_no is null then a.excluding_tax_profit end) as `20年履约且22年、21年未履约的20年前新客户不含税毛利额`,
	sum(case when b.rp_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is null and f.customer_no is null then a.excluding_tax_sales end) as `20年履约且22年、21年未履约的20年前新客户不含税销售额`,
	
	sum(case when b.rp_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is null then a.excluding_tax_profit end) as `21年履约且22年未履约的20年前新客户不含税毛利额`,
	sum(case when b.rp_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is null then a.excluding_tax_sales end) as `21年履约且22年未履约的20年前新客户不含税销售额`,
	
	sum(case when b.rp_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is not null then a.excluding_tax_profit end) as `22年履约的20年前新客户不含税毛利额`,
	sum(case when b.rp_customer_type='19年及之前新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is not null then a.excluding_tax_sales end) as `22年履约的20年前新客户不含税销售额`,

	--20年新客
	count(distinct case when b.rp_customer_type='20年新客' and d.customer_no is not null and e.customer_no is null and f.customer_no is null then a.customer_no end) as `21、22均未履约的20年新客户`,
	count(distinct case when b.rp_customer_type='20年新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is null then a.customer_no end) as `21年履约且22年未履约的20年新客户`,
	count(distinct case when b.rp_customer_type='20年新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is not null then a.customer_no end) as `22年履约的20年新客户`,
	sum(case when b.rp_customer_type='20年新客' and d.customer_no is not null and e.customer_no is null and f.customer_no is null then a.sales_value end) as `21、22均未履约的20年新客户含税收入`,
	sum(case when b.rp_customer_type='20年新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is null then a.sales_value end) as `21年履约且22年未履约的20年新客户含税收入`,
	sum(case when b.rp_customer_type='20年新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is not null then a.sales_value end) as `22年履约的20年新客户含税收入`,

	sum(case when b.rp_customer_type='20年新客' and d.customer_no is not null and e.customer_no is null and f.customer_no is null then a.excluding_tax_profit end)/
	abs(sum(case when b.rp_customer_type='20年新客' and d.customer_no is not null and e.customer_no is null and f.customer_no is null then a.excluding_tax_sales end)) as `21、22均未履约的20年新客户不含税毛利率`,
	
	sum(case when b.rp_customer_type='20年新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is null then a.excluding_tax_profit end)/
	abs(sum(case when b.rp_customer_type='20年新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is null then a.excluding_tax_sales end)) as `21年履约且22年未履约的20年新客户不含税毛利率`,
	
	sum(case when b.rp_customer_type='20年新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is not null then a.excluding_tax_profit end)/
	abs(sum(case when b.rp_customer_type='20年新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is not null then a.excluding_tax_sales end)) as `22年履约的20年新客户不含税毛利率`,	

	
	sum(case when b.rp_customer_type='20年新客' and d.customer_no is not null and e.customer_no is null and f.customer_no is null then a.excluding_tax_profit end) as `21、22均未履约的20年新客户不含税毛利额`,
	sum(case when b.rp_customer_type='20年新客' and d.customer_no is not null and e.customer_no is null and f.customer_no is null then a.excluding_tax_sales end) as `21、22均未履约的20年新客户不含税销售额`,
	
	sum(case when b.rp_customer_type='20年新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is null then a.excluding_tax_profit end) as `21年履约且22年未履约的20年新客户不含税毛利额`,
	sum(case when b.rp_customer_type='20年新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is null then a.excluding_tax_sales end) as `21年履约且22年未履约的20年新客户不含税销售额`,
	
	sum(case when b.rp_customer_type='20年新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is not null then a.excluding_tax_profit end) as `22年履约的20年新客户不含税毛利额`,	
	sum(case when b.rp_customer_type='20年新客' and d.customer_no is not null and e.customer_no is not null and f.customer_no is not null then a.excluding_tax_sales end) as `22年履约的20年新客户不含税销售额`,	
	--21年新客
	count(distinct case when b.rp_customer_type='21年新客' and e.customer_no is not null and f.customer_no is null then a.customer_no end) as `21年履约且22年未履约的21年新客户`,
	count(distinct case when b.rp_customer_type='21年新客' and e.customer_no is not null and f.customer_no is not null then a.customer_no end) as `22年履约的21年新客户`,
	sum(case when b.rp_customer_type='21年新客' and e.customer_no is not null and f.customer_no is null then a.sales_value end) as `21年履约且22年未履约的21年新客户含税收入`,
	sum(case when b.rp_customer_type='21年新客' and e.customer_no is not null and f.customer_no is not null then a.sales_value end) as `22年履约的21年新客户含税收入`,

	sum(case when b.rp_customer_type='21年新客' and e.customer_no is not null and f.customer_no is null then a.excluding_tax_profit end)/
	abs(sum(case when b.rp_customer_type='21年新客' and e.customer_no is not null and f.customer_no is null then a.excluding_tax_sales end)) as `21年履约且22年未履约的21年新客户不含税毛利率`,
	
	sum(case when b.rp_customer_type='21年新客' and e.customer_no is not null and f.customer_no is not null then a.excluding_tax_profit end)/
	abs(sum(case when b.rp_customer_type='21年新客' and e.customer_no is not null and f.customer_no is not null then a.excluding_tax_sales end)) as `22年履约的21年新客户不含税毛利率`,


	sum(case when b.rp_customer_type='21年新客' and e.customer_no is not null and f.customer_no is null then a.excluding_tax_profit end) as `21年履约且22年未履约的21年新客户不含税毛利额`,
	sum(case when b.rp_customer_type='21年新客' and e.customer_no is not null and f.customer_no is null then a.excluding_tax_sales end) as `21年履约且22年未履约的21年新客户不含税销售额`,
	
	sum(case when b.rp_customer_type='21年新客' and e.customer_no is not null and f.customer_no is not null then a.excluding_tax_profit end) as `22年履约的21年新客户不含税毛利额`,	
	sum(case when b.rp_customer_type='21年新客' and e.customer_no is not null and f.customer_no is not null then a.excluding_tax_sales end) as `22年履约的21年新客户不含税销售额`,	
	--22年新客
	count(distinct case when b.rp_customer_type='22年新客' and f.customer_no is not null then a.customer_no end) as `22年履约的22年新客户`,
	sum(case when b.rp_customer_type='22年新客' and f.customer_no is not null then a.sales_value end) as `22年履约的22年新客户含税收入`,

	sum(case when b.rp_customer_type='22年新客' and f.customer_no is not null then a.excluding_tax_profit end)/
	abs(sum(case when b.rp_customer_type='22年新客' and f.customer_no is not null then a.excluding_tax_sales end)) as `22年履约的22年新客户不含税毛利率`,
	sum(case when b.rp_customer_type='22年新客' and f.customer_no is not null then a.excluding_tax_profit end)as `22年履约的22年新客户不含税毛利额`,
	sum(case when b.rp_customer_type='22年新客' and f.customer_no is not null then a.excluding_tax_sales end) as `22年履约的22年新客户不含税销售额`
	
from
	(--履约
	select
		customer_no,substr(sdt,1,4) as syear,
		sum(sales_value) as sales_value,
		sum(excluding_tax_sales) as excluding_tax_sales,
		sum(excluding_tax_profit) as excluding_tax_profit
	from
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20200101' and sdt<='20220630'
		and channel_code in('1','7','9')
		and business_type_code='1'
		and sales_type<>'fanli'
	group by 
		customer_no,substr(sdt,1,4)
	) a
	join
		(
		select
			province_name,customer_no,rp_first_order_date,rp_customer_type
		from
			csx_tmp.tmp_customer_first_order_date_20220711
		where
			1=1
			and rp_customer_type<>'其他'
		) b on b.customer_no=a.customer_no
	left join --2020年履约
		(
		select
			customer_no,substr(sdt,1,4) as syear,
			sum(sales_value) as sales_value,
			sum(excluding_tax_sales) as excluding_tax_sales,
			sum(excluding_tax_profit) as excluding_tax_profit
		from
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20200101' and sdt<='20201231'
			and channel_code in('1','7','9')
			and business_type_code='1'
			and sales_type<>'fanli'
		group by 
			customer_no,substr(sdt,1,4)
		) d on d.customer_no=a.customer_no
	left join --2021年履约
		(
		select
			customer_no,substr(sdt,1,4) as syear,
			sum(sales_value) as sales_value,
			sum(excluding_tax_sales) as excluding_tax_sales,
			sum(excluding_tax_profit) as excluding_tax_profit
		from
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20210101' and sdt<='20211231'
			and channel_code in('1','7','9')
			and business_type_code='1'
			and sales_type<>'fanli'
		group by 
			customer_no,substr(sdt,1,4)
		) e on e.customer_no=a.customer_no
	left join --2022年履约
		(
		select
			customer_no,substr(sdt,1,4) as syear,
			sum(sales_value) as sales_value,
			sum(excluding_tax_sales) as excluding_tax_sales,
			sum(excluding_tax_profit) as excluding_tax_profit
		from
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20220101' and sdt<='20220630'
			and channel_code in('1','7','9')
			and business_type_code='1'
			and sales_type<>'fanli'
		group by 
			customer_no,substr(sdt,1,4)
		) f on f.customer_no=a.customer_no
group by 
	b.province_name,b.rp_customer_type,a.syear
;