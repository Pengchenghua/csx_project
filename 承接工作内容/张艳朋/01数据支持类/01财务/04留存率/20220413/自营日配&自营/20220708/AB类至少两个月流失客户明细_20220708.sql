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
			sdt>='20190101' and sdt<='20220626'
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

-- ====================================================================================================================================================
insert overwrite directory '/tmp/zhangyanpeng/20220629_02' row format delimited fields terminated by '\t' 
--判断各月新客，计算每月新客在之后各月是否有销售
select 
	c.province_name,
	a.customer_no,
	c.customer_name,
	a.new_smonth,
	a.12_months,
	c.work_no,
	c.sales_name,
	c.first_category_name,
	c.second_category_name,
	c.third_category_name,	
	b.customer_no,
	e.excluding_tax_sales,
	e.excluding_tax_profit
from
	(
	select 
		customer_no,substr(next_month,1,6) new_smonth,substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(next_month,'01'),'yyyyMMdd')),12),'-',''),1,6) as 12_months
	from
		csx_tmp.tmp_ripei_lianxu_months
	where 
		substr(next_month,1,6)>='201901'
	)a
	left join 	
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
		)b on a.customer_no=b.customer_no and a.12_months=b.smonth
	join
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			sales_region_code,sales_region_name,province_code,province_name,city_group_code,city_group_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = '20220626'
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
			month>='202101'
			and customer_large_level in ('A','B')
		group by 
			customer_no
		having
			count(distinct month)>=2
		) d on d.customer_no=a.customer_no
	left join 	
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
		)e on a.customer_no=e.customer_no and a.new_smonth=e.smonth
where
	b.customer_no is null
	and a.new_smonth between '202101' and '202106'
;

	