-- ====================================================================================================================================================
-- BBC


--2、留存率
-- 客户最小成交日期 、首单日期 首单--首日

with cur_sale_value_1 as 
(
select
	customer_no,
	min(sdt) as min_sdt,
	max(sdt) as max_sdt
from 
	csx_dw.dws_sale_r_d_detail 
where 
	sdt>='20190101' and sdt<='20210630' 
	and business_type_code='6' -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
group by 
	customer_no
)

insert overwrite directory '/tmp/zhangyanpeng/20210726_linshi_1' row format delimited fields terminated by '\t' 
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
		customer_no,substr(min_sdt,1,6) new_smonth
	from
		cur_sale_value_1
	where 
		substr(min_sdt,1,6)>='201901'
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
			and business_type_code in ('6') -- BBC
		group by 
			customer_no,substr(sdt,1,6)
		having
			sum(sales_value)>0
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



--===================================================================================================================================================================================
-- BBC 连续俩月
with cur_sale_value_2 as 
(
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
			customer_no,substr(sdt,1,6) as s_month,sum(sales_value) as sales_value
		from
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt>='20190101' and sdt<='20210630' 
			and business_type_code='6' -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			-- and customer_no in ('118148','117244')
		group by 
			customer_no,substr(sdt,1,6)
		--having
		--	sum(sales_value)>0
		) t1
	) t1
where
	rn=1
	and substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(s_month,'01'),'yyyyMMdd')),1),'-',''),1,6)=next_month
)

insert overwrite directory '/tmp/zhangyanpeng/20210726_linshi_2' row format delimited fields terminated by '\t' 
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
		customer_no,substr(next_month,1,6) new_smonth
	from
		cur_sale_value_2
	where 
		substr(next_month,1,6)>='201901'
		-- and substr(min_sdt,1,6)<substr(max_sdt,1,6)  --至少销售跨两月的客户
		-- and count_day>1  --销售天数大于1
	)a
	join -- 每月日配业务履约金额大于0	
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
				and business_type_code='6'
			group by 
				customer_no,substr(sdt,1,6),business_type_name
			having
				sum(sales_value)>0
			) t1
		group by 
			customer_no,smonth
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

