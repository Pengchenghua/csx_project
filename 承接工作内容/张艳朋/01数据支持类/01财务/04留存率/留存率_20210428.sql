-- ====================================================================================================================================================
-- 基础数据

insert overwrite directory '/tmp/zhangyanpeng/20210428_linshi_1' row format delimited fields terminated by '\t' 

select	
	c.region_name,
	c.province_name,
	a.business_type_name,
	a.customer_no,
	b.customer_name,
	b.second_category_name,
	a.s_sdt,
	substr(e.first_order_date,1,6) as first_order_date,
	substr(d.min_sdt,1,6) as min_sdt,
	b.cooperation_mode_name,
	a.sales_value,
	a.profit,
	a.profit_rate
from
	(
	select
		substr(sdt,1,6) as s_sdt,region_code,province_code,customer_no,business_type_name,
		sum(sales_value) as sales_value,
		sum(profit) as profit,
		sum(profit)/abs(sum(sales_value)) as profit_rate
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt >= '20200101' 
		and sdt <= '20210426'
		and channel_code in ('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and business_type_code not in ('7','8','9') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
	group by 
		substr(sdt,1,6),region_code,province_code,customer_no,business_type_name
	) as a
	left join 
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			cooperation_mode_name
			-- regexp_replace(split_part(sign_time,' ',1),'-','') as sign_date
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt='current'
		group by 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			cooperation_mode_name
			-- regexp_replace(split_part(sign_time,' ',1),'-','')
		) b on a.customer_no=b.customer_no
	left join 
		(
		select 
			province_code,province_name,region_code,region_name
		from 
			csx_dw.dws_sale_w_a_area_belong
		group by 
			province_code,province_name,region_code,region_name
		) c on c.province_code=a.province_code	
	left join 
		(
		select 
			customer_no,
			business_type_name,
			min(sdt) as min_sdt
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20190101'
			and sdt<='20210426'
			and channel_code in ('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
			and business_type_code not in ('7','8','9') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		group by 
			customer_no,business_type_name
		) d on d.customer_no=a.customer_no and d.business_type_name=a.business_type_name
	left join 
		(
		select 
			customer_no,customer_name,first_order_date
		from 
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt='current'
		group by 
			customer_no,customer_name,first_order_date
		) e on e.customer_no=a.customer_no	
		
		
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
	max(sdt) as max_sdt
from 
	csx_dw.dws_sale_r_d_detail 
where 
	sdt>='20190101' and sdt<='20210426' 
	and business_type_code='1'
group by 
	customer_no
)

--结果表  
--判断各月新客，计算每月新客在之后各月是否有销售
select 
	c.sales_province_name,
	a.new_smonth,
	--b.smonth,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01'))) diff_month,
	count(distinct b.customer_no) counts,
	sum(b.sales_value) sales_value,
	avg(b.sales_value) avg_sales_value
from
	(
	select 
		customer_no,substr(min_sdt,1,6) new_smonth
	from
		cur_sale_value
	where 
		substr(min_sdt,1,6)>='202001'
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
			sdt>='20200101' and sdt<='20210426'
			and channel_code in('1','7','9')
			and business_type_code<>'4' -- 自营业务的收入
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
	--b.smonth
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),
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
	customer_no,
	min(sdt) as min_sdt,
	max(sdt) as max_sdt,
	count(distinct sdt) as count_day
from 
	csx_dw.dws_sale_r_d_detail 
where 
	sdt>='20190101' and sdt<='20210426'
	and channel_code in ('1','7','9')
	-- and business_type_code='1'
group by 
	customer_no
)

--结果表  
--判断各月新客，计算每月新客在之后各月是否有销售
select
	c.sales_province_name, 
	a.new_smonth,
	--b.smonth,
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01'))) diff_month,
	count(distinct b.customer_no) counts,
	sum(b.sales_value) sales_value,
	avg(b.sales_value) avg_sales_value
from
	(
	select 
		customer_no,substr(min_sdt,1,6) new_smonth
	from
		cur_sale_value_2
	where 
		substr(min_sdt,1,6)>='202001'
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
			sdt>='20200101' and sdt<='20210426'
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
	--b.smonth
	floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),
	concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01')))
;		


	