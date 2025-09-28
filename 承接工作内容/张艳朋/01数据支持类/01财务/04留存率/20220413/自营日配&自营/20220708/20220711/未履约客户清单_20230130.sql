-- ====================================================================================================================================================
--连续两个月履约日配业务
drop table if exists csx_analyse_tmp.csx_analyse_tmp_ripei_lianxu_months;
create table csx_analyse_tmp.csx_analyse_tmp_ripei_lianxu_months
as
select
	customer_code,next_month
from
	(
	select
		customer_code,
		s_month,
		lead(s_month,1,0) over (partition by customer_code order by s_month) as next_month,
		row_number() over(partition by customer_code order by s_month) as rn
	from
		(
		select
			customer_code,substr(sdt,1,6) as s_month,sum(sale_amt_no_tax) as sale_amt_no_tax
		from
			-- csx_dw.dws_sale_r_d_detail 
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20190101' and sdt<='20221231'
			and channel_code in('1','7','9')			
			and business_type_code=1
		group by 
			customer_code,substr(sdt,1,6)
		) t1
	) t1
where
	rn=1
	and substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(s_month,'01'),'yyyyMMdd')),1),'-',''),1,6)=next_month
group by 
	customer_code,next_month
;

--=============================================================================================================================================================================
-- 至少AB类有两个月的客户_21年新客在22年Q4都没履约的客户
select 
	c.performance_province_name,
	a.customer_code,
	c.customer_name,
	a.new_smonth,
	c.sales_user_number,
	c.sales_user_name,
	c.first_category_name,
	c.second_category_name,
	c.third_category_name
from
	(
	select 
		customer_code,substr(next_month,1,6) new_smonth
	from
		csx_analyse_tmp.csx_analyse_tmp_ripei_lianxu_months
	where 
		substr(next_month,1,6) between '202101' and '202112'
	)a
	left join
		(
		select 
			customer_code
		from 
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20221001' and sdt<='20221231'
			and channel_code in('1','7','9')
		group by 
			customer_code
		)b on a.customer_code=b.customer_code
	join
		(
		select 
			customer_code,customer_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			performance_province_name,sales_user_number,sales_user_name
		from 
			-- csx_dw.dws_crm_w_a_customer
			csx_dim.csx_dim_crm_customer_info
		where 
			sdt = 'current'
			and channel_code in('1','7','9')
			and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
		)c on c.customer_code=a.customer_code	
	 join
	 	( --至少出现过两个月AB类的客户
	 	select
	 		customer_no,count(distinct month) as months_cnt
	 	from 
	 		-- csx_dw.report_sale_r_m_customer_level
	 		csx_analyse.csx_analyse_report_sale_customer_level_mf
	 	where
	 		month>='201901' and month<='202212'
	 		and customer_large_level in ('A','B')
	 		and tag=1 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
	 	group by 
	 		customer_no
	 	having
	 		count(distinct month)>=2
	 	) d on d.customer_no=a.customer_code
where
	b.customer_code is null
;

--=============================================================================================================================================================================
-- 至少AB类有两个月的客户_22年新客在22年Q4都没履约的客户
select 
	c.performance_province_name,
	a.customer_code,
	c.customer_name,
	a.new_smonth,
	c.sales_user_number,
	c.sales_user_name,
	c.first_category_name,
	c.second_category_name,
	c.third_category_name
from
	(
	select 
		customer_code,substr(next_month,1,6) new_smonth
	from
		csx_analyse_tmp.csx_analyse_tmp_ripei_lianxu_months
	where 
		substr(next_month,1,6) between '202201' and '202212'
	)a
	left join
		(
		select 
			customer_code
		from 
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20221001' and sdt<='20221231'
			and channel_code in('1','7','9')
		group by 
			customer_code
		)b on a.customer_code=b.customer_code
	join
		(
		select 
			customer_code,customer_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			performance_province_name,sales_user_number,sales_user_name
		from 
			-- csx_dw.dws_crm_w_a_customer
			csx_dim.csx_dim_crm_customer_info
		where 
			sdt = 'current'
			and channel_code in('1','7','9')
			and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
		)c on c.customer_code=a.customer_code	
	 join
	 	( --至少出现过两个月AB类的客户
	 	select
	 		customer_no,count(distinct month) as months_cnt
	 	from 
	 		-- csx_dw.report_sale_r_m_customer_level
	 		csx_analyse.csx_analyse_report_sale_customer_level_mf
	 	where
	 		month>='201901' and month<='202212'
	 		and customer_large_level in ('A','B')
	 		and tag=1 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
	 	group by 
	 		customer_no
	 	having
	 		count(distinct month)>=2
	 	) d on d.customer_no=a.customer_code
where
	b.customer_code is null
;
	