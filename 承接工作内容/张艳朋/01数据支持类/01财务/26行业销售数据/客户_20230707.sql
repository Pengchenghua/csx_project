-- B+BBC新老客行业分析
drop table if exists csx_analyse_tmp.csx_analyse_tmp_b_category_sale_detail;
create table csx_analyse_tmp.csx_analyse_tmp_b_category_sale_detail
as	
select
	a.customer_code,
	-- a.credit_code,
	c.customer_name,
	c.first_category_name,
	c.second_category_name,
	c.third_category_name,
	c.performance_region_name,
	c.performance_province_name,
	c.performance_city_name,
	substr(a.sdt,1,6) as smonth,
	a.business_type_name,
	sum(a.sale_amt) sale_amt,
	sum(a.sale_amt_no_tax) as sale_amt_no_tax,
	sum(a.profit) profit,
	sum(a.profit_no_tax) as profit_no_tax,
	e.yw_first_sale_date,
	e.yw_first_sale_month,
	d.first_sale_date,		
	d.first_order_month,
	coalesce(if(a.business_type_name='日配业务',f.customer_large_level,''),'') as customer_large_level,
	c.customer_acquisition_type_name
from 
	(
	select
		sdt,customer_code,business_type_code,business_type_name,sale_amt,sale_amt_no_tax,profit,profit_no_tax,credit_code
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20210101' and sdt<='20230630' 
		and channel_code in ('1', '7', '9') and business_type_code in (1,2,3,4,5,6)
	) a 
	left join
		(
		select
			calday,quarter_of_year
		from
			csx_dim.csx_dim_basic_date
		) b on b.calday=a.sdt
	left join
		(
		select
			customer_code,customer_name,first_category_name,second_category_name,third_category_name,performance_region_name,performance_province_name,performance_city_name,
			customer_acquisition_type_name
		from 
			csx_dim.csx_dim_crm_customer_info
		where 
			sdt = 'current'
		) c on a.customer_code = c.customer_code
	left join
		(
		select 
			t1.customer_code,t1.first_sale_date,t2.quarter_of_year as first_order_quarter,substr(t1.first_sale_date,1,6) as first_order_month
		from
			(
			select
				customer_code,first_sale_date
			from
				csx_dws.csx_dws_crm_customer_active_di
			where 
				sdt='current'
			) t1
			left join
				(
				select
					calday,quarter_of_year
				from
					csx_dim.csx_dim_basic_date
				) t2 on t2.calday=t1.first_sale_date	
		) d on a.customer_code = d.customer_code
	left join 
		(
		select 
			t1.customer_code,t1.business_type_code,t1.business_type_name,t1.first_business_sale_date as yw_first_sale_date,
			t2.quarter_of_year as yw_first_order_quarter ,
			substr(t1.first_business_sale_date,1,6) as yw_first_sale_month
		from 
			(
			select
				customer_code,business_type_code,business_type_name,first_business_sale_date
			from
				csx_dws.csx_dws_crm_customer_business_active_di
			where 
				sdt='current' 
				and business_type_code in (1,2,3,4,5,6)
			group by 
				customer_code,business_type_code,business_type_name,first_business_sale_date
			) t1
			left join
				(
				select
					calday,quarter_of_year
				from
					csx_dim.csx_dim_basic_date
				) t2 on t2.calday=t1.first_business_sale_date				
		) e on a.customer_code = e.customer_code and a.business_type_code = e.business_type_code
	left join
		(
		select
			customer_no,month,customer_large_level
		from 
			csx_analyse.csx_analyse_report_sale_customer_level_mf
		where
			month>='202101' and month<='202306'
			and tag=1 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
		group by 
			customer_no,month,customer_large_level
		) f on f.customer_no=a.customer_code and f.month=substr(a.sdt,1,6)
group by 
	a.customer_code,
	-- a.credit_code,
	c.customer_name,
	c.first_category_name,
	c.second_category_name,
	c.third_category_name,
	c.performance_region_name,
	c.performance_province_name,
	c.performance_city_name,
	substr(a.sdt,1,6),
	a.business_type_name,
	e.yw_first_sale_date,
	e.yw_first_sale_month,
	d.first_sale_date,		
	d.first_order_month,
	coalesce(if(a.business_type_name='日配业务',f.customer_large_level,''),''),
	c.customer_acquisition_type_name 
;
select * from csx_analyse_tmp.csx_analyse_tmp_b_category_sale_detail;

-- 202306 首次履约时间在3-5

-- 所有客户层面：1、当月新客；2、次新客（之前三个月内的客户新客）；3、预断约（未来三个月都没有履约）；4、轮配断约（下月没有履约，后面两月有履约）5、老客

-- 日配业务单层面：2、当月日配新业务；2、次新业务单；3、预断约；4、轮配断约

-- 标识-客户整体
drop table if exists csx_analyse_tmp.csx_analyse_tmp_b_category_sale_detail_cust_flag;
create table csx_analyse_tmp.csx_analyse_tmp_b_category_sale_detail_cust_flag
as
select
	a.customer_code,a.sdt_month,a.smonth,b.first_sale_month,b.s_first_sale_month,months_between(a.sdt_month,b.first_sale_month) as diff_month,
	case when a.sdt_month=b.first_sale_month then '当月新客'
		when months_between(a.sdt_month,b.first_sale_month)<=3 then '次新客'
		when (a.next_1_month !='9999-12-31' and months_between(a.next_1_month,a.add_3_month)>=1) 
			or (a.next_1_month ='9999-12-31' and months_between('2023-09-01',a.add_3_month)>=1) then '预断约'
		when a.next_1_month =a.add_2_month or a.next_1_month =a.add_3_month then '轮配断约'
		else '老客'
	end as cust_type
from
	(
	select
		customer_code,sdt_month,smonth,
		lead(sdt_month,1,'9999-12-31')over(partition by customer_code order by sdt_month) as next_1_month,
		lead(sdt_month,2,'9999-12-31')over(partition by customer_code order by sdt_month) as next_2_month,
		lead(sdt_month,3,'9999-12-31')over(partition by customer_code order by sdt_month) as next_3_month,
		to_date(add_months(sdt_month,1)) as add_1_month,
		to_date(add_months(sdt_month,2)) as add_2_month,
		to_date(add_months(sdt_month,3)) as add_3_month,
		row_number()over(partition by customer_code order by sdt_month) as rn
	from
		(
		select
			to_date(trunc(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')),'MM')) as sdt_month,customer_code,substr(sdt,1,6) as smonth
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20210101' and sdt<='20230630' 
			and channel_code in ('1', '7', '9') 
			and business_type_code in (1,2,3,4,5,6)
		group by 
			to_date(trunc(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')),'MM')),customer_code,substr(sdt,1,6)
		) a 
	) a 
	left join
		(
		select
			customer_code,first_sale_date,
			to_date(trunc(from_unixtime(unix_timestamp(first_sale_date,'yyyyMMdd')),'MM')) as first_sale_month,
			substr(first_sale_date,1,6) as s_first_sale_month
		from
			csx_dws.csx_dws_crm_customer_active_di
		where 
			sdt='current'
		) b on b.customer_code=a.customer_code
;
-- 标识-日配业务
drop table if exists csx_analyse_tmp.csx_analyse_tmp_b_category_sale_detail_rp_flag;
create table csx_analyse_tmp.csx_analyse_tmp_b_category_sale_detail_rp_flag
as
select
	'日配业务' as business_type_name,
	a.customer_code,a.sdt_month,a.smonth,b.first_sale_month,b.s_first_sale_month,months_between(a.sdt_month,b.first_sale_month) as diff_month,
	case when a.sdt_month=b.first_sale_month then '当月日配新业务'
		when months_between(a.sdt_month,b.first_sale_month)<=3 then '次新业务单'
		when (a.next_1_month !='9999-12-31' and months_between(a.next_1_month,a.add_3_month)>=1) 
			or (a.next_1_month ='9999-12-31' and months_between('2023-09-01',a.add_3_month)>=1) then '预断约'
		when a.next_1_month =a.add_2_month or a.next_1_month =a.add_3_month then '轮配断约'
		else '老客'
	end as cust_type
from
	(
	select
		customer_code,sdt_month,smonth,
		lead(sdt_month,1,'9999-12-31')over(partition by customer_code order by sdt_month) as next_1_month,
		lead(sdt_month,2,'9999-12-31')over(partition by customer_code order by sdt_month) as next_2_month,
		lead(sdt_month,3,'9999-12-31')over(partition by customer_code order by sdt_month) as next_3_month,
		to_date(add_months(sdt_month,1)) as add_1_month,
		to_date(add_months(sdt_month,2)) as add_2_month,
		to_date(add_months(sdt_month,3)) as add_3_month,
		row_number()over(partition by customer_code order by sdt_month) as rn
	from
		(
		select
			to_date(trunc(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')),'MM')) as sdt_month,customer_code,substr(sdt,1,6) as smonth
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20210101' and sdt<='20230630' 
			and channel_code in ('1', '7', '9') 
			and business_type_code in (1)
		group by 
			to_date(trunc(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')),'MM')),customer_code,substr(sdt,1,6)
		) a 
	) a 
	left join
		(
		select
			customer_code,first_business_sale_date,
			to_date(trunc(from_unixtime(unix_timestamp(first_business_sale_date,'yyyyMMdd')),'MM')) as first_sale_month,
			substr(first_business_sale_date,1,6) as s_first_sale_month
		from
			csx_dws.csx_dws_crm_customer_business_active_di
		where 
			sdt='current' 
			and business_type_code in (1)
		) b on b.customer_code=a.customer_code	
;

select 
	a.*,b.cust_type,coalesce(c.cust_type,'') as yw_cust_type 
from 
	csx_analyse_tmp.csx_analyse_tmp_b_category_sale_detail a 
	left join csx_analyse_tmp.csx_analyse_tmp_b_category_sale_detail_cust_flag b on b.customer_code=a.customer_code and b.smonth=a.smonth
	left join csx_analyse_tmp.csx_analyse_tmp_b_category_sale_detail_rp_flag c on c.customer_code=a.customer_code and c.smonth=a.smonth and c.business_type_name=a.business_type_name
	
