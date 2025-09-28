-- B+BBC新老客行业分析
drop table if exists csx_analyse_tmp.csx_analyse_tmp_b_category_sale_detail;
create table csx_analyse_tmp.csx_analyse_tmp_b_category_sale_detail
as	
select
	a.customer_code,
	c.customer_name,
	c.first_category_name,
	c.second_category_name,
	c.third_category_name,
	c.performance_region_name,
	c.performance_province_name,
	c.performance_city_name,
	b.quarter_of_year,
	a.business_type_name,
	sum(a.sale_amt) sale_amt,
	sum(a.sale_amt_no_tax) as sale_amt_no_tax,
	sum(a.profit) profit,
	sum(a.profit_no_tax) as profit_no_tax,
	e.yw_first_sale_date,
	(case when e.yw_first_sale_date<'20210101' then '20年及之前' else e.yw_first_order_quarter end) as yw_first_order_quarter,
	d.first_sale_date,		
	(case when d.first_sale_date<'20210101' then '20年及之前' else d.first_order_quarter end) as first_order_quarter 
from 
	(
	select
		sdt,customer_code,business_type_code,business_type_name,sale_amt,sale_amt_no_tax,profit,profit_no_tax
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20210101' and sdt<='20230930' 
		and channel_code in ('1', '7', '9') and business_type_code in ('1','2','3','4','5','6')
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
			customer_code,customer_name,first_category_name,second_category_name,third_category_name,performance_region_name,performance_province_name,performance_city_name
		from 
			csx_dim.csx_dim_crm_customer_info
		where 
			sdt = 'current'
		) c on a.customer_code = c.customer_code
	left join
		(
		select 
			t1.customer_code,t1.first_sale_date,t2.quarter_of_year as first_order_quarter  
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
			t2.quarter_of_year as yw_first_order_quarter  
		from 
			(
			select
				customer_code,business_type_code,business_type_name,first_business_sale_date
			from
				csx_dws.csx_dws_crm_customer_business_active_di
			where 
				sdt='current' 
				and business_type_code in ('1','2','3','4','5','6')
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
group by 
	a.customer_code,
	c.customer_name,
	c.first_category_name,
	c.second_category_name,
	c.third_category_name,
	c.performance_region_name,
	c.performance_province_name,
	c.performance_city_name,
	b.quarter_of_year,
	a.business_type_name,
	e.yw_first_sale_date,
	(case when e.yw_first_sale_date<'20210101' then '20年及之前' else e.yw_first_order_quarter end),
	d.first_sale_date,		
	(case when d.first_sale_date<'20210101' then '20年及之前' else d.first_order_quarter end)  
;
select * from csx_analyse_tmp.csx_analyse_tmp_b_category_sale_detail;

-- 日配ABCD类分析
drop table if exists csx_analyse_tmp.csx_analyse_tmp_rp_category_sale_detail;
create table csx_analyse_tmp.csx_analyse_tmp_rp_category_sale_detail
as	
select
	a.customer_code,
	c.customer_name,
	c.first_category_name,
	c.second_category_name,
	c.third_category_name,
	c.performance_region_name,
	c.performance_province_name,
	c.performance_city_name,
	b.quarter_of_year,
	a.business_type_name,
    f.customer_large_level,
	f.customer_small_level,
	f.customer_level_tag,
	sum(a.sale_amt) sale_amt,
	sum(a.sale_amt_no_tax) as sale_amt_no_tax,
	sum(a.profit) profit,
	sum(a.profit_no_tax) as profit_no_tax,
	e.yw_first_sale_date,
	(case when e.yw_first_sale_date<'20210101' then '20年及之前' else e.yw_first_order_quarter end) as yw_first_order_quarter,
	d.first_sale_date,		
	(case when d.first_sale_date<'20210101' then '20年及之前' else d.first_order_quarter end) as first_order_quarter 
from 
	(
	select
		sdt,customer_code,business_type_code,business_type_name,sale_amt,sale_amt_no_tax,profit,profit_no_tax,performance_city_code
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20210101' and sdt<='20230930' 
		and channel_code in ('1', '7', '9') and business_type_code in (1)
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
			customer_code,customer_name,first_category_name,second_category_name,third_category_name,performance_region_name,performance_province_name,performance_city_name,performance_city_code
		from 
			csx_dim.csx_dim_crm_customer_info
		where 
			sdt = 'current'
		) c on a.customer_code = c.customer_code
	left join
		(
		select 
			t1.customer_code,t1.first_sale_date,t2.quarter_of_year as first_order_quarter  
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
			t2.quarter_of_year as yw_first_order_quarter  
		from 
			(
			select
				customer_code,business_type_code,business_type_name,first_business_sale_date
			from
				csx_dws.csx_dws_crm_customer_business_active_di
			where 
				sdt='current' 
				and business_type_code in (1)
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
			quarter,customer_no,customer_large_level,customer_small_level,customer_level_tag,city_group_code
        from 
			csx_analyse.csx_analyse_report_sale_customer_level_qf
        where 
			quarter>='20211' and tag=1
		) f on a.customer_code=f.customer_no and b.quarter_of_year=f.quarter and a.performance_city_code=f.city_group_code
group by 
	a.customer_code,
	c.customer_name,
	c.first_category_name,
	c.second_category_name,
	c.third_category_name,
	c.performance_region_name,
	c.performance_province_name,
	c.performance_city_name,
	b.quarter_of_year,
	a.business_type_name,
    f.customer_large_level,
	f.customer_small_level,
	f.customer_level_tag,
	e.yw_first_sale_date,
	(case when e.yw_first_sale_date<'20210101' then '20年及之前' else e.yw_first_order_quarter end),
	d.first_sale_date,		
	(case when d.first_sale_date<'20210101' then '20年及之前' else d.first_order_quarter end)  
;
select * from csx_analyse_tmp.csx_analyse_tmp_rp_category_sale_detail;
