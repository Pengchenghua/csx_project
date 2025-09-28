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
		sdt>='20210101' and sdt<='20230630' 
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
		sdt>='20210101' and sdt<='20230630' 
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
--================================================================================================================================================================================			
--计算客户最早下单日期
drop table if exists csx_analyse_tmp.csx_analyse_tmp_customer_first_order_date_20230117;
create table csx_analyse_tmp.csx_analyse_tmp_customer_first_order_date_20230117	
as
select
	b.performance_province_name,
	a.customer_code,
	a.b_bbc_first_order_date,
	case when substr(b_bbc_first_order_date,1,4) <='2019' then '19年及之前新客'
		when substr(b_bbc_first_order_date,1,4)='2020' then '20年新客'
		when substr(b_bbc_first_order_date,1,4)='2021' then '21年新客'
		when substr(b_bbc_first_order_date,1,4)='2022' then '22年新客'
		when substr(b_bbc_first_order_date,1,4)='2023' then '23年新客'
		else '其他'
	end as b_bbc_customer_type,
	a.rp_first_order_date,
	case when substr(rp_first_order_date,1,4) <='2019' then '19年及之前新客'
		when substr(rp_first_order_date,1,4)='2020' then '20年新客'
		when substr(rp_first_order_date,1,4)='2021' then '21年新客'
		when substr(rp_first_order_date,1,4)='2022' then '22年新客'
		when substr(rp_first_order_date,1,4)='2023' then '23年新客'
		else '其他'
	end as rp_customer_type
from	
	(
	select
		customer_code,
		min(sdt) as b_bbc_first_order_date,
		min(case when business_type_code in (1) then sdt end) as rp_first_order_date
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20190101' and sdt<='20230630'
		and channel_code in('1','7','9')
		and order_channel_code not in(4,6)
	group by 
		customer_code
	) a 
	left join
		(
		select
			customer_code,customer_name,performance_province_name
		from 
			csx_dim.csx_dim_crm_customer_info
		where 
			sdt = 'current'
		) b on b.customer_code=a.customer_code
;
--================================================================================================================================================================================
--b+bbc
-- d:2020
-- e:2021
-- f:2022
-- g:2023
select
	b.performance_province_name,
	b.b_bbc_customer_type,
	a.syear,
	count(distinct case when b.b_bbc_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is null and f.customer_code is null and g.customer_code is null then a.customer_code end) as `20年履约且23年、22、21年未履约的20年前新客户客户数`,
	count(distinct case when b.b_bbc_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.customer_code end) as `21年履约且23年、22年未履约的20年前新客户客户数`,
	count(distinct case when b.b_bbc_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.customer_code end) as `22年履约且23年未履约的20年前新客户客户数`,
	count(distinct case when b.b_bbc_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.customer_code end) as `23年履约的20年前新客户客户数`,
	sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is null and f.customer_code is null and g.customer_code is null then a.sale_amt end) as `20年履约且23年、22、21年未履约的20年前新客户含税收入`,
	sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.sale_amt end) as `21年履约且23年、22年未履约的20年前新客户含税收入`,
	sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.sale_amt end) as `22年履约且23年未履约的20年前新客户含税收入`,
	sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.sale_amt end) as `23年履约的20年前新客户含税收入`,

	sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is null and f.customer_code is null and g.customer_code is null then a.profit_no_tax end)/
	abs(sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is null and f.customer_code is null and g.customer_code is null then a.sale_amt_no_tax end)) as `20年履约且23年、22、21年未履约的20年前新客户不含税毛利率`,
	
	sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.profit_no_tax end)/
	abs(sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.sale_amt_no_tax end)) as `21年履约且23年、22年未履约的20年前新客户不含税毛利率`,
	
	sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.profit_no_tax end)/
	abs(sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.sale_amt_no_tax end)) as `22年履约且23年未履约的20年前新客户不含税毛利率`,
	
	sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.profit_no_tax end)/
	abs(sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.sale_amt_no_tax end)) as `23年履约的20年前新客户不含税毛利率`,
	
	sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is null and f.customer_code is null and g.customer_code is null then a.profit_no_tax end) as `20年履约且23年、22、21年未履约的20年前新客户不含税毛利额`,
	sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is null and f.customer_code is null and g.customer_code is null then a.sale_amt_no_tax end) as `20年履约且23年、22、21年未履约的20年前新客户不含税销售额`,
	
	sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.profit_no_tax end)as `21年履约且23年、22年未履约的20年前新客户不含税毛利额`,
	sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.sale_amt_no_tax end) as `21年履约且23年、22年未履约的20年前新客户不含税销售额`,
	
	sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.profit_no_tax end)as `22年履约且23年未履约的20年前新客户不含税毛利额`,
	sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.sale_amt_no_tax end) as `22年履约且23年未履约的20年前新客户不含税销售额`,
	
	sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.profit_no_tax end)as `23年履约的20年前新客户不含税毛利额`,
	sum(case when b.b_bbc_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.sale_amt_no_tax end) as `23年履约的20年前新客户不含税销售额`,

	--20年新客
	count(distinct case when b.b_bbc_customer_type='20年新客' and d.customer_code is not null and e.customer_code is null and f.customer_code is null and g.customer_code is null then a.customer_code end) as `20年履约且23年、22、21年未履约的20年新客户客户数`,
	count(distinct case when b.b_bbc_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.customer_code end) as `21年履约且23年、22年未履约的20年新客户客户数`,
	count(distinct case when b.b_bbc_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.customer_code end) as `22年履约且23年未履约的20年新客户客户数`,
	count(distinct case when b.b_bbc_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.customer_code end) as `23年履约的20年新客户客户数`,
	sum(case when b.b_bbc_customer_type='20年新客' and d.customer_code is not null and e.customer_code is null and f.customer_code is null and g.customer_code is null then a.sale_amt end) as `20年履约且23年、22、21年未履约的20年新客户含税收入`,
	sum(case when b.b_bbc_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.sale_amt end) as `21年履约且23年、22年未履约的20年新客户含税收入`,
	sum(case when b.b_bbc_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.sale_amt end) as `22年履约且23年未履约的20年新客户含税收入`,
	sum(case when b.b_bbc_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.sale_amt end) as `23年履约的20年新客户含税收入`,	

	sum(case when b.b_bbc_customer_type='20年新客' and d.customer_code is not null and e.customer_code is null and f.customer_code is null and g.customer_code is null then a.profit_no_tax end)/
	abs(sum(case when b.b_bbc_customer_type='20年新客' and d.customer_code is not null and e.customer_code is null and f.customer_code is null and g.customer_code is null then a.sale_amt_no_tax end)) as `20年履约且23年、22、21年未履约的20年新客户不含税毛利率`,
	
	sum(case when b.b_bbc_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.profit_no_tax end)/
	abs(sum(case when b.b_bbc_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.sale_amt_no_tax end)) as `21年履约且23年、22年未履约的20年新客户不含税毛利率`,
	
	sum(case when b.b_bbc_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.profit_no_tax end)/
	abs(sum(case when b.b_bbc_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.sale_amt_no_tax end)) as `22年履约且23年未履约的20年新客户不含税毛利率`,	

	sum(case when b.b_bbc_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.profit_no_tax end)/
	abs(sum(case when b.b_bbc_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.sale_amt_no_tax end)) as `23年履约的20年新客户不含税毛利率`,	

	sum(case when b.b_bbc_customer_type='20年新客' and d.customer_code is not null and e.customer_code is null and f.customer_code is null and g.customer_code is null then a.profit_no_tax end) as `20年履约且23年、22、21年未履约的20年新客户不含税毛利额`,
	sum(case when b.b_bbc_customer_type='20年新客' and d.customer_code is not null and e.customer_code is null and f.customer_code is null and g.customer_code is null then a.sale_amt_no_tax end) as `20年履约且23年、22、21年未履约的20年新客户不含税销售额`,
	
	sum(case when b.b_bbc_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.profit_no_tax end) as `21年履约且23年、22年未履约的20年新客户不含税毛利额`,
	sum(case when b.b_bbc_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.sale_amt_no_tax end) as `21年履约且23年、22年未履约的20年新客户不含税销售额`,
	
	sum(case when b.b_bbc_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.profit_no_tax end) as `22年履约且23年未履约的20年新客户不含税毛利额`,
	sum(case when b.b_bbc_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.sale_amt_no_tax end) as `22年履约且23年未履约的20年新客户不含税销售额`,
	
	sum(case when b.b_bbc_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.profit_no_tax end) as `23年履约的20年新客户不含税毛利额`,
	sum(case when b.b_bbc_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.sale_amt_no_tax end) as `23年履约的20年新客户不含税销售额`,	
	--21年新客
	count(distinct case when b.b_bbc_customer_type='21年新客' and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.customer_code end) as `22、23均未履约的21年新客户客户数`,
	count(distinct case when b.b_bbc_customer_type='21年新客' and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.customer_code end) as `22年履约且23年未履约的21年新客户客户数`,
	count(distinct case when b.b_bbc_customer_type='21年新客' and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.customer_code end) as `23年履约的21年新客户客户数`,
	sum(case when b.b_bbc_customer_type='21年新客' and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.sale_amt end) as `22、23均未履约的21年新客户含税收入`,
	sum(case when b.b_bbc_customer_type='21年新客' and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.sale_amt end) as `22年履约且23年未履约的21年新客户含税收入`,
	sum(case when b.b_bbc_customer_type='21年新客' and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.sale_amt end) as `23年履约的21年新客户含税收入`,

	sum(case when b.b_bbc_customer_type='21年新客' and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.profit_no_tax end)/
	abs(sum(case when b.b_bbc_customer_type='21年新客' and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.sale_amt_no_tax end)) as `22、23均未履约的21年新客户不含税毛利率`,
	
	sum(case when b.b_bbc_customer_type='21年新客' and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.profit_no_tax end)/
	abs(sum(case when b.b_bbc_customer_type='21年新客' and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.sale_amt_no_tax end)) as `22年履约且23年未履约的21年新客户不含税毛利率`,

	sum(case when b.b_bbc_customer_type='21年新客' and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.profit_no_tax end)/
	abs(sum(case when b.b_bbc_customer_type='21年新客' and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.sale_amt_no_tax end)) as `23年履约的21年新客户不含税毛利率`,
	
	sum(case when b.b_bbc_customer_type='21年新客' and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.profit_no_tax end) as `22、23均未履约的21年新客户不含税毛利额`,
	sum(case when b.b_bbc_customer_type='21年新客' and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.sale_amt_no_tax end) as `22、23均未履约的21年新客户不含税销售额`,
	
	sum(case when b.b_bbc_customer_type='21年新客' and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.profit_no_tax end) as `22年履约且23年未履约的21年新客户不含税毛利额`,	
	sum(case when b.b_bbc_customer_type='21年新客' and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.sale_amt_no_tax end) as `22年履约且23年未履约的21年新客户不含税销售额`,

	sum(case when b.b_bbc_customer_type='21年新客' and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.profit_no_tax end) as `23年履约的21年新客户不含税毛利额`,	
	sum(case when b.b_bbc_customer_type='21年新客' and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.sale_amt_no_tax end) as `23年履约的21年新客户不含税销售额`,	
	--22年新客
	count(distinct case when b.b_bbc_customer_type='22年新客' and f.customer_code is not null and g.customer_code is null then a.customer_code end) as `22年履约且23年未履约的22年新客户客户数`,
	count(distinct case when b.b_bbc_customer_type='22年新客' and f.customer_code is not null and g.customer_code is not null then a.customer_code end) as `23年履约的22年新客户客户数`,
	sum(case when b.b_bbc_customer_type='22年新客' and f.customer_code is not null and g.customer_code is null then a.sale_amt end) as `22年履约且23年未履约的22年新客户含税收入`,
	sum(case when b.b_bbc_customer_type='22年新客' and f.customer_code is not null and g.customer_code is not null then a.sale_amt end) as `23年履约的22年新客户含税收入`, 
	
	sum(case when b.b_bbc_customer_type='22年新客' and f.customer_code is not null and g.customer_code is null then a.profit_no_tax end)/
	abs(sum(case when b.b_bbc_customer_type='22年新客' and f.customer_code is not null and g.customer_code is null then a.sale_amt_no_tax end)) as `22年履约且23年未履约的22年新客户不含税毛利率`,

	sum(case when b.b_bbc_customer_type='22年新客' and f.customer_code is not null and g.customer_code is not null then a.profit_no_tax end)/
	abs(sum(case when b.b_bbc_customer_type='22年新客' and f.customer_code is not null and g.customer_code is not null then a.sale_amt_no_tax end)) as `23年履约的22年新客户不含税毛利率`,	
	
	sum(case when b.b_bbc_customer_type='22年新客' and f.customer_code is not null and g.customer_code is null then a.profit_no_tax end) as `22年履约且23年未履约的22年新客户不含税毛利额`,
	sum(case when b.b_bbc_customer_type='22年新客' and f.customer_code is not null and g.customer_code is null then a.sale_amt_no_tax end) as `22年履约且23年未履约的22年新客户不含税销售额`,
	
	sum(case when b.b_bbc_customer_type='22年新客' and f.customer_code is not null and g.customer_code is not null then a.profit_no_tax end) as `23年履约的22年新客户不含税毛利额`,
	sum(case when b.b_bbc_customer_type='22年新客' and f.customer_code is not null and g.customer_code is not null then a.sale_amt_no_tax end) as `23年履约的22年新客户不含税销售额`,
	--23年新客
	count(distinct case when b.b_bbc_customer_type='23年新客' and g.customer_code is not null then a.customer_code end) as `23年履约的23年新客户客户数`,
	sum(case when b.b_bbc_customer_type='23年新客' and g.customer_code is not null then a.sale_amt end) as `23年履约的23年新客户含税收入`,
	
	sum(case when b.b_bbc_customer_type='23年新客' and g.customer_code is not null then a.profit_no_tax end)/
	abs(sum(case when b.b_bbc_customer_type='23年新客' and g.customer_code is not null then a.sale_amt_no_tax end)) as `23年履约的23年新客户不含税毛利率`,	
	
	sum(case when b.b_bbc_customer_type='23年新客' and g.customer_code is not null then a.profit_no_tax end) as `23年履约的23年新客户不含税毛利额`,
	sum(case when b.b_bbc_customer_type='23年新客' and g.customer_code is not null then a.sale_amt_no_tax end) as `23年履约的23年新客户不含税销售额`
from
	(-- 履约
	select
		customer_code,substr(sdt,1,4) as syear,
		sum(sale_amt) as sale_amt,
		sum(sale_amt_no_tax) as sale_amt_no_tax,
		sum(profit_no_tax) as profit_no_tax
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20200101' and sdt<='20230630'
		and channel_code in('1','7','9')
	group by 
		customer_code,substr(sdt,1,4)
	) a
	join
		(
		select
			performance_province_name,customer_code,b_bbc_first_order_date,b_bbc_customer_type
		from
			csx_analyse_tmp.csx_analyse_tmp_customer_first_order_date_20230117
		where
			1=1
			--b_bbc_customer_type='19年及之前新客'
		) b on b.customer_code=a.customer_code
	left join --2020年履约
		(
		select
			customer_code,substr(sdt,1,4) as syear,
			sum(sale_amt) as sale_amt,
			sum(sale_amt_no_tax) as sale_amt_no_tax,
			sum(profit_no_tax) as profit_no_tax
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20200101' and sdt<='20201231'
			and channel_code in('1','7','9')
		group by 
			customer_code,substr(sdt,1,4)
		) d on d.customer_code=a.customer_code
	left join --2021年履约
		(
		select
			customer_code,substr(sdt,1,4) as syear,
			sum(sale_amt) as sale_amt,
			sum(sale_amt_no_tax) as sale_amt_no_tax,
			sum(profit_no_tax) as profit_no_tax
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20210101' and sdt<='20211231'
			and channel_code in('1','7','9')
		group by 
			customer_code,substr(sdt,1,4)
		) e on e.customer_code=a.customer_code
	left join --2022年履约
		(
		select
			customer_code,substr(sdt,1,4) as syear,
			sum(sale_amt) as sale_amt,
			sum(sale_amt_no_tax) as sale_amt_no_tax,
			sum(profit_no_tax) as profit_no_tax
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20220101' and sdt<='20221231'
			and channel_code in('1','7','9')
		group by 
			customer_code,substr(sdt,1,4)
		) f on f.customer_code=a.customer_code
	left join --2023年履约
		(
		select
			customer_code,substr(sdt,1,4) as syear,
			sum(sale_amt) as sale_amt,
			sum(sale_amt_no_tax) as sale_amt_no_tax,
			sum(profit_no_tax) as profit_no_tax
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20230101' and sdt<='20230630'
			and channel_code in('1','7','9')
		group by 
			customer_code,substr(sdt,1,4)
		) g on g.customer_code=a.customer_code
group by 
	b.performance_province_name,b.b_bbc_customer_type,a.syear
;
		
--================================================================================================================================================================================
-- 日配
select
	b.performance_province_name,
	b.rp_customer_type,
	a.syear,
	count(distinct case when b.rp_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is null and f.customer_code is null and g.customer_code is null then a.customer_code end) as `20年履约且23年、22、21年未履约的20年前新客户客户数`,
	count(distinct case when b.rp_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.customer_code end) as `21年履约且23年、22年未履约的20年前新客户客户数`,
	count(distinct case when b.rp_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.customer_code end) as `22年履约且23年未履约的20年前新客户客户数`,
	count(distinct case when b.rp_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.customer_code end) as `23年履约的20年前新客户客户数`,
	sum(case when b.rp_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is null and f.customer_code is null and g.customer_code is null then a.sale_amt end) as `20年履约且23年、22、21年未履约的20年前新客户含税收入`,
	sum(case when b.rp_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.sale_amt end) as `21年履约且23年、22年未履约的20年前新客户含税收入`,
	sum(case when b.rp_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.sale_amt end) as `22年履约且23年未履约的20年前新客户含税收入`,
	sum(case when b.rp_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.sale_amt end) as `23年履约的20年前新客户含税收入`,

	sum(case when b.rp_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is null and f.customer_code is null and g.customer_code is null then a.profit_no_tax end)/
	abs(sum(case when b.rp_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is null and f.customer_code is null and g.customer_code is null then a.sale_amt_no_tax end)) as `20年履约且23年、22、21年未履约的20年前新客户不含税毛利率`,
	
	sum(case when b.rp_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.profit_no_tax end)/
	abs(sum(case when b.rp_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.sale_amt_no_tax end)) as `21年履约且23年、22年未履约的20年前新客户不含税毛利率`,
	
	sum(case when b.rp_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.profit_no_tax end)/
	abs(sum(case when b.rp_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.sale_amt_no_tax end)) as `22年履约且23年未履约的20年前新客户不含税毛利率`,
	
	sum(case when b.rp_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.profit_no_tax end)/
	abs(sum(case when b.rp_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.sale_amt_no_tax end)) as `23年履约的20年前新客户不含税毛利率`,
	
	sum(case when b.rp_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is null and f.customer_code is null and g.customer_code is null then a.profit_no_tax end) as `20年履约且23年、22、21年未履约的20年前新客户不含税毛利额`,
	sum(case when b.rp_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is null and f.customer_code is null and g.customer_code is null then a.sale_amt_no_tax end) as `20年履约且23年、22、21年未履约的20年前新客户不含税销售额`,
	
	sum(case when b.rp_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.profit_no_tax end)as `21年履约且23年、22年未履约的20年前新客户不含税毛利额`,
	sum(case when b.rp_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.sale_amt_no_tax end) as `21年履约且23年、22年未履约的20年前新客户不含税销售额`,
	
	sum(case when b.rp_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.profit_no_tax end)as `22年履约且23年未履约的20年前新客户不含税毛利额`,
	sum(case when b.rp_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.sale_amt_no_tax end) as `22年履约且23年未履约的20年前新客户不含税销售额`,
	
	sum(case when b.rp_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.profit_no_tax end)as `23年履约的20年前新客户不含税毛利额`,
	sum(case when b.rp_customer_type='19年及之前新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.sale_amt_no_tax end) as `23年履约的20年前新客户不含税销售额`,

	--20年新客
	count(distinct case when b.rp_customer_type='20年新客' and d.customer_code is not null and e.customer_code is null and f.customer_code is null and g.customer_code is null then a.customer_code end) as `20年履约且23年、22、21年未履约的20年新客户客户数`,
	count(distinct case when b.rp_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.customer_code end) as `21年履约且23年、22年未履约的20年新客户客户数`,
	count(distinct case when b.rp_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.customer_code end) as `22年履约且23年未履约的20年新客户客户数`,
	count(distinct case when b.rp_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.customer_code end) as `23年履约的20年新客户客户数`,
	sum(case when b.rp_customer_type='20年新客' and d.customer_code is not null and e.customer_code is null and f.customer_code is null and g.customer_code is null then a.sale_amt end) as `20年履约且23年、22、21年未履约的20年新客户含税收入`,
	sum(case when b.rp_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.sale_amt end) as `21年履约且23年、22年未履约的20年新客户含税收入`,
	sum(case when b.rp_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.sale_amt end) as `22年履约且23年未履约的20年新客户含税收入`,
	sum(case when b.rp_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.sale_amt end) as `23年履约的20年新客户含税收入`,	

	sum(case when b.rp_customer_type='20年新客' and d.customer_code is not null and e.customer_code is null and f.customer_code is null and g.customer_code is null then a.profit_no_tax end)/
	abs(sum(case when b.rp_customer_type='20年新客' and d.customer_code is not null and e.customer_code is null and f.customer_code is null and g.customer_code is null then a.sale_amt_no_tax end)) as `20年履约且23年、22、21年未履约的20年新客户不含税毛利率`,
	
	sum(case when b.rp_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.profit_no_tax end)/
	abs(sum(case when b.rp_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.sale_amt_no_tax end)) as `21年履约且23年、22年未履约的20年新客户不含税毛利率`,
	
	sum(case when b.rp_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.profit_no_tax end)/
	abs(sum(case when b.rp_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.sale_amt_no_tax end)) as `22年履约且23年未履约的20年新客户不含税毛利率`,	

	sum(case when b.rp_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.profit_no_tax end)/
	abs(sum(case when b.rp_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.sale_amt_no_tax end)) as `23年履约的20年新客户不含税毛利率`,	

	sum(case when b.rp_customer_type='20年新客' and d.customer_code is not null and e.customer_code is null and f.customer_code is null and g.customer_code is null then a.profit_no_tax end) as `20年履约且23年、22、21年未履约的20年新客户不含税毛利额`,
	sum(case when b.rp_customer_type='20年新客' and d.customer_code is not null and e.customer_code is null and f.customer_code is null and g.customer_code is null then a.sale_amt_no_tax end) as `20年履约且23年、22、21年未履约的20年新客户不含税销售额`,
	
	sum(case when b.rp_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.profit_no_tax end) as `21年履约且23年、22年未履约的20年新客户不含税毛利额`,
	sum(case when b.rp_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.sale_amt_no_tax end) as `21年履约且23年、22年未履约的20年新客户不含税销售额`,
	
	sum(case when b.rp_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.profit_no_tax end) as `22年履约且23年未履约的20年新客户不含税毛利额`,
	sum(case when b.rp_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.sale_amt_no_tax end) as `22年履约且23年未履约的20年新客户不含税销售额`,
	
	sum(case when b.rp_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.profit_no_tax end) as `23年履约的20年新客户不含税毛利额`,
	sum(case when b.rp_customer_type='20年新客' and d.customer_code is not null and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.sale_amt_no_tax end) as `23年履约的20年新客户不含税销售额`,	
	--21年新客
	count(distinct case when b.rp_customer_type='21年新客' and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.customer_code end) as `22、23均未履约的21年新客户客户数`,
	count(distinct case when b.rp_customer_type='21年新客' and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.customer_code end) as `22年履约且23年未履约的21年新客户客户数`,
	count(distinct case when b.rp_customer_type='21年新客' and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.customer_code end) as `23年履约的21年新客户客户数`,
	sum(case when b.rp_customer_type='21年新客' and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.sale_amt end) as `22、23均未履约的21年新客户含税收入`,
	sum(case when b.rp_customer_type='21年新客' and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.sale_amt end) as `22年履约且23年未履约的21年新客户含税收入`,
	sum(case when b.rp_customer_type='21年新客' and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.sale_amt end) as `23年履约的21年新客户含税收入`,

	sum(case when b.rp_customer_type='21年新客' and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.profit_no_tax end)/
	abs(sum(case when b.rp_customer_type='21年新客' and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.sale_amt_no_tax end)) as `22、23均未履约的21年新客户不含税毛利率`,
	
	sum(case when b.rp_customer_type='21年新客' and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.profit_no_tax end)/
	abs(sum(case when b.rp_customer_type='21年新客' and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.sale_amt_no_tax end)) as `22年履约且23年未履约的21年新客户不含税毛利率`,

	sum(case when b.rp_customer_type='21年新客' and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.profit_no_tax end)/
	abs(sum(case when b.rp_customer_type='21年新客' and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.sale_amt_no_tax end)) as `23年履约的21年新客户不含税毛利率`,
	
	sum(case when b.rp_customer_type='21年新客' and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.profit_no_tax end) as `22、23均未履约的21年新客户不含税毛利额`,
	sum(case when b.rp_customer_type='21年新客' and e.customer_code is not null and f.customer_code is null and g.customer_code is null then a.sale_amt_no_tax end) as `22、23均未履约的21年新客户不含税销售额`,
	
	sum(case when b.rp_customer_type='21年新客' and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.profit_no_tax end) as `22年履约且23年未履约的21年新客户不含税毛利额`,	
	sum(case when b.rp_customer_type='21年新客' and e.customer_code is not null and f.customer_code is not null and g.customer_code is null then a.sale_amt_no_tax end) as `22年履约且23年未履约的21年新客户不含税销售额`,

	sum(case when b.rp_customer_type='21年新客' and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.profit_no_tax end) as `23年履约的21年新客户不含税毛利额`,	
	sum(case when b.rp_customer_type='21年新客' and e.customer_code is not null and f.customer_code is not null and g.customer_code is not null then a.sale_amt_no_tax end) as `23年履约的21年新客户不含税销售额`,	
	--22年新客
	count(distinct case when b.rp_customer_type='22年新客' and f.customer_code is not null and g.customer_code is null then a.customer_code end) as `22年履约且23年未履约的22年新客户客户数`,
	count(distinct case when b.rp_customer_type='22年新客' and f.customer_code is not null and g.customer_code is not null then a.customer_code end) as `23年履约的22年新客户客户数`,
	sum(case when b.rp_customer_type='22年新客' and f.customer_code is not null and g.customer_code is null then a.sale_amt end) as `22年履约且23年未履约的22年新客户含税收入`,
	sum(case when b.rp_customer_type='22年新客' and f.customer_code is not null and g.customer_code is not null then a.sale_amt end) as `23年履约的22年新客户含税收入`, 
	
	sum(case when b.rp_customer_type='22年新客' and f.customer_code is not null and g.customer_code is null then a.profit_no_tax end)/
	abs(sum(case when b.rp_customer_type='22年新客' and f.customer_code is not null and g.customer_code is null then a.sale_amt_no_tax end)) as `22年履约且23年未履约的22年新客户不含税毛利率`,

	sum(case when b.rp_customer_type='22年新客' and f.customer_code is not null and g.customer_code is not null then a.profit_no_tax end)/
	abs(sum(case when b.rp_customer_type='22年新客' and f.customer_code is not null and g.customer_code is not null then a.sale_amt_no_tax end)) as `23年履约的22年新客户不含税毛利率`,	
	
	sum(case when b.rp_customer_type='22年新客' and f.customer_code is not null and g.customer_code is null then a.profit_no_tax end) as `22年履约且23年未履约的22年新客户不含税毛利额`,
	sum(case when b.rp_customer_type='22年新客' and f.customer_code is not null and g.customer_code is null then a.sale_amt_no_tax end) as `22年履约且23年未履约的22年新客户不含税销售额`,
	
	sum(case when b.rp_customer_type='22年新客' and f.customer_code is not null and g.customer_code is not null then a.profit_no_tax end) as `23年履约的22年新客户不含税毛利额`,
	sum(case when b.rp_customer_type='22年新客' and f.customer_code is not null and g.customer_code is not null then a.sale_amt_no_tax end) as `23年履约的22年新客户不含税销售额`,
	--23年新客
	count(distinct case when b.rp_customer_type='23年新客' and g.customer_code is not null then a.customer_code end) as `23年履约的23年新客户客户数`,
	sum(case when b.rp_customer_type='23年新客' and g.customer_code is not null then a.sale_amt end) as `23年履约的23年新客户含税收入`,
	
	sum(case when b.rp_customer_type='23年新客' and g.customer_code is not null then a.profit_no_tax end)/
	abs(sum(case when b.rp_customer_type='23年新客' and g.customer_code is not null then a.sale_amt_no_tax end)) as `23年履约的23年新客户不含税毛利率`,	
	
	sum(case when b.rp_customer_type='23年新客' and g.customer_code is not null then a.profit_no_tax end) as `23年履约的23年新客户不含税毛利额`,
	sum(case when b.rp_customer_type='23年新客' and g.customer_code is not null then a.sale_amt_no_tax end) as `23年履约的23年新客户不含税销售额`
	
from
	(--履约
	select
		customer_code,substr(sdt,1,4) as syear,
		sum(sale_amt) as sale_amt,
		sum(sale_amt_no_tax) as sale_amt_no_tax,
		sum(profit_no_tax) as profit_no_tax
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20200101' and sdt<='20230630'
		and channel_code in('1','7','9')
		and business_type_code=1
		and order_channel_code not in(4,6)
	group by 
		customer_code,substr(sdt,1,4)
	) a
	join
		(
		select
			performance_province_name,customer_code,rp_first_order_date,rp_customer_type
		from
			csx_analyse_tmp.csx_analyse_tmp_customer_first_order_date_20230117
		where
			1=1
			and rp_customer_type<>'其他'
		) b on b.customer_code=a.customer_code
	left join --2020年履约
		(
		select
			customer_code,substr(sdt,1,4) as syear,
			sum(sale_amt) as sale_amt,
			sum(sale_amt_no_tax) as sale_amt_no_tax,
			sum(profit_no_tax) as profit_no_tax
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20200101' and sdt<='20201231'
			and channel_code in('1','7','9')
			and business_type_code=1
			and order_channel_code not in(4,6)
		group by 
			customer_code,substr(sdt,1,4)
		) d on d.customer_code=a.customer_code
	left join --2021年履约
		(
		select
			customer_code,substr(sdt,1,4) as syear,
			sum(sale_amt) as sale_amt,
			sum(sale_amt_no_tax) as sale_amt_no_tax,
			sum(profit_no_tax) as profit_no_tax
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20210101' and sdt<='20211231'
			and channel_code in('1','7','9')
			and business_type_code=1
			and order_channel_code not in(4,6)
		group by 
			customer_code,substr(sdt,1,4)
		) e on e.customer_code=a.customer_code
	left join --2022年履约
		(
		select
			customer_code,substr(sdt,1,4) as syear,
			sum(sale_amt) as sale_amt,
			sum(sale_amt_no_tax) as sale_amt_no_tax,
			sum(profit_no_tax) as profit_no_tax
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20220101' and sdt<='20221231'
			and channel_code in('1','7','9')
			and business_type_code=1
			and order_channel_code not in(4,6)
		group by 
			customer_code,substr(sdt,1,4)
		) f on f.customer_code=a.customer_code
	left join --2023年履约
		(
		select
			customer_code,substr(sdt,1,4) as syear,
			sum(sale_amt) as sale_amt,
			sum(sale_amt_no_tax) as sale_amt_no_tax,
			sum(profit_no_tax) as profit_no_tax
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20230101' and sdt<='20230630'
			and channel_code in('1','7','9')
			and business_type_code=1
			and order_channel_code not in(4,6)
		group by 
			customer_code,substr(sdt,1,4)
		) g on g.customer_code=a.customer_code
group by 
	b.performance_province_name,b.rp_customer_type,a.syear
;