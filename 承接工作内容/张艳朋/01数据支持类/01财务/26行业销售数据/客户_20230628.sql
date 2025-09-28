-- B+BBC新老客行业分析
drop table if exists csx_analyse_tmp.csx_analyse_tmp_b_category_sale_detail;
create table csx_analyse_tmp.csx_analyse_tmp_b_category_sale_detail
as	
select
	a.customer_code,
	a.credit_code,
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
		sdt>='20220101' and sdt<='20230627' 
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
	a.credit_code,
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

select
	*
from
	csx_dws.csx_dws_sale_detail_di
where 
	sdt>='20230301' and sdt<='20230627' 
	and channel_code in ('1', '7', '9') 
	and business_type_code in (1)
	-- and customer_code='128026'
	and credit_code =''
	and require_delivery_date>='20230301'
	and order_channel_code not in (4,6) -- 订单来源渠道: 1-b端 2-m端 3-bbc 4-调价返利 -1-sap
	and order_code not like 'CA%'
	
