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
		sdt>='20220101' and sdt<='20230630' 
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

	
select
	a.smonth,
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	a.customer_code,
	a.sale_amt
from
	(
	select
		substr(sdt,1,6) as smonth,
		customer_code,
		performance_region_name,
		performance_province_name,
		performance_city_name,
		sum(sale_amt) as sale_amt
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20230101' and sdt<='20230630' 
		and channel_code in ('1', '7', '9') 
		and business_type_code in (1)
	group by 
		substr(sdt,1,6),customer_code,
		performance_region_name,
		performance_province_name,
		performance_city_name
	) a 
	join
		(
		select
			customer_no,month,customer_large_level
		from 
			csx_analyse.csx_analyse_report_sale_customer_level_mf
		where
			month>='202301' and month<='202306'
			and tag=1 -- 数据标识：1：全量数据；2：剔除不统计业绩仓数据
			and customer_large_level in ('A','B')
		group by 
			customer_no,month,customer_large_level
		) f on f.customer_no=a.customer_code and f.month=a.smonth
		
-- 签收SKU数 只取渠道为大客户、业务代理，业务类型剔除BBC，城市服务商，不包含退货和调价返利
drop table if exists csx_analyse_tmp.csx_analyse_tmp_oms_complaint_report_01;
create table csx_analyse_tmp.csx_analyse_tmp_oms_complaint_report_01
as
select 
	substr(a.sdt,1,6) as smonth,a.performance_region_name,a.performance_province_name,a.performance_city_name,b.classify_large_name,b.classify_middle_name,b.business_division_name,
	count(a.goods_code) as sku_cnt
from
	( 
	select
		sdt,customer_code,order_code,goods_code,sale_amt,profit,performance_region_name,performance_province_name,performance_city_name
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20230101' and sdt<='20230630'
		and channel_code in('1','7','9')
		and business_type_code not in(4,6) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
		and order_channel_code =1 -- 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
		and refund_order_flag=0 -- 退货订单标识(0-正向单 1-逆向单)
		and performance_province_name !='平台-B'
	) a 
	left join
		(
		select
			goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name,business_division_code,business_division_name
		from
			csx_dim.csx_dim_basic_goods
		where
			sdt='current'
		) b on b.goods_code=a.goods_code	
group by 
	substr(a.sdt,1,6),a.performance_region_name,a.performance_province_name,a.performance_city_name,b.classify_large_name,b.classify_middle_name,b.business_division_name
;
select * from csx_analyse_tmp.csx_analyse_tmp_oms_complaint_report_01;	
