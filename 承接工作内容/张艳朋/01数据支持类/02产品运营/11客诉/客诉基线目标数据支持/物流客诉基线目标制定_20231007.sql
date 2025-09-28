

-- 签收SKU数 只取渠道为大客户、业务代理，业务类型剔除BBC，城市服务商，不包含退货和调价返利
drop table if exists csx_analyse_tmp.csx_analyse_tmp_oms_complaint_report_01;
create table csx_analyse_tmp.csx_analyse_tmp_oms_complaint_report_01
as
select 
	substr(a.sdt,1,6) as smonth,c.week_of_year,a.performance_region_name,a.performance_province_name,a.performance_city_name,
	count(a.goods_code) as sku_cnt
from
	( 
	select
		sdt,customer_code,order_code,goods_code,sale_amt,profit,performance_region_name,performance_province_name,performance_city_name
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20230701' and sdt<='20231006'
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
	left join
		(
		select
			calday,week_of_year
		from
			csx_dim.csx_dim_basic_date
		) c on c.calday=a.sdt
group by 
	substr(a.sdt,1,6),c.week_of_year,a.performance_region_name,a.performance_province_name,a.performance_city_name
;
select * from csx_analyse_tmp.csx_analyse_tmp_oms_complaint_report_01;	



-- 责任部门统计 同一个客诉单涉及多个责任部门则每个部门+1
drop table if exists csx_analyse_tmp.csx_analyse_tmp_oms_complaint_report_03;
create table csx_analyse_tmp.csx_analyse_tmp_oms_complaint_report_03
as
select 
	a.smonth,e.week_of_year,a.performance_region_name,a.performance_province_name,a.performance_city_name,
	a.first_level_department_name,
	a.second_level_department_name,
	count(distinct a.complaint_code) as ks_cnt
from
	(
	select
		substr(sdt,1,6) as smonth,sdt,performance_region_name,performance_province_name,performance_city_name,customer_code,sale_order_code,goods_code,goods_name,complaint_code,
		first_level_department_name,second_level_department_name,main_category_name,sub_category_name
	from
		csx_analyse.csx_analyse_fr_oms_complaint_detail_di
	where 
		sdt>='20230701' and sdt<='20231006'
		and complaint_status_code in(20,30) -- 客诉单状态 10'待判责' 20'处理中' 21'待审核' 30'已完成' -1'已取消'
		--and complaint_deal_status !=-1 -- 责任环节状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
		and complaint_deal_status in(10,40) -- 责任环节状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
		and second_level_department_name !=''
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
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) c on c.customer_code=a.customer_code
	left join
		(
		select
			customer_no,customer_large_level,month
		from 
			csx_analyse.csx_analyse_report_sale_customer_level_mf
		where
			month>='202307' and month<='202310'
			-- and customer_large_level in ('A','B')
			and tag=1 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
		) d on d.customer_no=a.customer_code and d.month=a.smonth
	left join
		(
		select
			calday,week_of_year
		from
			csx_dim.csx_dim_basic_date
		) e on e.calday=a.sdt
where
	a.performance_province_name !='平台-B'
group by 
	a.smonth,e.week_of_year,a.performance_region_name,a.performance_province_name,a.performance_city_name,
	a.first_level_department_name,
	a.second_level_department_name
;
select * from csx_analyse_tmp.csx_analyse_tmp_oms_complaint_report_03;


	--select
	--	sdt,performance_region_name,performance_province_name,
	--	first_level_department_name,-- second_level_department_name,
	--	count(distinct complaint_code) as ks_cnt
	--from
	--	csx_analyse.csx_analyse_fr_oms_complaint_detail_di
	--where 
	--	sdt>='20230701' and sdt<='20230814'
	--	and complaint_status_code in(20,30) -- 客诉单状态 10'待判责' 20'处理中' 21'待审核' 30'已完成' -1'已取消'
	--	and complaint_deal_status !=-1 -- 责任环节状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
	--	and second_level_department_name !=''
	--	and performance_province_name='北京市'
	--group by 
	--	sdt,performance_region_name,performance_province_name,
	--	first_level_department_name -- second_level_department_name		