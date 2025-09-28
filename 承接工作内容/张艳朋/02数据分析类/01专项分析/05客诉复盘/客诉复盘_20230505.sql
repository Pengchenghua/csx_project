
-- ================================================================================================================================================================================
-- 签收SKU数 只取渠道为大客户、业务代理，业务类型剔除BBC，城市服务商，不包含退货和调价返利
select 
	d.performance_region_name,d.performance_province_name,substr(a.sdt,1,6) as smonth,
	count(a.goods_code) as sku_cnt
from
	( 
	select
		sdt,customer_code,order_code,goods_code,sale_amt,profit
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20230101' and sdt<='20230430'
		and channel_code in('1','7','9')
		and business_type_code not in(4,6) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
		and order_channel_code =1 -- 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
		and refund_order_flag=0 -- 退货订单标识(0-正向单 1-逆向单)
	) a 
	-- left join
	-- 	(
	-- 	select
	-- 		calday,quarter_of_year
	-- 	from
	-- 		csx_dim.csx_dim_basic_date
	-- 	) b on b.calday=a.sdt
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) d on d.customer_code=a.customer_code
where
	d.performance_province_name !='平台-B'		
group by 
	d.performance_region_name,d.performance_province_name,substr(a.sdt,1,6)
;

-- 客诉数
select 
	d.performance_region_name,d.performance_province_name,substr(a.sdt,1,6) as smonth,
	count(distinct a.complaint_code) as ks_cnt
from
	(
	select
		sdt,performance_region_name,performance_province_name,customer_code,sale_order_code,goods_code,goods_name,complaint_code
	from
		csx_analyse.csx_analyse_fr_oms_complaint_detail_di
	where 
		sdt>='20230101' and sdt<='20230430'
		and complaint_status_code in(20,30) -- 客诉单状态 10'待判责' 20'处理中' 21'待审核' 30'已完成' -1'已取消'
		and complaint_deal_status !=-1 -- 责任环节状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
	) a 
	-- left join
	-- 	(
	-- 	select
	-- 		calday,quarter_of_year
	-- 	from
	-- 		csx_dim.csx_dim_basic_date
	-- 	) b on b.calday=a.sdt
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) d on d.customer_code=a.customer_code
where
	d.performance_province_name !='平台-B'
group by 
	d.performance_region_name,d.performance_province_name,substr(a.sdt,1,6)

-- 客诉数 按一二级分类
select 
	d.performance_region_name,d.performance_province_name,substr(a.sdt,1,6) as smonth,main_category_name,sub_category_name,
	count(distinct a.complaint_code) as ks_cnt
from
	(
	select
		sdt,performance_region_name,performance_province_name,customer_code,sale_order_code,goods_code,goods_name,complaint_code,main_category_name,sub_category_name
	from
		csx_analyse.csx_analyse_fr_oms_complaint_detail_di
	where 
		sdt>='20230101' and sdt<='20230430'
		and complaint_status_code in(20,30) -- 客诉单状态 10'待判责' 20'处理中' 21'待审核' 30'已完成' -1'已取消'
		and complaint_deal_status !=-1 -- 责任环节状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
	) a 
	-- left join
	-- 	(
	-- 	select
	-- 		calday,quarter_of_year
	-- 	from
	-- 		csx_dim.csx_dim_basic_date
	-- 	) b on b.calday=a.sdt
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) d on d.customer_code=a.customer_code
where
	d.performance_province_name !='平台-B'
group by 
	d.performance_region_name,d.performance_province_name,substr(a.sdt,1,6),main_category_name,sub_category_name

	
-- 客诉 高频客户
select 
	d.performance_region_name,d.performance_province_name,substr(a.sdt,1,6) as smonth,a.customer_code,d.customer_name,a.main_category_name,e.customer_large_level,
	count(distinct a.complaint_code) as ks_cnt
from
	(
	select
		substr(sdt,1,6) as month,sdt,performance_region_name,performance_province_name,customer_code,sale_order_code,goods_code,goods_name,complaint_code,main_category_name,sub_category_name
	from
		csx_analyse.csx_analyse_fr_oms_complaint_detail_di
	where 
		sdt>='20230101' and sdt<='20230430'
		and complaint_status_code in(20,30) -- 客诉单状态 10'待判责' 20'处理中' 21'待审核' 30'已完成' -1'已取消'
		and complaint_deal_status !=-1 -- 责任环节状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
	) a 
	-- left join
	-- 	(
	-- 	select
	-- 		calday,quarter_of_year
	-- 	from
	-- 		csx_dim.csx_dim_basic_date
	-- 	) b on b.calday=a.sdt
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) d on d.customer_code=a.customer_code
	left join
		(
		select
			customer_no,customer_large_level,month
		from 
			csx_analyse.csx_analyse_report_sale_customer_level_mf
		where
			month>='202301' and month<='202304'
			-- and customer_large_level in ('A','B')
			and tag=1 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
		) e on e.customer_no=a.customer_code and e.month=a.month
where
	d.performance_province_name !='平台-B'
group by 
	d.performance_region_name,d.performance_province_name,substr(a.sdt,1,6),a.customer_code,d.customer_name,a.main_category_name,e.customer_large_level
;

-- 销售
	select
		substr(sdt,1,6) as smonth,customer_code,sum(sale_amt) as sale_amt,sum(profit) as profit
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20230101' and sdt<='20230430'
		and channel_code in('1','7','9')
		and business_type_code not in(4,6) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
		and customer_code in ('128359','127391','111608','118094','114244','129558','116715','131162','125624','114548','129581','131427','130061','106124','106775','119171',
		'106287','115006','104901','123086','PF1205','127082','115906','107305','103320','128359','127391','111608','118094','116715','129558','120416','106898',
		'130465','117721','114244','114548','125624','106124','119171','114682','129543','125029','130843','223816')
	group by 
		substr(sdt,1,6),customer_code
;

-- 责任部门
	select
		substr(sdt,1,6) as month,first_level_department_name,second_level_department_name,concat(first_level_department_name,'-',second_level_department_name) as department_name,count(distinct complaint_code) as ks_cnt
	from
		csx_analyse.csx_analyse_fr_oms_complaint_detail_di
	where 
		sdt>='20230101' and sdt<='20230430'
		and complaint_status_code in(20,30) -- 客诉单状态 10'待判责' 20'处理中' 21'待审核' 30'已完成' -1'已取消'
		and complaint_deal_status !=-1 -- 责任环节状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
	group by 
		substr(sdt,1,6),first_level_department_name,second_level_department_name,concat(first_level_department_name,'-',second_level_department_name)
		
-- 品类
	select
		substr(sdt,1,6) as month,classify_large_name,classify_middle_name,count(distinct complaint_code) as ks_cnt
	from
		csx_analyse.csx_analyse_fr_oms_complaint_detail_di
	where 
		sdt>='20230101' and sdt<='20230430'
		and complaint_status_code in(20,30) -- 客诉单状态 10'待判责' 20'处理中' 21'待审核' 30'已完成' -1'已取消'
		and complaint_deal_status !=-1 -- 责任环节状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
	group by 
		substr(sdt,1,6),classify_large_name,classify_middle_name
		

-- 品类
	select
		substr(sdt,1,6) as month,performance_region_name,classify_large_name,classify_middle_name,main_category_name,count(distinct complaint_code) as ks_cnt
	from
		csx_analyse.csx_analyse_fr_oms_complaint_detail_di
	where 
		sdt>='20230101' and sdt<='20230430'
		and complaint_status_code in(20,30) -- 客诉单状态 10'待判责' 20'处理中' 21'待审核' 30'已完成' -1'已取消'
		and complaint_deal_status !=-1 -- 责任环节状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
	group by 
		substr(sdt,1,6),performance_region_name,classify_large_name,classify_middle_name,main_category_name

-- ===============================================================================================================================================================================
-- 签收SKU数 只取渠道为大客户、业务代理，业务类型剔除BBC，城市服务商，不包含退货和调价返利
select 
	d.performance_region_name,d.performance_province_name,substr(a.sdt,1,6) as smonth,classify_large_name,classify_middle_name,
	count(a.goods_code) as sku_cnt
from
	( 
	select
		sdt,customer_code,order_code,goods_code,sale_amt,profit,classify_large_name,classify_middle_name
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20230101' and sdt<='20230430'
		and channel_code in('1','7','9')
		and business_type_code not in(4,6) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
		and order_channel_code =1 -- 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
		and refund_order_flag=0 -- 退货订单标识(0-正向单 1-逆向单)
	) a 
	-- left join
	-- 	(
	-- 	select
	-- 		calday,quarter_of_year
	-- 	from
	-- 		csx_dim.csx_dim_basic_date
	-- 	) b on b.calday=a.sdt
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) d on d.customer_code=a.customer_code
where
	d.performance_province_name !='平台-B'		
group by 
	d.performance_region_name,d.performance_province_name,substr(a.sdt,1,6),classify_large_name,classify_middle_name
;

-- 客诉数
select 
	d.performance_region_name,d.performance_province_name,substr(a.sdt,1,6) as smonth,classify_large_name,classify_middle_name,
	count(distinct a.complaint_code) as ks_cnt
from
	(
	select
		sdt,performance_region_name,performance_province_name,customer_code,sale_order_code,goods_code,goods_name,complaint_code,classify_large_name,classify_middle_name
	from
		csx_analyse.csx_analyse_fr_oms_complaint_detail_di
	where 
		sdt>='20230101' and sdt<='20230430'
		and complaint_status_code in(20,30) -- 客诉单状态 10'待判责' 20'处理中' 21'待审核' 30'已完成' -1'已取消'
		and complaint_deal_status !=-1 -- 责任环节状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
	) a 
	-- left join
	-- 	(
	-- 	select
	-- 		calday,quarter_of_year
	-- 	from
	-- 		csx_dim.csx_dim_basic_date
	-- 	) b on b.calday=a.sdt
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) d on d.customer_code=a.customer_code
where
	d.performance_province_name !='平台-B'
group by 
	d.performance_region_name,d.performance_province_name,substr(a.sdt,1,6),classify_large_name,classify_middle_name
;

-- 高频责任部门
select 
	d.performance_region_name,d.performance_province_name,substr(a.sdt,1,6) as smonth,first_level_department_name,second_level_department_name,main_category_name,
	count(distinct a.complaint_code) as ks_cnt
from
	(
	select
		sdt,performance_region_name,performance_province_name,customer_code,sale_order_code,goods_code,goods_name,complaint_code,classify_large_name,classify_middle_name,
		first_level_department_name,second_level_department_name,main_category_name
	from
		csx_analyse.csx_analyse_fr_oms_complaint_detail_di
	where 
		sdt>='20230101' and sdt<='20230430'
		and complaint_status_code in(20,30) -- 客诉单状态 10'待判责' 20'处理中' 21'待审核' 30'已完成' -1'已取消'
		and complaint_deal_status !=-1 -- 责任环节状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
	) a 
	-- left join
	-- 	(
	-- 	select
	-- 		calday,quarter_of_year
	-- 	from
	-- 		csx_dim.csx_dim_basic_date
	-- 	) b on b.calday=a.sdt
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) d on d.customer_code=a.customer_code
where
	d.performance_province_name !='平台-B'
group by 
	d.performance_region_name,d.performance_province_name,substr(a.sdt,1,6),first_level_department_name,second_level_department_name,main_category_name
