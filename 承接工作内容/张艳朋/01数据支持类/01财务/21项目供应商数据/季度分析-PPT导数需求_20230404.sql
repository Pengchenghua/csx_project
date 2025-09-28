
-- ===============================================================================================================================================================
-- 
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_q_01;
create table csx_analyse_tmp.csx_analyse_tmp_finance_q_01
as
select
	a.performance_province_name,a.customer_code,b.customer_name,a.smonth,a.business_type_name,a.sale_amt,a.smonth as smonth_refund,'退货单' as refund_type,a.sale_amt_refund	
from
	(
	select
		substr(sdt,1,6) as smonth,performance_province_name,customer_code,business_type_name,
		sum(if(refund_order_flag=0,sale_amt,0)) as sale_amt,
		sum(if(refund_order_flag=1,sale_amt,0)) as sale_amt_refund
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20230101' and sdt<='20230331'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		and business_type_code=4
		-- and refund_order_flag=0 -- 退货订单标识(0-正向单 1-逆向单)
		and order_channel_code not in (4,6) -- 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
	group by 
		substr(sdt,1,6),performance_province_name,customer_code,business_type_name
	) a
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) b on b.customer_code=a.customer_code			
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_q_01

-- 调价返利明细
select
	a.performance_province_name,a.customer_code,b.customer_name,a.smonth,a.business_type_name,a.sale_amt,'调价返利' as refund_type
from
	(
	select
		substr(sdt,1,6) as smonth,performance_province_name,customer_code,business_type_name,
		sum(sale_amt) as sale_amt
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20230101' and sdt<='20230331'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		and business_type_code=4
		-- and refund_order_flag=0 -- 退货订单标识(0-正向单 1-逆向单)
		and order_channel_code in (4,6) -- 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
	group by 
		substr(sdt,1,6),performance_province_name,customer_code,business_type_name
	) a
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) b on b.customer_code=a.customer_code	
;

-- ===============================================================================================================================================================
-- 
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_q_01;
create table csx_analyse_tmp.csx_analyse_tmp_finance_q_01
as
select
	a.performance_province_name,a.customer_code,b.customer_name,a.smonth,a.business_type_name,a.sale_amt,a.smonth as smonth_refund,'退货单' as refund_type,a.sale_amt_refund	
from
	(
	select
		substr(sdt,1,6) as smonth,performance_province_name,customer_code,business_type_name,
		sum(if(refund_order_flag=0,sale_amt,0)) as sale_amt,
		sum(if(refund_order_flag=1,sale_amt,0)) as sale_amt_refund
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20230101' and sdt<='20230331'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		and business_type_code in(1,2,3,5)
		-- and refund_order_flag=0 -- 退货订单标识(0-正向单 1-逆向单)
		and order_channel_code not in (4,6) -- 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
	group by 
		substr(sdt,1,6),performance_province_name,customer_code,business_type_name
	) a
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) b on b.customer_code=a.customer_code			
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_q_01

-- 调价返利明细
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_q_02;
create table csx_analyse_tmp.csx_analyse_tmp_finance_q_02
as
select
	a.performance_province_name,a.customer_code,b.customer_name,a.smonth,a.business_type_name,a.sale_amt,'调价返利' as refund_type
from
	(
	select
		substr(sdt,1,6) as smonth,performance_province_name,customer_code,business_type_name,
		sum(sale_amt) as sale_amt
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20230101' and sdt<='20230331'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		and business_type_code in(1,2,3,5)
		-- and refund_order_flag=0 -- 退货订单标识(0-正向单 1-逆向单)
		and order_channel_code in (4,6) -- 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
	group by 
		substr(sdt,1,6),performance_province_name,customer_code,business_type_name
	) a
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) b on b.customer_code=a.customer_code	
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_q_02


