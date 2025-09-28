-- 退货率-自营
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_12;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_12
as
select
	a.smonth,
	concat(a.performance_province_name,a.performance_city_name) as aa,
	a.performance_province_name,
	a.performance_city_name,
	a.channel_name,
	a.customer_code,
	b.customer_name,
	a.business_type_name,
	a.sale_amt,
	a.sale_amt_refund
from
	(
	select
		substr(sdt,1,6) as smonth,performance_province_name,performance_city_name,channel_name,customer_code,business_type_name,
		sum(sale_amt) as sale_amt,sum(if(refund_order_flag=1,sale_amt,0)) as sale_amt_refund
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20221001' and sdt<='20230430'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		-- and performance_province_name='北京市'
		-- and business_type_code not in (4,6) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
		and performance_province_name !='平台-B'
	group by 
		substr(sdt,1,6),performance_province_name,performance_city_name,channel_name,customer_code,business_type_name
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
		) b on b.customer_code=a.customer_code;
		
select * from csx_analyse_tmp.csx_analyse_tmp_finance_shenji_12		

-- =================================================================================================================================================================================
-- 自提销售占比-日配
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_13;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_13
as
select
	a.smonth,
	concat(a.performance_province_name,a.performance_city_name) as aa,
	a.performance_province_name,
	a.performance_city_name,
	a.channel_name,
	a.customer_code,
	b.customer_name,
	a.business_type_name,
	a.sale_amt,
	a.sale_amt_ziti
from
	(
	select
		substr(sdt,1,6) as smonth,performance_province_name,performance_city_name,channel_name,customer_code,business_type_name,
		sum(sale_amt) as sale_amt,sum(if(delivery_type_code=3,sale_amt,0)) as sale_amt_ziti
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20221001' and sdt<='20230430'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		-- and performance_province_name='北京市'
		and business_type_code in (1) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
		and performance_province_name !='平台-B'
	group by 
		substr(sdt,1,6),performance_province_name,performance_city_name,channel_name,customer_code,business_type_name
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
		) b on b.customer_code=a.customer_code;
		
select * from csx_analyse_tmp.csx_analyse_tmp_finance_shenji_13	

-- =================================================================================================================================================================================
-- 逾期率
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_14;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_14
as
select
	a.smonth,
	concat(a.province_name,a.city_group_name) as aa,
	a.province_name,
	a.city_group_name,
	a.channel_name,
	a.customer_code,
	b.customer_name,
	a.receivable_amount,
	a.overdue_amount
from
	(
	select 
		substr(sdt,1,6) as smonth,province_name,city_group_name,channel_name,customer_code,
		sum(case when receivable_amount>0 then receivable_amount else 0 end) as receivable_amount, -- 应收金额
		sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else 0 end) overdue_amount	-- 逾期金额
	from 
		csx_dws.csx_dws_sss_customer_invoice_bill_settle_stat_di
	where 
		sdt in('20221031','20221130','20221231','20230131','20230228','20230331','20230430')
		and channel_code in ('1','7','9')
		-- and province_name='北京市'
	group by 
		substr(sdt,1,6),province_name,city_group_name,channel_name,customer_code
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
		) b on b.customer_code=a.customer_code;
		
select * from csx_analyse_tmp.csx_analyse_tmp_finance_shenji_14;
