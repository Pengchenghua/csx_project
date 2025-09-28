-- 对账
select
	substr(a.sdt,1,6) as smonth,
	b.province_name as province_name,
	a.customer_code,
	b.customer_name customer_name,
	a.company_code,
	a.company_name,
	b.work_no,
	b.sales_name,
	a.bill_amt_all,--对账金额
	--a.unstatement_amount,--未对账金额
	-- a.invoice_amount_all,--开票金额
	a.sale_amt_all,--财务含税销售额
	a.statement_ratio--对账率
	-- a.kp_ratio --开票率	
from
	(
	select
		customer_code,company_code,company_name,
		bill_amt_all,--对账金额
		invoice_amount_all,--开票金额
		sale_amt_all,--财务含税销售额
		bill_amt_all/sale_amt_all as statement_ratio,
		invoice_amount_all/sale_amt_all as kp_ratio,
		-- statement_ratio,--对账率
		-- kp_ratio,--开票率
		sdt
	from
		-- csx_dw.dws_sss_r_d_customer_settle_detail
		csx_dws.csx_dws_sss_customer_invoice_bill_settle_stat_di
	where
		sdt in ('20221017')
	) a 
	join
		(
		select distinct 
			sdt,customer_no,customer_name,region_name,province_name,city_group_name,
			rp_service_user_work_no_new,rp_service_user_name_new,
			fl_service_user_work_no_new,fl_service_user_name_new,
			bbc_service_user_work_no_new,bbc_service_user_name_new,
			work_no,sales_name
		from
			-- csx_tmp.report_crm_w_a_customer_service_manager_info_business_new
			csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
		where
			sdt in ('20221031')
			and work_no='80894243'
		) b on a.customer_code=b.customer_no
where
	sale_amt_all !=0
;

-- 开票
select
	substr(a.sdt,1,6) as smonth,
	b.province_name as province_name,
	a.customer_code,
	b.customer_name customer_name,
	a.company_code,
	a.company_name,
	b.work_no,
	b.sales_name,
	-- a.bill_amt_all,--对账金额
	-- a.unstatement_amount,--未对账金额
	a.invoice_amount_all,--开票金额
	a.sale_amt_all,--财务含税销售额
	-- a.statement_ratio--对账率
	a.kp_ratio --开票率	
from
	(
	select
		customer_code,company_code,company_name,
		bill_amt_all,--对账金额
		invoice_amount_all,--开票金额
		sale_amt_all,--财务含税销售额
		bill_amt_all/sale_amt_all as statement_ratio,
		invoice_amount_all/sale_amt_all as kp_ratio,
		-- statement_ratio,--对账率
		-- kp_ratio,--开票率
		sdt
	from
		-- csx_dw.dws_sss_r_d_customer_settle_detail
		csx_dws.csx_dws_sss_customer_invoice_bill_settle_stat_di
	where
		sdt in ('20221022')
	) a 
	join
		(
		select distinct 
			sdt,customer_no,customer_name,region_name,province_name,city_group_name,
			rp_service_user_work_no_new,rp_service_user_name_new,
			fl_service_user_work_no_new,fl_service_user_name_new,
			bbc_service_user_work_no_new,bbc_service_user_name_new,
			work_no,sales_name
		from
			csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
		where
			sdt in ('20221031')
			and work_no='80894243'
		) b on a.customer_code=b.customer_no
where
	sale_amt_all !=0
;
			