-- 对账
-- 对账
select
	substr(a.sdt,1,6) as smonth,
	b.province_name as province_name,
	city_group_name,
	a.customer_code,
	b.customer_name customer_name,
	a.company_code,
	a.company_name,
	coalesce(b.rp_service_user_work_no_new,'') as rp_service_user_work_no_new,
	coalesce(b.rp_service_user_name_new,'') as rp_service_user_name_new,
	coalesce(b.fl_service_user_work_no_new,'') as fl_service_user_work_no_new,
	coalesce(b.fl_service_user_name_new,'') as fl_service_user_name_new,
	coalesce(b.bbc_service_user_work_no_new,'') as bbc_service_user_work_no_new,
	coalesce(b.bbc_service_user_name_new,'') as bbc_service_user_name_new,
	a.bill_amt,--对账金额
	--a.unstatement_amount,--未对账金额
	-- a.invoice_amount,--开票金额
	a.sale_amt,--财务含税销售额
	a.statement_ratio--对账率
	-- a.kp_ratio --开票率	
from
	(
	select
		customer_code,company_code,company_name,
		bill_amt,--对账金额
		invoice_amount,--开票金额
		sale_amt,--财务含税销售额
		bill_amt/sale_amt as statement_ratio,
		invoice_amount/sale_amt as kp_ratio,
		-- statement_ratio,--对账率
		-- kp_ratio,--开票率
		sdt
	from
		-- csx_dw.dws_sss_r_d_customer_settle_detail
		csx_dws.csx_dws_sss_customer_invoice_bill_settle_stat_di
	where
		sdt in ('20240110')
	) a 
	join
		(
		select distinct 
			sdt,customer_no,customer_name,region_name,province_name,city_group_name,
			rp_service_user_work_no_new,rp_service_user_name_new,
			fl_service_user_work_no_new,fl_service_user_name_new,
			bbc_service_user_work_no_new,bbc_service_user_name_new
		from
			-- csx_tmp.report_crm_w_a_customer_service_manager_info_business_new
			csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
		where
			sdt in ('20240131')
			--and (rp_service_user_work_no_new='80955319' or fl_service_user_work_no_new='80955319' or bbc_service_user_work_no_new='80955319')
		) b on a.customer_code=b.customer_no
where
	(b.rp_service_user_work_no_new is not null
	or b.fl_service_user_work_no_new is not null
	or b.bbc_service_user_work_no_new is not null)
	and sale_amt !=0
;

-- 开票
select
	substr(a.sdt,1,6) as smonth,
	b.province_name as province_name,
	city_group_name,
	a.customer_code,
	b.customer_name customer_name,
	a.company_code,
	a.company_name,
	coalesce(b.rp_service_user_work_no_new,'') as rp_service_user_work_no_new,
	coalesce(b.rp_service_user_name_new,'') as rp_service_user_name_new,
	coalesce(b.fl_service_user_work_no_new,'') as fl_service_user_work_no_new,
	coalesce(b.fl_service_user_name_new,'') as fl_service_user_name_new,
	coalesce(b.bbc_service_user_work_no_new,'') as bbc_service_user_work_no_new,
	coalesce(b.bbc_service_user_name_new,'') as bbc_service_user_name_new,
	-- a.bill_amt,--对账金额
	-- a.unstatement_amount,--未对账金额
	a.invoice_amount,--开票金额
	a.sale_amt,--财务含税销售额
	-- a.statement_ratio--对账率
	a.kp_ratio --开票率	
from
	(
	select
		customer_code,company_code,company_name,
		bill_amt,--对账金额
		invoice_amount,--开票金额
		sale_amt,--财务含税销售额
		bill_amt/sale_amt as statement_ratio,
		invoice_amount/sale_amt as kp_ratio,
		-- statement_ratio,--对账率
		-- kp_ratio,--开票率
		sdt
	from
		csx_dws.csx_dws_sss_customer_invoice_bill_settle_stat_di
	where
		sdt in ('20240115')
	) a 
	join
		(
		select distinct 
			sdt,customer_no,customer_name,region_name,province_name,city_group_name,
			rp_service_user_work_no_new,rp_service_user_name_new,
			fl_service_user_work_no_new,fl_service_user_name_new,
			bbc_service_user_work_no_new,bbc_service_user_name_new
		from
			csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
		where
			sdt in ('20240131')
			--and (rp_service_user_work_no_new='80955319' or fl_service_user_work_no_new='80955319' or bbc_service_user_work_no_new='80955319')
		) b on a.customer_code=b.customer_no
where
	(b.rp_service_user_work_no_new is not null
	or b.fl_service_user_work_no_new is not null
	or b.bbc_service_user_work_no_new is not null)
	and sale_amt !=0
;
			