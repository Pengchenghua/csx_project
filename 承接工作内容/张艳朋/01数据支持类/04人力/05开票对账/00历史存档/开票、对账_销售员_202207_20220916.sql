-- 对账
insert overwrite directory '/tmp/zhangyanpeng/20220610_02' row format delimited fields terminated by '\t'

select
	substr(a.sdt,1,6) as smonth,
	coalesce(b.province_name,'') as province_name,
	a.customer_code,
	coalesce(b.customer_name,'') as customer_name,
	a.company_code,
	a.company_name,
	coalesce(b.work_no_new,'') as work_no_new,
	coalesce(b.sales_name_new,'') as sales_name_new,
	a.statement_amount,--对账金额
	--a.unstatement_amount,--未对账金额
	--a.kp_amount,--开票金额
	a.tax_sale_amount,--财务含税销售额
	a.statement_ratio--对账率
	--a.kp_ratio --开票率	
from
	(
	select
		customer_code,company_code,company_name,
		statement_amount,--对账金额
		kp_amount,--开票金额
		tax_sale_amount,--财务含税销售额
		statement_amount/tax_sale_amount as statement_ratio,
		kp_amount/tax_sale_amount as kp_ratio,
		--statement_ratio,--对账率
		--kp_ratio,--开票率
		sdt
	from
		csx_dw.dws_sss_r_d_customer_settle_detail
	where
		sdt in ('20220710')
	) a 
	join
		(
		select distinct 
			month,customer_no,customer_name,region_name,province_name,city_group_name,
			rp_service_user_work_no_new,rp_service_user_name_new,
			fl_service_user_work_no_new,fl_service_user_name_new,
			bbc_service_user_work_no_new,bbc_service_user_name_new,
			work_no_new,sales_name_new
		from
			csx_tmp.report_crm_w_a_customer_service_manager_info_business_new
		where
			month in ('202207')
			and work_no_new in ('81022821','80955319','81089336','81052035','80890403')
		) b on a.customer_code=b.customer_no
;

-- 开票
			
insert overwrite directory '/tmp/zhangyanpeng/20220610_03' row format delimited fields terminated by '\t'

select
	substr(a.sdt,1,6) as smonth,
	coalesce(b.province_name,'') as province_name,
	a.customer_code,
	coalesce(b.customer_name,'') as customer_name,
	a.company_code,
	a.company_name,
	coalesce(b.work_no_new,'') as work_no_new,
	coalesce(b.sales_name_new,'') as sales_name_new,
	--a.statement_amount,--对账金额
	--a.unstatement_amount,--未对账金额
	a.kp_amount,--开票金额
	a.tax_sale_amount,--财务含税销售额
	--a.statement_ratio,--对账率
	a.kp_ratio --开票率	
from
	(
	select
		customer_code,company_code,company_name,
		statement_amount,--对账金额
		kp_amount,--开票金额
		tax_sale_amount,--财务含税销售额
		statement_amount/tax_sale_amount as statement_ratio,
		kp_amount/tax_sale_amount as kp_ratio,
		--statement_ratio,--对账率
		--kp_ratio,--开票率
		sdt
	from
		csx_dw.dws_sss_r_d_customer_settle_detail
	where
		sdt in ('20220715')
	) a 
	join
		(
		select distinct 
			month,customer_no,customer_name,region_name,province_name,city_group_name,
			rp_service_user_work_no_new,rp_service_user_name_new,
			fl_service_user_work_no_new,fl_service_user_name_new,
			bbc_service_user_work_no_new,bbc_service_user_name_new,
			work_no_new,sales_name_new
		from
			csx_tmp.report_crm_w_a_customer_service_manager_info_business_new
		where
			month in ('202207')
			and work_no_new in ('81022821','80955319','81089336','81052035','80890403')
		) b on a.customer_code=b.customer_no
			