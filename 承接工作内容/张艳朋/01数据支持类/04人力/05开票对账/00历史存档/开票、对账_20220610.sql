
insert overwrite directory '/tmp/zhangyanpeng/20220610_01' row format delimited fields terminated by '\t'

select
	substr(a.sdt,1,6) as smonth,
	coalesce(b.province_name,c.province_name) as province_name,
	a.customer_code,
	coalesce(b.customer_name,c.customer_name) as customer_name,
	a.company_code,
	a.company_name,
	coalesce(b.rp_service_user_work_no_new,c.rp_service_user_work_no,'') as rp_service_user_work_no_new,
	coalesce(b.rp_service_user_name_new,c.rp_service_user_name,'') as rp_service_user_name_new,
	coalesce(b.fl_service_user_work_no_new,c.fl_service_user_work_no,'') as fl_service_user_work_no_new,
	coalesce(b.fl_service_user_name_new,c.fl_service_user_name,'') as fl_service_user_name_new,
	coalesce(b.bbc_service_user_work_no_new,c.bbc_service_user_work_no,'') as bbc_service_user_work_no_new,
	coalesce(b.bbc_service_user_name_new,c.bbc_service_user_name,'') as bbc_service_user_name_new,
	a.statement_amount,--对账金额
	--a.unstatement_amount,--未对账金额
	a.kp_amount,--开票金额
	a.tax_sale_amount,--财务含税销售额
	a.statement_ratio,--对账率
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
		sdt in ('20220331','20220430','20220531')
	) a 
	left join
		(
		select distinct 
			month,customer_no,customer_name,region_name,province_name,city_group_name,
			rp_service_user_work_no_new,rp_service_user_name_new,
			fl_service_user_work_no_new,fl_service_user_name_new,
			bbc_service_user_work_no_new,bbc_service_user_name_new
		from
			csx_tmp.report_crm_w_a_customer_service_manager_info_business_new
		where
			month in ('202204','202205')
		) b on a.customer_code=b.customer_no and substr(a.sdt,1,6)=b.month
	left join
		(
		select distinct
			'202203' as month,customer_no,customer_name,province_name,rp_service_user_work_no,rp_service_user_name,
			fl_service_user_work_no,fl_service_user_name,bbc_service_user_work_no,bbc_service_user_name
		from
			csx_tmp.tc_customer_service_manager_info_new_20220424
		) c on a.customer_code=c.customer_no and substr(a.sdt,1,6)=c.month
			
		
			