

-- 开票
insert overwrite directory '/tmp/zhangyanpeng/20220610_02' row format delimited fields terminated by '\t'

select
	substr(a.sdt,1,6) as smonth,
	b.province_name as province_name,
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
		sdt in ('20221031')
		and tax_sale_amount !=0
	) a 
	join
		(
		select distinct 
			month,customer_no,customer_name,region_name,province_name,city_group_name,
			rp_service_user_work_no_new,rp_service_user_name_new,
			fl_service_user_work_no_new,fl_service_user_name_new,
			bbc_service_user_work_no_new,bbc_service_user_name_new
		from
			csx_tmp.report_crm_w_a_customer_service_manager_info_business_new
		where
			month in ('202210')
			and province_name='重庆市'
			--and (rp_service_user_work_no_new='80955319' or fl_service_user_work_no_new='80955319' or bbc_service_user_work_no_new='80955319')
		) b on a.customer_code=b.customer_no and substr(a.sdt,1,6)=b.month
where
	b.rp_service_user_work_no_new is not null
	or b.fl_service_user_work_no_new is not null
	or b.bbc_service_user_work_no_new is not null
;
			