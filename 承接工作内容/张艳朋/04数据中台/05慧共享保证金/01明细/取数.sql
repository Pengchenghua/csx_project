select 
	claim_bill_no,
	serial_no,
	remedy_no,
	claim_employee_code,
	claim_employee_name,
	bank_acc,
	acc_name,
	opp_acc_no,
	opp_acc_name,
	amount,
	trans_time,
	claim_time,
	fd_abs,
	posting_time,
	case when data_source=1 then '下发' when data_source=2 then '导入' end as  			data_source_name
from
	csx_ods.csx_ods_csx_b2b_sss_sss_money_back_head_df
where 
	sdt=regexp_replace(to_date(days_sub(now(),1)),'-','');
	
	
	
select 
	claim_bill_no,serial_no,remedy_no,claim_employee_code,claim_employee_name,bank_acc,acc_name,
	opp_acc_no,opp_acc_name,amount,trans_time,claim_time,fd_abs,posting_time,
	case when data_source=1 then '下发' when data_source=2 then '导入' end as data_source_name
from
	csx_analyse.csx_analyse_fr_sss_money_back_head_df
where 
	-- sdt=regexp_replace(to_date(days_sub(now(),1)),'-','') 
	sdt='20220828'  limit 10
