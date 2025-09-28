select
	region_code,region_name,province_code,province_name,city_group_code,city_group_name,customer_id,customer_no,customer_name,work_no,sales_name,
	sign_date,normal_first_order_date,estimate_contract_amount,contract_cycle,contract_cycle_type,
	coalesce(estimate_contract_amount/contract_cycle_type,0) as avg_amount_target,
	coalesce(this_month_sales_value,0) as this_month_sales_value,
	coalesce(this_month_sales_value/(estimate_contract_amount/contract_cycle_type),0) as this_month_achievement,
	coalesce(last_month_sales_value,0) as last_month_sales_value,
	coalesce(total_sales_value,0) as total_sales_value,
	coalesce(total_sales_value/(estimate_contract_amount/contract_cycle_type*2),0) as total_achievement,
	row_number()over(partition by province_name,city_group_name,sales_name order by estimate_contract_amount desc) as rn
from
	(
	select 
		region_code,region_name,province_code,province_name,city_group_code,city_group_name,customer_id,customer_no,customer_name,work_no,sales_name,
		sign_date,normal_first_order_date,estimate_contract_amount,contract_cycle,
		case when contract_cycle like '%个月' then cast(regexp_replace(contract_cycle,'个月','') as int)
			when contract_cycle like '%年' then cast(regexp_replace(contract_cycle,'年','') as int)*12
			when contract_cycle ='30日' or contract_cycle ='31日' then 1
			when contract_cycle ='365日' then 12
			else 0 end as contract_cycle_type,
		this_month_sales_value/10000 as this_month_sales_value,
		last_month_sales_value/10000 as last_month_sales_value,
		total_sales_value/10000 as total_sales_value,
		csp_rate,sdt
	from 
		csx_tmp.report_sale_r_a_customer_normal_performance
	where
		sdt='${SDATE}'
		and province_name not like '%平台%'
		and (csp_rate is null or csp_rate<=0.4)
		--${if(len(sq)==0,"","AND province_name in( '"+sq+"') ")}
	) a 
;

select
	performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name,
	customer_id,customer_code,customer_name,sales_user_number,sales_user_name,
	sign_date,first_business_sale_date,estimate_contract_amount,contract_cycle,contract_cycle_type,
	if(coalesce(contract_cycle_type,0)=0,0,coalesce(estimate_contract_amount/contract_cycle_type,0)) as avg_amount_target,
	coalesce(this_month_sale_amt,0) as this_month_sale_amt,
	if(coalesce(contract_cycle_type,0)=0,0,coalesce(this_month_sale_amt/(estimate_contract_amount/contract_cycle_type),0)) as this_month_achievement,
	coalesce(last_month_sale_amt,0) as last_month_sale_amt,
	coalesce(total_sale_amt,0) as total_sale_amt,
	if(coalesce(contract_cycle_type,0)=0,0,coalesce(total_sale_amt/(estimate_contract_amount/contract_cycle_type*2),0)) as total_achievement,
	row_number()over(partition by performance_province_name,performance_city_name,sales_user_name order by estimate_contract_amount desc) as rn
from
	(
	select 
		performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name,
		customer_id,customer_code,customer_name,sales_user_number,sales_user_name,
		sign_date,first_business_sale_date,estimate_contract_amount,contract_cycle,
		case when contract_cycle like '%个月' then cast(regexp_replace(contract_cycle,'个月','') as int)
			when contract_cycle like '%年' then cast(regexp_replace(contract_cycle,'年','') as int)*12
			when contract_cycle ='30日' or contract_cycle ='31日' then 1
			when contract_cycle ='365日' then 12
			else 0 end as contract_cycle_type,
		this_month_sale_amt/10000 as this_month_sale_amt,
		last_month_sale_amt/10000 as last_month_sale_amt,
		total_sale_amt/10000 as total_sale_amt,
		csp_rate,sdt
	from 
		csx_analyse.csx_analyse_fr_sale_customer_normal_performance_di
	where
		sdt='${SDATE}'
		and performance_province_name not like '%平台%'
		and (csp_rate is null or csp_rate<=0.4)
		${if(len(sq)==0,"","AND performance_province_name in( '"+sq+"') ")}
	) a 
	
;



select
	performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name,
	customer_id,customer_code,customer_name,sales_user_number,sales_user_name,
	sign_date,first_business_sale_date,estimate_contract_amount,contract_cycle,contract_cycle_type,
	if(coalesce(contract_cycle_type,0)=0,0,coalesce(estimate_contract_amount/contract_cycle_type,0)) as avg_amount_target,
	coalesce(this_month_sale_amt,0) as this_month_sale_amt,
	if(coalesce(contract_cycle_type,0)=0 or coalesce(estimate_contract_amount,0)=0,0,coalesce(this_month_sale_amt/(estimate_contract_amount/contract_cycle_type),0)) as this_month_achievement,
	coalesce(last_month_sale_amt,0) as last_month_sale_amt,
	coalesce(total_sale_amt,0) as total_sale_amt,
	if(coalesce(contract_cycle_type,0)=0 or coalesce(estimate_contract_amount,0)=0,0,coalesce(total_sale_amt/(estimate_contract_amount/contract_cycle_type*2),0)) as total_achievement,
	row_number()over(partition by performance_province_name,performance_city_name,sales_user_name order by estimate_contract_amount desc) as rn
from
	(
	select 
		performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name,
		customer_id,customer_code,customer_name,sales_user_number,sales_user_name,
		sign_date,first_business_sale_date,estimate_contract_amount,contract_cycle,
		case when contract_cycle like '%个月' then cast(regexp_replace(contract_cycle,'个月','') as int)
			when contract_cycle like '%年' then cast(regexp_replace(contract_cycle,'年','') as int)*12
			when contract_cycle ='30日' or contract_cycle ='31日' then 1
			when contract_cycle ='365日' then 12
			else 0 end as contract_cycle_type,
		this_month_sale_amt/10000 as this_month_sale_amt,
		last_month_sale_amt/10000 as last_month_sale_amt,
		total_sale_amt/10000 as total_sale_amt,
		csp_rate,sdt
	from 
		csx_analyse.csx_analyse_fr_sale_customer_normal_performance_di
	where
		sdt='${SDATE}'
		and performance_province_name not like '%平台%'
		and (csp_rate is null or csp_rate<=0.4)
		${if(len(sq)==0,"","AND performance_province_name in( '"+sq+"') ")}
	) a 


	
select
	performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name,
	customer_id,customer_code,customer_name,sales_user_number,sales_user_name,
	sign_date,first_business_sale_date,estimate_contract_amount,contract_cycle,contract_cycle_type,
	if(coalesce(contract_cycle_type,0)=0,0,coalesce(estimate_contract_amount/contract_cycle_type,0)) as avg_amount_target,
	coalesce(this_month_sale_amt,0) as this_month_sale_amt,
	if(coalesce(contract_cycle_type,0)=0 or coalesce(estimate_contract_amount,0)=0,0,coalesce(this_month_sale_amt/(estimate_contract_amount/contract_cycle_type),0)) as this_month_achievement,
	coalesce(last_month_sale_amt,0) as last_month_sale_amt,
	coalesce(total_sale_amt,0) as total_sale_amt,
	if(coalesce(contract_cycle_type,0)=0 or coalesce(estimate_contract_amount,0)=0,0,coalesce(total_sale_amt/(estimate_contract_amount/contract_cycle_type*2),0)) as total_achievement,
	row_number()over(partition by performance_province_name,performance_city_name,sales_user_name order by estimate_contract_amount desc) as rn
from
	(
	select 
		performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name,
		customer_id,customer_code,customer_name,sales_user_number,sales_user_name,
		sign_date,first_business_sale_date,estimate_contract_amount,contract_cycle,
		case when contract_cycle like '%个月' then cast(regexp_replace(contract_cycle,'个月','') as int)
			when contract_cycle like '%年' then cast(regexp_replace(contract_cycle,'年','') as int)*12
			when contract_cycle ='30日' or contract_cycle ='31日' then 1
			when contract_cycle ='365日' then 12
			else 0 end as contract_cycle_type,
		this_month_sale_amt/10000 as this_month_sale_amt,
		last_month_sale_amt/10000 as last_month_sale_amt,
		total_sale_amt/10000 as total_sale_amt,
		csp_rate,sdt
	from 
		csx_analyse.csx_analyse_fr_sale_customer_normal_performance_di
	where
		sdt='${SDATE}'
		and performance_province_name not like '%平台%'
		and (csp_rate is null or csp_rate<=0.4)
		${if(len(sq)==0,"","AND performance_province_name in( '"+sq+"') ")}
	) a 
	
	
	
	
	
select
	performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name,
	customer_id,customer_code,customer_name,sales_user_number,sales_user_name,
	sign_date,first_business_sale_date,estimate_contract_amount,contract_cycle,contract_cycle_type,
	if(coalesce(contract_cycle_type,0)=0,0,coalesce(estimate_contract_amount/contract_cycle_type,0)) as avg_amount_target,
	coalesce(this_month_sale_amt,0) as this_month_sale_amt,
	if(coalesce(contract_cycle_type,0)=0 or coalesce(estimate_contract_amount,0)=0,0,coalesce(this_month_sale_amt/(estimate_contract_amount/contract_cycle_type),0)) as this_month_achievement,
	coalesce(last_month_sale_amt,0) as last_month_sale_amt,
	coalesce(total_sale_amt,0) as total_sale_amt,
	if(coalesce(contract_cycle_type,0)=0 or coalesce(estimate_contract_amount,0)=0,0,coalesce(total_sale_amt/(estimate_contract_amount/contract_cycle_type*2),0)) as total_achievement,
	row_number()over(partition by performance_province_name,performance_city_name,sales_user_name order by estimate_contract_amount desc) as rn
from
	(
	select 
		performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name,
		customer_id,customer_code,customer_name,sales_user_number,sales_user_name,
		sign_date,first_business_sale_date,estimate_contract_amount,contract_cycle,
		if(contract_cycle="0",0,cast(contract_cycle as int)) as contract_cycle_type,
		this_month_sale_amt/10000 as this_month_sale_amt,
		last_month_sale_amt/10000 as last_month_sale_amt,
		total_sale_amt/10000 as total_sale_amt,
		csp_rate,sdt
	from 
		csx_analyse.csx_analyse_fr_sale_customer_normal_performance_di
	where
		sdt='${SDATE}'
		and performance_province_name not like '%平台%'
		and (csp_rate is null or csp_rate<=0.4)
		${if(len(sq)==0,"","AND performance_province_name in( '"+sq+"') ")}
	) a 
