select
	a.sales_province_name,
	--a.city_group_name,
	a.sales_city_name,
	a.b_type,
	count(distinct a.business_number) as cnt,
	sum(estimate_contract_amount) as estimate_contract_amount
from
	(
	select 
		id,business_number,customer_name,channel_code,channel_name,sales_province_name,sales_city_name,city_group_name,cooperation_mode_name,
		customer_nature_name,business_type_name,dev_source_name,estimate_contract_amount,business_stage,status,
		case when attribute=1 then '日配'
			when attribute=2 then '福利'
			when attribute=3 then '大宗贸易'
			when attribute=4 then 'M端'
			when attribute=5 then 'BBC'
			when attribute=6 then '内购'
		else '其他' end as b_type
	from
		csx_dw.dws_crm_w_a_business_customer 
	where 
		sdt='current'
		and status='1'
		and business_stage=5
		and attribute in (1,2,5)
		and sales_province_name not like '%平台%'
	) a 
	join
		(
		select
			business_number,create_time
		from
			csx_ods.source_crm_r_d_operate_log
		where
			--sdt='20210927'
			after_data=5
			and to_date(create_time) between '2021-09-20' and '2021-09-26'
		group by 
			business_number,create_time
		) b on b.business_number=a.business_number
group by 
	a.sales_province_name,
	--a.city_group_name,
	a.sales_city_name,
	a.b_type