--==================================================================================================================================================================================
-- 个人奖项_日配新客激励方案_新签贡献奖
select
	performance_region_name,performance_province_name,performance_city_name,owner_user_number,owner_user_name,
	owner_user_total_amt,owner_user_business_cnt,amount_flag,amount_flag_code,
	count(business_number) as business_cnt,
	count(case when business_flag='新客' then business_number end) as new_business_cnt,
	count(case when business_flag='老客' then business_number end) as old_business_cnt
from
	(
	select
		*,sum(amount)over(partition by owner_user_number) as owner_user_total_amt,
		case when amount<=30 then '0-30'
			when amount>30 and amount<=60 then '30-60'
			when amount>60 and amount<=200 then '60-200'
			when amount>200 and amount<=600 then '200-600'
			when amount>600 and amount<=1000 then '600-1000'
			when amount>1000 then '1000+'
		end as amount_flag,
		case when amount<=30 then 0
			when amount>30 and amount<=60 then 1
			when amount>60 and amount<=200 then 2
			when amount>200 and amount<=600 then 3
			when amount>600 and amount<=1000 then 4
			when amount>1000 then 5
		end as amount_flag_code,
		count(case when amount>30 then business_number end)over(partition by owner_user_number) as owner_user_business_cnt
	from
		(
		select
			*,if(contract_months_cnt<=12,estimate_contract_amount,estimate_contract_amount/(contract_months_cnt/12)) as amount
		from
			(
			select
				performance_region_name,performance_province_name,performance_city_name,business_number,customer_id,customer_code,customer_name,owner_user_number,owner_user_name,guide_user_name,
				regexp_replace(to_date(business_sign_time),'-','') as business_sign_date,
				regexp_replace(to_date(first_business_sign_time),'-','') as first_business_sign_date,
				cast(estimate_contract_amount as double) as estimate_contract_amount,contract_cycle,
				case when contract_cycle like '%月' then cast(regexp_replace(contract_cycle,'月','') as int)
					when contract_cycle like '%年' then cast(regexp_replace(contract_cycle,'年','') as int)*12
					else cast(contract_cycle as int)
				end as contract_months_cnt,
				if(to_date(business_sign_time)=to_date(first_business_sign_time),'新客','老客') as business_flag
				
			from
				csx_dim.csx_dim_crm_business_info
			where
				sdt='current'
				and business_attribute_code=1 -- 1：日配客户 2：福利客户 3：大宗贸易 4：m端 5：bbc 6：内购
				and status=1 -- 是否有效 0无效 1有效
				and approval_status_code=2 -- 审批状态编码 0:待发起 1：审批中 2：审批完成 3：审批拒绝
				and business_stage=5 -- 阶段状态 1.阶段1 2.阶段2 3.阶段3 4.阶段4 5.阶段5
				and to_date(business_sign_time) between '2022-04-01' and '2022-06-30'
			) a 
		where
			1=1
			-- contract_months_cnt>=6
		) a 
	) a 
where
	(owner_user_total_amt>=200 or owner_user_business_cnt>3)
	and amount_flag_code>0 -- 小于等于30万的不统计
group by 
	performance_region_name,performance_province_name,performance_city_name,owner_user_number,owner_user_name,owner_user_total_amt,owner_user_business_cnt,amount_flag,amount_flag_code
order by 
	performance_region_name,performance_province_name,performance_city_name,owner_user_number,owner_user_name,amount_flag_code

;