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
				and to_date(business_sign_time) between '2022-10-01' and '2022-12-31'
			) a 
		where
			contract_months_cnt>=6
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

--===================================================================================================================================================================
-- 个人奖项_日配新客激励方案_业绩贡献奖

select
	performance_region_name,performance_province_name,performance_city_name,sales_user_number,sales_user_name,
	sum(sale_amt) as sale_amt,sum(profit) as profit,sum(profit)/abs(sum(sale_amt)) as profit_rate,row_number()over(order by sum(sale_amt) desc) as rn
from
	(
	select
		performance_region_name,performance_province_name,performance_city_name,customer_code,customer_name,sales_user_number,sales_user_name,
		business_attribute_code,first_business_sale_date
	from
		csx_dws.csx_dws_crm_customer_business_active_di
	where
		sdt=regexp_replace(to_date(date_sub(current_date(),1)),'-','')
		and business_type_code=1
		and first_business_sale_date between '20221001' and '20221231'
	) a 
	join
		(
		select 
			customer_code,
			sum(sale_amt)as sale_amt,
			sum(profit) as profit
		from 
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt between '20221001' and '20221231'
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
			and business_type_code in (1) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		group by 
			customer_code
		having
			if(sum(sale_amt)=0,0,sum(profit)/abs(sum(sale_amt)))>=0.08
		) b on b.customer_code=a.customer_code
group by 
	performance_region_name,performance_province_name,performance_city_name,sales_user_number,sales_user_name
;

-- 个人奖项_日配新客激励方案_业绩贡献奖 明细

select
	performance_region_name,performance_province_name,performance_city_name,sales_user_number,sales_user_name,
	a.customer_code,a.customer_name,a.first_business_sale_date,b.sale_amt,b.profit,b.profit_rate,row_number()over(order by b.sale_amt desc) as rn
from
	(
	select
		performance_region_name,performance_province_name,performance_city_name,customer_code,customer_name,sales_user_number,sales_user_name,
		business_attribute_code,first_business_sale_date
	from
		csx_dws.csx_dws_crm_customer_business_active_di
	where
		sdt=regexp_replace(to_date(date_sub(current_date(),1)),'-','')
		and business_type_code=1
		and first_business_sale_date between '20221001' and '20221231'
	) a 
	join
		(
		select 
			customer_code,
			sum(sale_amt)as sale_amt,
			sum(profit) as profit,
			sum(profit)/abs(sum(sale_amt)) as profit_rate
		from 
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt between '20221001' and '20221231'
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
			and business_type_code in (1) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		group by 
			customer_code
		having
			if(sum(sale_amt)=0,0,sum(profit)/abs(sum(sale_amt)))>=0.08
		) b on b.customer_code=a.customer_code
;


-- 查数
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
				and to_date(business_sign_time) between '2022-10-01' and '2022-12-31'
				and customer_code in ('130087','130369','130625','130145')
			) a 
		where
			1=1
			-- contract_months_cnt>=6
		) a 

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
				-- and status=1 -- 是否有效 0无效 1有效
				-- and approval_status_code=2 -- 审批状态编码 0:待发起 1：审批中 2：审批完成 3：审批拒绝
				-- and business_stage=5 -- 阶段状态 1.阶段1 2.阶段2 3.阶段3 4.阶段4 5.阶段5
				-- and to_date(business_sign_time) between '2022-10-01' and '2022-12-31'
				and customer_code in ('130087')
			) a 
		where
			1=1
			-- contract_months_cnt>=6
		) a 		
		
			select
				performance_region_name,performance_province_name,performance_city_name,business_number,customer_id,customer_code,customer_name,owner_user_number,owner_user_name,guide_user_name,
				regexp_replace(to_date(business_sign_time),'-','') as business_sign_date,
				regexp_replace(to_date(first_business_sign_time),'-','') as first_business_sign_date,
				cast(estimate_contract_amount as double) as estimate_contract_amount,contract_cycle,
				case when contract_cycle like '%月' then cast(regexp_replace(contract_cycle,'月','') as int)
					when contract_cycle like '%年' then cast(regexp_replace(contract_cycle,'年','') as int)*12
					else cast(contract_cycle as int)
				end as contract_months_cnt,
				if(to_date(business_sign_time)=to_date(first_business_sign_time),'新客','老客') as business_flag,
				business_attribute_name				
			from
				csx_dim.csx_dim_crm_business_info
			where
				sdt='current'
				-- and business_attribute_code=1 -- 1：日配客户 2：福利客户 3：大宗贸易 4：m端 5：bbc 6：内购
				and status=1 -- 是否有效 0无效 1有效
				and approval_status_code=2 -- 审批状态编码 0:待发起 1：审批中 2：审批完成 3：审批拒绝
				and business_stage=5 -- 阶段状态 1.阶段1 2.阶段2 3.阶段3 4.阶段4 5.阶段5
				and to_date(business_sign_time) between '2022-10-01' and '2022-12-31'
				and customer_code in ('130087','130369','130625','130145')
