
select 
	c.performance_region_name,
	c.performance_province_name,
	c.performance_city_name,
	a.customer_code,
	c.customer_name,
	c.sales_user_name,
	c.first_category_name,
	c.second_category_name,
	c.third_category_name,
	a.business_attribute_name,
	a.contract_number,
	a.contract_type,-- 正式合同or临时合同
	a.contract_cycle,-- 合同周期
	a.estimate_contract_amount,-- 合同签约金额
	a.contract_begin_date,
	a.contract_end_date,
	b.fixed_begin_date,
	b.fixed_end_date,
	c.cooperation_mode_name,-- 合作模式
	c.dev_source_name,-- 开发来源
	c.customer_address_full -- 客户详细地址
from 
	(
	select 
		*
	from 
		(
		select 
			substr(business_sign_time,1,7) month, 
		    customer_code,
			customer_name,
			business_number,
			performance_region_code,
			performance_region_name,
			performance_province_code,
			performance_province_name,
			performance_city_code,
			performance_city_name,
			first_category_code,
			first_category_name,
			second_category_code,
			second_category_name,
			third_category_code,
			third_category_name,
			business_attribute_code,
			business_attribute_name,
			estimate_contract_amount,
			to_date(first_sign_time) first_sign_date,
			case when substr(first_sign_time,1,7) = substr(business_sign_time,1,7) then '新签约客户' else '老签约客户' end as new_or_old_customer_mark,
			regexp_replace(to_date(business_sign_time),'-','') business_sign_date,
			to_date(first_business_sign_time) first_business_sign_date,
			contract_number,  -- 合同编号
			contract_cycle,-- 合同周期
			(case when contract_type=1 then '临时合同' 
				  when contract_type=2 then '正式合同' 
			end) as contract_type,  -- 合同类型(1临时合同 2正式合同)
			contract_must,  -- 是否需签订合同 0否 1是
			to_date(contract_begin_date) as contract_begin_date,  -- 合同起始日期
			to_date(contract_end_date) as contract_end_date,  -- 合同终止日期
			estimate_contract_amount,
			regexp_replace(to_date(contract_begin_date),'-','') as contract_begin_sdt,
			row_number() over(partition by customer_code,business_attribute_code order by contract_end_date desc)	as num
		from csx_dim.csx_dim_crm_business_info
		where sdt='current' 
		    and business_stage = 5 
			and status='1'
			and shipper_code='YHCSX'
		)a
	where a.num=1
	) a 
	-- 客户信控额度明细
	left join  
	(
	  select *
	  from 
			(
			  select *,
			  to_date(fixed_begin_time) as fixed_begin_date,
			  to_date(fixed_end_time) as fixed_end_date,
			  concat(to_date(fixed_begin_time),'~',to_date(fixed_end_time)) as fixed_begin_end, -- 固定额度起止时间 
			  concat(to_date(temp_begin_time),'~',to_date(temp_end_time)) as temp_begin_end, -- 临时额度起止时间
			  row_number() over(partition by customer_code,business_attribute_code order by if(fixed_end_time>temp_end_time,fixed_end_time,temp_end_time) desc)	as num 
			  from csx_dim.csx_dim_crm_customer_company_details
			  where sdt='current'
			  and shipper_code='YHCSX'
			  and status=1  -- 状态 0.无效 1.有效
			  and business_attribute_code=1
			  and (credit_limit>0 or temp_credit_limit>0)
			)a 
		where num=1
	) b  
	on a.customer_code=b.customer_code and a.business_attribute_code=b.business_attribute_code 
	left join 
	(select 
		* 
	from csx_dim.csx_dim_crm_customer_info 
	where sdt='current' 
	and customer_type_code=4 
	) c 
	on a.customer_code=c.customer_code 
where c.customer_code is not null 
-- 近2个月断约客户
and 
(
	(a.contract_type='正式合同' and a.contract_end_date>=date_add('${yes_date}',1) and a.contract_end_date<=date_add('${yes_date}',60))
	or 
	(a.contract_type='临时合同' and b.fixed_end_date>=date_add('${yes_date}',1) and b.fixed_end_date<=date_add('${yes_date}',60))
)