--1、履约客户数、新签客户数目标 对调位置
--2、新增：新签商机数、新签商机金额
--3、商机数：只取50% 和 75% 金额*对应的阶段数

--=============================================================================================================================================================================
--销售员
set current_start_day ='20211201';

set current_end_day ='20211214';

insert overwrite directory '/tmp/zhangyanpeng/20211110_rank_sales' row format delimited fields terminated by '\t'
			
select
	c.province_name,
	c.city_group_name,
	a.first_supervisor_name,
	a.work_no,
	a.sales_name,
	coalesce(e.begin_date,'') as begin_date,
	coalesce(b.status,'') as status,
	'' as sales_target,
	coalesce(f.sales_value,0) as sales_value,
	'' as sales_target_achievement_rate,
	'' as profit_rate_target,
	coalesce(f.profit_rate,0) as profit_rate,
	'' as profit_rate_achievement,
	coalesce(f.performance_customers,0) as performance_customers,
	'' as new_sign_customers_target,
	coalesce(g.new_sign_customers,0) as new_sign_customers,
	coalesce(g.new_sign_amount,0) as new_sign_amount,
	coalesce(i.new_sign_business,0) as new_sign_business,
	coalesce(i.new_sign_business_amount,0) as new_sign_business_amount,
	'' as new_sign_customers_achievement_rate,
	coalesce(h.business_number_cnt,0) as business_number_cnt,
	coalesce(h.estimate_contract_amount,0) as estimate_contract_amount,
	row_number()over(partition by c.province_name,c.city_group_name order by f.sales_value desc) as rn
from
	(
	select --销售员信息
		sales_id,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,second_supervisor_work_no,second_supervisor_name
	from
		csx_dw.dws_crm_w_a_customer
	where 
		sdt = ${hiveconf:current_end_day}
		--and sales_position='SALES'
	group by 
		sales_id,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,second_supervisor_work_no,second_supervisor_name
	) a 
	left join -- 用户表，获取城市信息
		(
		select
			id,user_number,name,user_position,city_name,prov_name,if(status=0,'启用','禁用') as status
		from
			csx_dw.dws_basic_w_a_user
		where
			sdt=${hiveconf:current_end_day}
			and del_flag = '0'
		group by 
			id,user_number,name,user_position,city_name,prov_name,if(status=0,'启用','禁用')
		) b on b.id=a.sales_id
	left join -- 区域表
		( 
		select
			city_code,city_name,area_province_code,area_province_name,city_group_code,city_group_name,province_code,province_name,region_code,region_name
		from
			csx_dw.dws_sale_w_a_area_belong
		) c on c.city_name=b.city_name and c.area_province_name=b.prov_name
	left join --兼岗信息
		(
		select
			user_id,user_position
		from
			csx_ods.source_uc_w_a_user_position 
		where
			sdt=${hiveconf:current_end_day}
		group by 
			user_id,user_position
		) d on d.user_id=a.sales_id			
	left join -- 入职信息
		(
		select 
			employee_code,employee_name,begin_date,leader_code,leader_name,emp_status
		from 
			csx_dw.dws_basic_w_a_employee_org_m
		where 
			sdt =${hiveconf:current_end_day}
		) e on e.employee_code=a.work_no
	left join --销售额 履约客户数
		(	
		select 
			t2.sales_id,
			sum(t1.sales_value) as sales_value,
			sum(t1.profit) as profit,
			sum(t1.profit)/abs(sum(t1.sales_value)) as profit_rate,
			count(distinct t1.customer_no) as performance_customers
		from
			(
			select
				customer_no,business_type_name,substr(sdt,1,6) as smonth,sales_value,profit
			from
				csx_dw.dws_sale_r_d_detail
			where 
				sdt>=${hiveconf:current_start_day} and sdt<=${hiveconf:current_end_day}
				and channel_code in('1','7','9')
				and business_type_code !='4'
			) t1 
			left join
				(
				select
					customer_id,customer_no,customer_name,sales_id,work_no,sales_name,first_supervisor_code,first_supervisor_work_no,first_supervisor_name
				from
					csx_dw.dws_crm_w_a_customer
				where
					sdt = ${hiveconf:current_end_day}
				) t2 on t2.customer_no=t1.customer_no					
		group by 
			t2.sales_id
		) f on f.sales_id=a.sales_id		
	left join -- 新签客户
		( 
		select
			sales_id,
			count(customer_no) as new_sign_customers,
			sum(estimate_contract_amount) as new_sign_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = ${hiveconf:current_end_day}
			and regexp_replace(substr(sign_time,1,10),'-','') between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
		group by 
			sales_id
		) g on g.sales_id=a.sales_id			
	left join -- 商机数量及金额
		( 
		select 
			sales_id,
			count(distinct business_number) as business_number_cnt,
			--count(distinct case when business_stage=5 then business_number else null end)/count(distinct business_number) as rate,
			sum(case when business_stage=3 then estimate_contract_amount*0.5 when business_stage=4 then estimate_contract_amount*0.75 else null end) as estimate_contract_amount		
		from 
			csx_dw.ads_crm_r_m_business_customer
		where 
			month = substr(${hiveconf:current_end_day},1,6)
			and status=1
			and business_stage in ('3','4')
		group by 
			sales_id
		) h on h.sales_id=a.sales_id			
	left join --新签商机数及金额
		(
		select
			a.sales_id,
			count(distinct a.business_number) as new_sign_business,
			sum(estimate_contract_amount) as new_sign_business_amount
		from
			(
			select 
				customer_id,business_number,customer_name,sales_id,
				estimate_contract_amount,business_stage,status
			from
				csx_dw.ads_crm_r_m_business_customer
			where 
				month = substr(${hiveconf:current_end_day},1,6)
				and status='1'
				and business_stage=5
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
					and regexp_replace(substr(create_time,1,10),'-','') between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
				group by 
					business_number,create_time
				) b on b.business_number=a.business_number
		group by 
			a.sales_id
		) i on i.sales_id=a.sales_id			
where
	c.province_name is not null
	and b.user_position='SALES'
	and (d.user_position is null or d.user_position !='CUSTOMER_SERVICE_MANAGER')
;
--=============================================================================================================================================================================
--销售主管
--set current_start_day ='20211001';

--set current_end_day ='20211031';

insert overwrite directory '/tmp/zhangyanpeng/20211110_rank_sales_manager' row format delimited fields terminated by '\t'
			
select
	c.province_name,
	c.city_group_name,
	j.third_supervisor_name,
	a.first_supervisor_work_no,
	a.first_supervisor_name,
	coalesce(e.begin_date,'') as begin_date,
	a.business_type,
	coalesce(b.status,'') as status,
	'' as sales_target,
	coalesce(f.sales_value,0) as sales_value,
	'' as sales_target_achievement_rate,
	'' as profit_rate_target,
	coalesce(f.profit_rate,0) as profit_rate,
	'' as profit_rate_achievement,
	coalesce(f.performance_customers,0) as performance_customers,
	'' as new_sign_customers_target,
	coalesce(g.new_sign_customers,0) as new_sign_customers,
	coalesce(g.new_sign_amount,0) as new_sign_amount,
	coalesce(i.new_sign_business,0) as new_sign_business,
	coalesce(i.new_sign_business_amount,0) as new_sign_business_amount,
	'' as new_sign_customers_achievement_rate,
	coalesce(h.business_number_cnt,0) as business_number_cnt,
	coalesce(h.estimate_contract_amount,0) as estimate_contract_amount,
	row_number()over(partition by c.province_name,c.city_group_name,a.first_supervisor_work_no order by f.sales_value desc) as rn
from
	(
	select --人员信息
		first_supervisor_code,first_supervisor_work_no,first_supervisor_name,
		if(work_no=first_supervisor_work_no,'自有','团队') as business_type
	from
		csx_dw.dws_crm_w_a_customer
	where 
		sdt = ${hiveconf:current_end_day}
		--and sales_position='SALES'
		and first_supervisor_work_no!=''
	group by 
		first_supervisor_code,first_supervisor_work_no,first_supervisor_name,
		if(work_no=first_supervisor_work_no,'自有','团队')
	) a 
	left join -- 用户表，获取城市信息
		(
		select
			id,user_number,name,user_position,city_name,prov_name,if(status=0,'启用','禁用') as status
		from
			csx_dw.dws_basic_w_a_user
		where
			sdt=${hiveconf:current_end_day}
			and del_flag = '0'
		group by 
			id,user_number,name,user_position,city_name,prov_name,if(status=0,'启用','禁用')
		) b on b.id=a.first_supervisor_code
	left join -- 区域表
		( 
		select
			city_code,city_name,area_province_code,area_province_name,city_group_code,city_group_name,province_code,province_name,region_code,region_name
		from
			csx_dw.dws_sale_w_a_area_belong
		) c on c.city_name=b.city_name and c.area_province_name=b.prov_name
	left join --兼岗信息
		(
		select
			user_id,user_position
		from
			csx_ods.source_uc_w_a_user_position 
		where
			sdt=${hiveconf:current_end_day}
		group by 
			user_id,user_position
		) d on d.user_id=a.first_supervisor_code			
	left join -- 入职信息
		(
		select 
			employee_code,employee_name,begin_date,leader_code,leader_name,emp_status
		from 
			csx_dw.dws_basic_w_a_employee_org_m
		where 
			sdt =${hiveconf:current_end_day}
		) e on e.employee_code=a.first_supervisor_work_no
	left join --销售额 履约客户数
		(	
		select 
			t2.first_supervisor_code,
			if(t2.work_no=t2.first_supervisor_work_no,'自有','团队') as business_type,
			sum(t1.sales_value) as sales_value,
			sum(t1.profit) as profit,
			sum(t1.profit)/abs(sum(t1.sales_value)) as profit_rate,
			count(distinct t1.customer_no) as performance_customers
		from
			(
			select
				customer_no,business_type_name,substr(sdt,1,6) as smonth,sales_value,profit
			from
				csx_dw.dws_sale_r_d_detail
			where 
				sdt>=${hiveconf:current_start_day} and sdt<=${hiveconf:current_end_day}
				and channel_code in('1','7','9')
				and business_type_code !='4'
			) t1 
			left join
				(
				select
					customer_id,customer_no,customer_name,sales_id,work_no,sales_name,first_supervisor_code,first_supervisor_work_no,first_supervisor_name
				from
					csx_dw.dws_crm_w_a_customer
				where
					sdt = ${hiveconf:current_end_day}
					and first_supervisor_code !=''
				) t2 on t2.customer_no=t1.customer_no					
		group by 
			t2.first_supervisor_code,if(t2.work_no=t2.first_supervisor_work_no,'自有','团队')
		) f on f.first_supervisor_code=a.first_supervisor_code and f.business_type=a.business_type		
	left join -- 新签客户
		( 
		select
			first_supervisor_code,
			if(work_no=first_supervisor_work_no,'自有','团队') as business_type,
			count(customer_no) as new_sign_customers,
			sum(estimate_contract_amount) as new_sign_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = ${hiveconf:current_end_day}
			and regexp_replace(substr(sign_time,1,10),'-','') between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
			and first_supervisor_code !=''
		group by 
			first_supervisor_code,
			if(work_no=first_supervisor_work_no,'自有','团队')
		) g on g.first_supervisor_code=a.first_supervisor_code and g.business_type=a.business_type
	left join -- 商机数量及金额
		( 
		select 
			t2.first_supervisor_code,
			if(t2.first_supervisor_work_no=t2.work_no,'自有','团队') as business_type,
			count(distinct t1.business_number) as business_number_cnt,
			sum(case when t1.business_stage=3 then t1.estimate_contract_amount*0.5 when t1.business_stage=4 then t1.estimate_contract_amount*0.75 else null end) as estimate_contract_amount		
		from 
			(
			select
				customer_id,business_number,customer_name,business_stage,estimate_contract_amount
			from
				csx_dw.ads_crm_r_m_business_customer
			where 
				month = substr(${hiveconf:current_end_day},1,6)
				and status=1
				and business_stage in ('3','4')
			) t1 
			join
				(
				select
					customer_id,customer_no,customer_name,sales_id,work_no,sales_name,first_supervisor_code,first_supervisor_work_no,first_supervisor_name
				from
					csx_dw.dws_crm_w_a_customer
				where
					sdt = ${hiveconf:current_end_day}
					and first_supervisor_code !=''
				) t2 on t2.customer_id=t1.customer_id
		group by 
			t2.first_supervisor_code,
			if(t2.first_supervisor_work_no=t2.work_no,'自有','团队')
		) h on h.first_supervisor_code=a.first_supervisor_code and h.business_type=a.business_type			
	left join --新签商机数及金额
		(
		select
			c.first_supervisor_code,
			if(c.first_supervisor_work_no=c.work_no,'自有','团队') as business_type,
			count(distinct a.business_number) as new_sign_business,
			sum(estimate_contract_amount) as new_sign_business_amount
		from
			(
			select 
				customer_id,business_number,customer_name,sales_id,
				estimate_contract_amount,business_stage,status
			from
				csx_dw.ads_crm_r_m_business_customer
			where 
				month = substr(${hiveconf:current_end_day},1,6)
				and status='1'
				and business_stage=5
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
					and regexp_replace(substr(create_time,1,10),'-','') between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
				group by 
					business_number,create_time
				) b on b.business_number=a.business_number
			join
				(
				select
					customer_id,customer_no,customer_name,sales_id,work_no,sales_name,first_supervisor_code,first_supervisor_work_no,first_supervisor_name
				from
					csx_dw.dws_crm_w_a_customer
				where
					sdt = ${hiveconf:current_end_day}
					and first_supervisor_code !=''
				) c on c.customer_id=a.customer_id
		group by 
			c.first_supervisor_code,
			if(c.first_supervisor_work_no=c.work_no,'自有','团队')
		) i on i.first_supervisor_code=a.first_supervisor_code and i.business_type=a.business_type	
	-- 人员信息
	left join
		( 
		select
			sales_name,work_no,position,third_supervisor_code,third_supervisor_work_no,third_supervisor_name
		from
			(
			select
				name as sales_name,
				user_number as work_no,
				user_position as position,
				-- 主管
				first_value(case when leader_user_position = 'SALES_CITY_MANAGER' then leader_id end, true) over(partition by user_number order by distance) as third_supervisor_code,
				first_value(case when leader_user_position = 'SALES_CITY_MANAGER' then leader_name end, true) over(partition by user_number order by distance) as third_supervisor_work_no,
				first_value(case when leader_user_position = 'SALES_CITY_MANAGER' then leader_user_number end, true) over(partition by user_number order by distance) as third_supervisor_name,
				row_number() over(partition by user_number order by distance desc) as rank
			from 
				csx_dw.dwd_uc_w_a_user_adjust
			where 
				sdt = ${hiveconf:current_end_day}
			) tmp 
		where 
			tmp.rank = 1
			and third_supervisor_work_no is not null
		group by 
			sales_name,work_no,position,third_supervisor_code,third_supervisor_work_no,third_supervisor_name
		) j on j.work_no = a.first_supervisor_work_no		
where
	c.province_name is not null
	and b.user_position='SALES_MANAGER'
	--and (d.user_position is null or d.user_position !='CUSTOMER_SERVICE_MANAGER')
;
--=============================================================================================================================================================================
--销售经理
--set current_start_day ='20211001';

--set current_end_day ='20211031';

insert overwrite directory '/tmp/zhangyanpeng/20211110_rank_sales_city_manager' row format delimited fields terminated by '\t'
			
select
	c.province_name,
	c.city_group_name,
	a.second_supervisor_work_no,
	a.second_supervisor_name,
	coalesce(e.begin_date,'') as begin_date,
	a.business_type,
	coalesce(b.status,'') as status,
	'' as sales_target,
	coalesce(f.sales_value,0) as sales_value,
	'' as sales_target_achievement_rate,
	'' as profit_rate_target,
	coalesce(f.profit_rate,0) as profit_rate,
	'' as profit_rate_achievement,
	coalesce(f.performance_customers,0) as performance_customers,
	'' as new_sign_customers_target,
	coalesce(g.new_sign_customers,0) as new_sign_customers,
	coalesce(g.new_sign_amount,0) as new_sign_amount,
	coalesce(i.new_sign_business,0) as new_sign_business,
	coalesce(i.new_sign_business_amount,0) as new_sign_business_amount,
	'' as new_sign_customers_achievement_rate,
	coalesce(h.business_number_cnt,0) as business_number_cnt,
	coalesce(h.estimate_contract_amount,0) as estimate_contract_amount,
	row_number()over(partition by c.province_name,c.city_group_name,a.second_supervisor_work_no order by f.sales_value desc) as rn
from
	(
	select --人员信息
		second_supervisor_code,second_supervisor_work_no,second_supervisor_name,
		if(work_no=second_supervisor_work_no,'自有','团队') as business_type
	from
		csx_dw.dws_crm_w_a_customer
	where 
		sdt = ${hiveconf:current_end_day}
		--and sales_position='SALES'
		and second_supervisor_work_no!=''
	group by 
		second_supervisor_code,second_supervisor_work_no,second_supervisor_name,
		if(work_no=second_supervisor_work_no,'自有','团队')
	) a 
	left join -- 用户表，获取城市信息
		(
		select
			id,user_number,name,user_position,city_name,prov_name,if(status=0,'启用','禁用') as status
		from
			csx_dw.dws_basic_w_a_user
		where
			sdt=${hiveconf:current_end_day}
			and del_flag = '0'
		group by 
			id,user_number,name,user_position,city_name,prov_name,if(status=0,'启用','禁用')
		) b on b.id=a.second_supervisor_code
	left join -- 区域表
		( 
		select
			city_code,city_name,area_province_code,area_province_name,city_group_code,city_group_name,province_code,province_name,region_code,region_name
		from
			csx_dw.dws_sale_w_a_area_belong
		) c on c.city_name=b.city_name and c.area_province_name=b.prov_name
	--left join --兼岗信息
	--	(
	--	select
	--		user_id,user_position
	--	from
	--		csx_ods.source_uc_w_a_user_position 
	--	where
	--		sdt=${hiveconf:current_end_day}
	--	group by 
	--		user_id,user_position
	--	) d on d.user_id=a.second_supervisor_code			
	left join -- 入职信息
		(
		select 
			employee_code,employee_name,begin_date,leader_code,leader_name,emp_status
		from 
			csx_dw.dws_basic_w_a_employee_org_m
		where 
			sdt =${hiveconf:current_end_day}
		) e on e.employee_code=a.second_supervisor_work_no
	left join --销售额 履约客户数
		(	
		select 
			t2.second_supervisor_code,
			t2.business_type,
			sum(t1.sales_value) as sales_value,
			sum(t1.profit) as profit,
			sum(t1.profit)/abs(sum(t1.sales_value)) as profit_rate,
			count(distinct t1.customer_no) as performance_customers
		from
			(
			select
				customer_no,business_type_name,substr(sdt,1,6) as smonth,sales_value,profit
			from
				csx_dw.dws_sale_r_d_detail
			where 
				sdt>=${hiveconf:current_start_day} and sdt<=${hiveconf:current_end_day}
				and channel_code in('1','7','9')
				and business_type_code !='4'
			) t1 
			left join
				(
				select
					customer_id,customer_no,customer_name,sales_id,work_no,sales_name,first_supervisor_code,first_supervisor_work_no,first_supervisor_name,
					second_supervisor_code,second_supervisor_work_no,second_supervisor_name,if(work_no=second_supervisor_work_no,'自有','团队') as business_type
				from
					csx_dw.dws_crm_w_a_customer
				where
					sdt = ${hiveconf:current_end_day}
					and second_supervisor_code !=''
				) t2 on t2.customer_no=t1.customer_no					
		group by 
			t2.second_supervisor_code,t2.business_type
		) f on f.second_supervisor_code=a.second_supervisor_code and f.business_type=a.business_type		
	left join -- 新签客户
		( 
		select
			second_supervisor_code,
			if(work_no=second_supervisor_work_no,'自有','团队') as business_type,
			count(customer_no) as new_sign_customers,
			sum(estimate_contract_amount) as new_sign_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = ${hiveconf:current_end_day}
			and regexp_replace(substr(sign_time,1,10),'-','') between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
			and second_supervisor_code !=''
		group by 
			second_supervisor_code,
			if(work_no=second_supervisor_work_no,'自有','团队')
		) g on g.second_supervisor_code=a.second_supervisor_code and g.business_type=a.business_type
	left join -- 商机数量及金额
		( 
		select 
			t2.second_supervisor_code,
			t2.business_type,
			count(distinct t1.business_number) as business_number_cnt,
			sum(case when t1.business_stage=3 then t1.estimate_contract_amount*0.5 when t1.business_stage=4 then t1.estimate_contract_amount*0.75 else null end) as estimate_contract_amount		
		from 
			(
			select
				customer_id,business_number,customer_name,business_stage,estimate_contract_amount
			from
				csx_dw.ads_crm_r_m_business_customer
			where 
				month = substr(${hiveconf:current_end_day},1,6)
				and status=1
				and business_stage in ('3','4')
			) t1 
			join
				(
				select
					customer_id,customer_no,customer_name,sales_id,work_no,sales_name,first_supervisor_code,first_supervisor_work_no,first_supervisor_name,
					second_supervisor_code,second_supervisor_work_no,second_supervisor_name,if(work_no=second_supervisor_work_no,'自有','团队') as business_type
				from
					csx_dw.dws_crm_w_a_customer
				where
					sdt = ${hiveconf:current_end_day}
					and second_supervisor_code !=''
				) t2 on t2.customer_id=t1.customer_id
		group by 
			t2.second_supervisor_code,
			t2.business_type
		) h on h.second_supervisor_code=a.second_supervisor_code and h.business_type=a.business_type			
	left join --新签商机数及金额
		(
		select
			c.second_supervisor_code,
			c.business_type,
			count(distinct a.business_number) as new_sign_business,
			sum(estimate_contract_amount) as new_sign_business_amount
		from
			(
			select 
				customer_id,business_number,customer_name,sales_id,
				estimate_contract_amount,business_stage,status
			from
				csx_dw.ads_crm_r_m_business_customer
			where 
				month = substr(${hiveconf:current_end_day},1,6)
				and status='1'
				and business_stage=5
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
					and regexp_replace(substr(create_time,1,10),'-','') between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
				group by 
					business_number,create_time
				) b on b.business_number=a.business_number
			join
				(
				select
					customer_id,customer_no,customer_name,sales_id,work_no,sales_name,first_supervisor_code,first_supervisor_work_no,first_supervisor_name,
					second_supervisor_code,second_supervisor_work_no,second_supervisor_name,if(work_no=second_supervisor_work_no,'自有','团队') as business_type
				from
					csx_dw.dws_crm_w_a_customer
				where
					sdt = ${hiveconf:current_end_day}
					and second_supervisor_code !=''
				) c on c.customer_id=a.customer_id
		group by 
			c.second_supervisor_code,
			c.business_type
		) i on i.second_supervisor_code=a.second_supervisor_code and i.business_type=a.business_type	
where
	c.province_name is not null
	and b.user_position='SALES_CITY_MANAGER'
	--and (d.user_position is null or d.user_position !='CUSTOMER_SERVICE_MANAGER')
;
--=============================================================================================================================================================================
--服务管家
--set current_start_day ='20211001';

--set current_end_day ='20211031';

insert overwrite directory '/tmp/zhangyanpeng/20211110_rank_customer_service_manager' row format delimited fields terminated by '\t'
			
select
	c.province_name,
	c.city_group_name,
	a.first_supervisor_name,
	a.work_no,
	a.sales_name,
	coalesce(e.begin_date,'') as begin_date,
	coalesce(b.status,'') as status,
	'' as sales_target,
	coalesce(f.sales_value,0) as sales_value,
	'' as sales_target_achievement_rate,
	'' as profit_rate_target,
	coalesce(f.profit_rate,0) as profit_rate,
	'' as profit_rate_achievement,
	coalesce(f.performance_customers,0) as performance_customers,
	'' as new_sign_customers_target,
	coalesce(g.new_sign_customers,0) as new_sign_customers,
	coalesce(g.new_sign_amount,0) as new_sign_amount,
	coalesce(i.new_sign_business,0) as new_sign_business,
	coalesce(i.new_sign_business_amount,0) as new_sign_business_amount,
	'' as new_sign_customers_achievement_rate,
	coalesce(h.business_number_cnt,0) as business_number_cnt,
	coalesce(h.estimate_contract_amount,0) as estimate_contract_amount,
	row_number()over(partition by c.province_name,c.city_group_name order by f.sales_value desc) as rn
from
	(
	select --销售员信息
		sales_id,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,second_supervisor_work_no,second_supervisor_name
	from
		csx_dw.dws_crm_w_a_customer
	where 
		sdt = ${hiveconf:current_end_day}
		--and sales_position='SALES'
	group by 
		sales_id,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,second_supervisor_work_no,second_supervisor_name
	) a 
	left join -- 用户表，获取城市信息
		(
		select
			id,user_number,name,user_position,city_name,prov_name,if(status=0,'启用','禁用') as status
		from
			csx_dw.dws_basic_w_a_user
		where
			sdt=${hiveconf:current_end_day}
			and del_flag = '0'
		group by 
			id,user_number,name,user_position,city_name,prov_name,if(status=0,'启用','禁用')
		) b on b.id=a.sales_id
	left join -- 区域表
		( 
		select
			city_code,city_name,area_province_code,area_province_name,city_group_code,city_group_name,province_code,province_name,region_code,region_name
		from
			csx_dw.dws_sale_w_a_area_belong
		) c on c.city_name=b.city_name and c.area_province_name=b.prov_name
	left join --兼岗信息
		(
		select
			user_id,user_position
		from
			csx_ods.source_uc_w_a_user_position 
		where
			sdt=${hiveconf:current_end_day}
		group by 
			user_id,user_position
		) d on d.user_id=a.sales_id			
	left join -- 入职信息
		(
		select 
			employee_code,employee_name,begin_date,leader_code,leader_name,emp_status
		from 
			csx_dw.dws_basic_w_a_employee_org_m
		where 
			sdt =${hiveconf:current_end_day}
		) e on e.employee_code=a.work_no
	left join --销售额 履约客户数
		(	
		select 
			t2.sales_id,
			sum(t1.sales_value) as sales_value,
			sum(t1.profit) as profit,
			sum(t1.profit)/abs(sum(t1.sales_value)) as profit_rate,
			count(distinct t1.customer_no) as performance_customers
		from
			(
			select
				customer_no,business_type_name,substr(sdt,1,6) as smonth,sales_value,profit
			from
				csx_dw.dws_sale_r_d_detail
			where 
				sdt>=${hiveconf:current_start_day} and sdt<=${hiveconf:current_end_day}
				and channel_code in('1','7','9')
				and business_type_code !='4'
			) t1 
			left join
				(
				select
					customer_id,customer_no,customer_name,sales_id,work_no,sales_name,first_supervisor_code,first_supervisor_work_no,first_supervisor_name
				from
					csx_dw.dws_crm_w_a_customer
				where
					sdt = ${hiveconf:current_end_day}
				) t2 on t2.customer_no=t1.customer_no					
		group by 
			t2.sales_id
		) f on f.sales_id=a.sales_id		
	left join -- 新签客户
		( 
		select
			sales_id,
			count(customer_no) as new_sign_customers,
			sum(estimate_contract_amount) as new_sign_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = ${hiveconf:current_end_day}
			and regexp_replace(substr(sign_time,1,10),'-','') between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
		group by 
			sales_id
		) g on g.sales_id=a.sales_id			
	left join -- 商机数量及金额
		( 
		select 
			sales_id,
			count(distinct business_number) as business_number_cnt,
			--count(distinct case when business_stage=5 then business_number else null end)/count(distinct business_number) as rate,
			sum(case when business_stage=3 then estimate_contract_amount*0.5 when business_stage=4 then estimate_contract_amount*0.75 else null end) as estimate_contract_amount		
		from 
			csx_dw.ads_crm_r_m_business_customer
		where 
			month = substr(${hiveconf:current_end_day},1,6)
			and status=1
			and business_stage in ('3','4')
		group by 
			sales_id
		) h on h.sales_id=a.sales_id			
	left join --新签商机数及金额
		(
		select
			a.sales_id,
			count(distinct a.business_number) as new_sign_business,
			sum(estimate_contract_amount) as new_sign_business_amount
		from
			(
			select 
				customer_id,business_number,customer_name,sales_id,
				estimate_contract_amount,business_stage,status
			from
				csx_dw.ads_crm_r_m_business_customer
			where 
				month = substr(${hiveconf:current_end_day},1,6)
				and status='1'
				and business_stage=5
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
					and regexp_replace(substr(create_time,1,10),'-','') between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
				group by 
					business_number,create_time
				) b on b.business_number=a.business_number
		group by 
			a.sales_id
		) i on i.sales_id=a.sales_id			
where
	c.province_name is not null
	and (b.user_position='CUSTOMER_SERVICE_MANAGER' or (b.user_position='SALES' and d.user_position='CUSTOMER_SERVICE_MANAGER'))
	--and (d.user_position is null or d.user_position !='CUSTOMER_SERVICE_MANAGER')
;
--=============================================================================================================================================================================
--销售支持
--set current_start_day ='20211001';

--set current_end_day ='20211031';

insert overwrite directory '/tmp/zhangyanpeng/20211110_rank_customer_sales_support' row format delimited fields terminated by '\t'
			
select
	c.province_name,
	c.city_group_name,
	a.first_supervisor_name,
	a.work_no,
	a.sales_name,
	coalesce(e.begin_date,'') as begin_date,
	coalesce(b.status,'') as status,
	'' as sales_target,
	coalesce(f.sales_value,0) as sales_value,
	'' as sales_target_achievement_rate,
	'' as profit_rate_target,
	coalesce(f.profit_rate,0) as profit_rate,
	'' as profit_rate_achievement,
	coalesce(f.performance_customers,0) as performance_customers,
	'' as new_sign_customers_target,
	coalesce(g.new_sign_customers,0) as new_sign_customers,
	coalesce(g.new_sign_amount,0) as new_sign_amount,
	coalesce(i.new_sign_business,0) as new_sign_business,
	coalesce(i.new_sign_business_amount,0) as new_sign_business_amount,
	'' as new_sign_customers_achievement_rate,
	coalesce(h.business_number_cnt,0) as business_number_cnt,
	coalesce(h.estimate_contract_amount,0) as estimate_contract_amount,
	row_number()over(partition by c.province_name,c.city_group_name order by f.sales_value desc) as rn
from
	(
	select --销售员信息
		sales_id,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,second_supervisor_work_no,second_supervisor_name
	from
		csx_dw.dws_crm_w_a_customer
	where 
		sdt = ${hiveconf:current_end_day}
		--and sales_position='SALES'
	group by 
		sales_id,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,second_supervisor_work_no,second_supervisor_name
	) a 
	left join -- 用户表，获取城市信息
		(
		select
			id,user_number,name,user_position,city_name,prov_name,if(status=0,'启用','禁用') as status
		from
			csx_dw.dws_basic_w_a_user
		where
			sdt=${hiveconf:current_end_day}
			and del_flag = '0'
		group by 
			id,user_number,name,user_position,city_name,prov_name,if(status=0,'启用','禁用')
		) b on b.id=a.sales_id
	left join -- 区域表
		( 
		select
			city_code,city_name,area_province_code,area_province_name,city_group_code,city_group_name,province_code,province_name,region_code,region_name
		from
			csx_dw.dws_sale_w_a_area_belong
		) c on c.city_name=b.city_name and c.area_province_name=b.prov_name
	left join --兼岗信息
		(
		select
			user_id,user_position
		from
			csx_ods.source_uc_w_a_user_position 
		where
			sdt=${hiveconf:current_end_day}
		group by 
			user_id,user_position
		) d on d.user_id=a.sales_id			
	left join -- 入职信息
		(
		select 
			employee_code,employee_name,begin_date,leader_code,leader_name,emp_status
		from 
			csx_dw.dws_basic_w_a_employee_org_m
		where 
			sdt =${hiveconf:current_end_day}
		) e on e.employee_code=a.work_no
	left join --销售额 履约客户数
		(	
		select 
			t2.sales_id,
			sum(t1.sales_value) as sales_value,
			sum(t1.profit) as profit,
			sum(t1.profit)/abs(sum(t1.sales_value)) as profit_rate,
			count(distinct t1.customer_no) as performance_customers
		from
			(
			select
				customer_no,business_type_name,substr(sdt,1,6) as smonth,sales_value,profit
			from
				csx_dw.dws_sale_r_d_detail
			where 
				sdt>=${hiveconf:current_start_day} and sdt<=${hiveconf:current_end_day}
				and channel_code in('1','7','9')
				and business_type_code !='4'
			) t1 
			left join
				(
				select
					customer_id,customer_no,customer_name,sales_id,work_no,sales_name,first_supervisor_code,first_supervisor_work_no,first_supervisor_name
				from
					csx_dw.dws_crm_w_a_customer
				where
					sdt = ${hiveconf:current_end_day}
				) t2 on t2.customer_no=t1.customer_no					
		group by 
			t2.sales_id
		) f on f.sales_id=a.sales_id		
	left join -- 新签客户
		( 
		select
			sales_id,
			count(customer_no) as new_sign_customers,
			sum(estimate_contract_amount) as new_sign_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = ${hiveconf:current_end_day}
			and regexp_replace(substr(sign_time,1,10),'-','') between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
		group by 
			sales_id
		) g on g.sales_id=a.sales_id			
	left join -- 商机数量及金额
		( 
		select 
			sales_id,
			count(distinct business_number) as business_number_cnt,
			--count(distinct case when business_stage=5 then business_number else null end)/count(distinct business_number) as rate,
			sum(case when business_stage=3 then estimate_contract_amount*0.5 when business_stage=4 then estimate_contract_amount*0.75 else null end) as estimate_contract_amount		
		from 
			csx_dw.ads_crm_r_m_business_customer
		where 
			month = substr(${hiveconf:current_end_day},1,6)
			and status=1
			and business_stage in ('3','4')
		group by 
			sales_id
		) h on h.sales_id=a.sales_id			
	left join --新签商机数及金额
		(
		select
			a.sales_id,
			count(distinct a.business_number) as new_sign_business,
			sum(estimate_contract_amount) as new_sign_business_amount
		from
			(
			select 
				customer_id,business_number,customer_name,sales_id,
				estimate_contract_amount,business_stage,status
			from
				csx_dw.ads_crm_r_m_business_customer
			where 
				month = substr(${hiveconf:current_end_day},1,6)
				and status='1'
				and business_stage=5
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
					and regexp_replace(substr(create_time,1,10),'-','') between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
				group by 
					business_number,create_time
				) b on b.business_number=a.business_number
		group by 
			a.sales_id
		) i on i.sales_id=a.sales_id			
where
	c.province_name is not null
	and b.user_position='SALES_SUPPORT'
	--and (d.user_position is null or d.user_position !='CUSTOMER_SERVICE_MANAGER')
;
