--1、履约客户数、新签客户数目标 对调位置
--2、新增：新签商机数、新签商机金额
--3、商机数：只取50% 和 75% 金额*对应的阶段数

--==============================================================================================================================================================================
--销售员

set current_start_day ='20211001';

set current_end_day ='20211031';

insert overwrite directory '/tmp/zhangyanpeng/20211108_rank_sales' row format delimited fields terminated by '\t'

select
	b.province_name,
	b.city_group_name,
	coalesce(f.sales_supervisor_name,'') as sales_supervisor_name,
	a.user_number,
	a.name,
	coalesce(g.begin_date,'') as begin_date,
	'' as target_1,
	c.sales_value,
	'' as target_2,
	'' as target_3,
	c.profit_rate,
	'' as target_4,
	c.customer_cnt as customer_cnt_2,
	'' as target_5,
	d.customer_cnt,
	d.estimate_contract_amount,
	h.cnt,
	h.estimate_contract_amount,
	'' as target_6,
	e.business_number_cnt,
	e.estimate_contract_amount as estimate_contract_amount_2
	--e.rate
from
	--销售员信息
	( 
	select
		user_number,name,city_name,prov_name
	from
		csx_dw.dws_basic_w_a_user
	where
		sdt=${hiveconf:current_end_day}
		and status = 0 
		and del_flag = '0'
		and user_position = 'SALES' 
		and prov_name not like '平台%'
		and channel in ('1','7')
		and name not rlike'A|B|C|M'
	group by 
		user_number,name,city_name,prov_name
	) a 
	-- 地区信息
	left join
		( 
		select
			city_code,city_name,area_province_code,area_province_name,city_group_code,city_group_name,province_code,province_name,region_code,region_name
		from
			csx_dw.dws_sale_w_a_area_belong
		) b on b.city_name=a.city_name and b.area_province_name=a.prov_name
	-- 销售额
	left join
		(	
		select 
			work_no,
			sum(sales_value) as sales_value,
			sum(profit) as profit,
			sum(profit)/abs(sum(sales_value)) as profit_rate,
			count(distinct customer_no) as customer_cnt
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>=${hiveconf:current_start_day} and sdt<=${hiveconf:current_end_day}
			and channel_code in('1','7','9')
		group by 
			work_no
		) c on c.work_no=a.user_number
	-- 新签客户
	left join
		( 
		select
			work_no,
			count(customer_no) as customer_cnt,
			sum(estimate_contract_amount) as estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = ${hiveconf:current_end_day}
			and regexp_replace(substr(sign_time,1,10),'-','') between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
		group by 
			work_no
		) d on d.work_no=a.user_number
	-- 商机数量及金额
	left join
		( 
		select 
			work_no,
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
			work_no
		) e on e.work_no=a.user_number
	-- 主管信息
	left join
		( 
		select
			sales_name,work_no,position,sales_supervisor_id,sales_supervisor_work_no,sales_supervisor_name
		from
			(
			select
				name as sales_name,
				user_number as work_no,
				user_position as position,
				-- 主管
				first_value(case when leader_user_position = 'SALES_MANAGER' then leader_id end, true) over(partition by user_number order by distance) as sales_supervisor_id,
				first_value(case when leader_user_position = 'SALES_MANAGER' then leader_name end, true) over(partition by user_number order by distance) as sales_supervisor_name,
				first_value(case when leader_user_position = 'SALES_MANAGER' then leader_user_number end, true) over(partition by user_number order by distance) as sales_supervisor_work_no,
				row_number() over(partition by user_number order by distance desc) as rank
			from 
				csx_dw.dwd_uc_w_a_user_adjust
			where 
				sdt = ${hiveconf:current_end_day}
			) tmp 
		where 
			tmp.rank = 1
			and sales_supervisor_work_no is not null
		group by 
			sales_name,work_no,position,sales_supervisor_id,sales_supervisor_work_no,sales_supervisor_name
		) f on f.work_no = a.user_number
	--入职信息
	left join
		(
		select
			employee_code,employee_name,begin_date,end_date,emp_status
		from
			csx_dw.dws_basic_w_a_employee_org_m
		where
			sdt = ${hiveconf:current_end_day}
		) g on g.employee_code=a.user_number
	left join
		(
		select
			c.work_no,
			count(distinct a.business_number) as cnt,
			sum(estimate_contract_amount) as estimate_contract_amount
		from
			(
			select 
				customer_id,business_number,customer_name,
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
			left join
				(
				select
					customer_id,work_no,first_supervisor_work_no,first_supervisor_name,second_supervisor_code,second_supervisor_work_no,third_supervisor_code
				from
					csx_dw.dws_crm_w_a_customer
				where
					sdt = ${hiveconf:current_end_day}
				) c on c.customer_id=a.customer_id
		group by 
			c.work_no
		) h on h.work_no=a.user_number
;

--销售主管

insert overwrite directory '/tmp/zhangyanpeng/20211108_rank_sales_manager' row format delimited fields terminated by '\t'

select
	b.province_name,
	b.city_group_name,
	coalesce(f.third_supervisor_work_no,'') as third_supervisor_work_no,
	a.user_number,
	a.name,
	coalesce(g.begin_date,'') as begin_date,
	'' as target_1,
	c.sales_value,
	'' as target_2,
	'' as target_3,
	c.profit_rate,
	'' as target_4,
	c.customer_cnt,
	'' as target_5,
	d.customer_cnt,
	d.estimate_contract_amount,
	h.cnt,
	h.estimate_contract_amount,
	'' as target_6,
	e.business_number_cnt,
	e.estimate_contract_amount
	--e.rate
from
	--人员信息
	( 
	select
		user_number,name,city_name,prov_name
	from
		csx_dw.dws_basic_w_a_user
	where
		sdt=${hiveconf:current_end_day}
		and status = 0 
		and del_flag = '0'
		and user_position = 'SALES_MANAGER' 
		and prov_name not like '平台%'
		and channel in ('1','7')
		and name not rlike'A|B|C|M'
	group by 
		user_number,name,city_name,prov_name
	) a 
	-- 地区信息
	left join
		( 
		select
			city_code,city_name,area_province_code,area_province_name,city_group_code,city_group_name,province_code,province_name,region_code,region_name
		from
			csx_dw.dws_sale_w_a_area_belong
		) b on b.city_name=a.city_name and b.area_province_name=a.prov_name
	-- 销售额
	left join
		(	
		select 
			supervisor_work_no,
			sum(sales_value) as sales_value,
			sum(profit) as profit,
			sum(profit)/abs(sum(sales_value)) as profit_rate,
			count(distinct customer_no) as customer_cnt
		from 
			csx_dw.dws_sale_r_d_sales_performance_detail
		where 
			sdt>=${hiveconf:current_start_day} and sdt<=${hiveconf:current_end_day}
			and channel_code in('1','7','9')
		group by 
			supervisor_work_no
		) c on c.supervisor_work_no=a.user_number
	-- 新签客户
	left join
		( 
		select
			first_supervisor_work_no,
			count(customer_no) as customer_cnt,
			sum(estimate_contract_amount) as estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = ${hiveconf:current_end_day}
			and regexp_replace(substr(sign_time,1,10),'-','') between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
		group by 
			first_supervisor_work_no
		) d on d.first_supervisor_work_no=a.user_number
	-- 商机数量及金额
	left join
		( 
		select 
			t2.first_supervisor_work_no,
			count(distinct t1.business_number) as business_number_cnt,
			--count(distinct case when t1.business_stage=5 then t1.business_number else null end)/count(distinct t1.business_number) as rate,
			sum(case when t1.business_stage=3 then t1.estimate_contract_amount*0.5 when t1.business_stage=4 then t1.estimate_contract_amount*0.75 else null end) as estimate_contract_amount
		from
			(
			select
				customer_id,business_number,business_stage,estimate_contract_amount
			from
				csx_dw.ads_crm_r_m_business_customer
			where 
				month = substr(${hiveconf:current_end_day},1,6)
				and status=1
				and business_stage in ('3','4')
			) t1
			left join
				(
				select
					customer_id,customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name
				from
					csx_dw.dws_crm_w_a_customer
				where
					sdt = ${hiveconf:current_end_day}
				) t2 on t2.customer_id=t1.customer_id
		group by 
			t2.first_supervisor_work_no
		) e on e.first_supervisor_work_no=a.user_number
	-- 主管信息
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
		) f on f.work_no = a.user_number
	--入职信息
	left join
		(
		select
			employee_code,employee_name,begin_date,end_date,emp_status
		from
			csx_dw.dws_basic_w_a_employee_org_m
		where
			sdt = ${hiveconf:current_end_day}
		) g on g.employee_code=a.user_number
	left join
		(
		select
			c.first_supervisor_work_no,
			count(distinct a.business_number) as cnt,
			sum(estimate_contract_amount) as estimate_contract_amount
		from
			(
			select 
				customer_id,business_number,customer_name,
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
			left join
				(
				select
					customer_id,work_no,first_supervisor_work_no,first_supervisor_name,second_supervisor_code,second_supervisor_work_no,third_supervisor_code
				from
					csx_dw.dws_crm_w_a_customer
				where
					sdt = ${hiveconf:current_end_day}
				) c on c.customer_id=a.customer_id
		group by 
			c.first_supervisor_work_no
		) h on h.first_supervisor_work_no=a.user_number
;

--销售经理

insert overwrite directory '/tmp/zhangyanpeng/20211108_rank_sales_city_manager' row format delimited fields terminated by '\t'

select
	b.province_name,
	b.city_group_name,
	a.user_number,
	a.name,
	coalesce(g.begin_date,'') as begin_date,
	'' as target_1,
	c.sales_value,
	'' as target_2,
	'' as target_3,
	c.profit_rate,
	'' as target_4,
	c.customer_cnt,
	'' as target_5,
	d.customer_cnt,
	d.estimate_contract_amount,
	h.cnt,
	h.estimate_contract_amount,
	'' as target_6,
	e.business_number_cnt,
	e.estimate_contract_amount
	--e.rate
from
	--人员信息
	( 
	select
		user_number,name,city_name,prov_name
	from
		csx_dw.dws_basic_w_a_user
	where
		sdt=${hiveconf:current_end_day}
		and status = 0 
		and del_flag = '0'
		and user_position = 'SALES_CITY_MANAGER' 
		and prov_name not like '平台%'
		and channel in ('1','7')
		and name not rlike'A|B|C|M'
	group by 
		user_number,name,city_name,prov_name
	) a 
	-- 地区信息
	left join
		( 
		select
			city_code,city_name,area_province_code,area_province_name,city_group_code,city_group_name,province_code,province_name,region_code,region_name
		from
			csx_dw.dws_sale_w_a_area_belong
		) b on b.city_name=a.city_name and b.area_province_name=a.prov_name
	-- 销售额
	left join
		(	
		select 
			t2.second_supervisor_work_no,
			sum(sales_value) as sales_value,
			sum(profit) as profit,
			sum(profit)/abs(sum(sales_value)) as profit_rate,
			count(distinct t1.customer_no) as customer_cnt
		from
			(
			select
				customer_no,sales_value,profit
			from
				csx_dw.dws_sale_r_d_detail
			where 
				sdt>=${hiveconf:current_start_day} 
				and sdt<=${hiveconf:current_end_day}
				and channel_code in('1','7','9')
			) t1
			left join
				(
				select
					customer_id,customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,second_supervisor_work_no,second_supervisor_name
				from
					csx_dw.dws_crm_w_a_customer
				where
					sdt = ${hiveconf:current_end_day}
				) t2 on t2.customer_no=t1.customer_no
		group by 
			t2.second_supervisor_work_no
		) c on c.second_supervisor_work_no=a.user_number
	-- 新签客户
	left join
		( 
		select
			second_supervisor_work_no,
			count(customer_no) as customer_cnt,
			sum(estimate_contract_amount) as estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = ${hiveconf:current_end_day}
			and regexp_replace(substr(sign_time,1,10),'-','') between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
		group by 
			second_supervisor_work_no
		) d on d.second_supervisor_work_no=a.user_number
	-- 商机数量及金额
	left join
		( 
		select 
			t2.second_supervisor_work_no,
			count(distinct t1.business_number) as business_number_cnt,
			--count(distinct case when t1.business_stage=5 then t1.business_number else null end)/count(distinct t1.business_number) as rate,
			sum(case when t1.business_stage=3 then t1.estimate_contract_amount*0.5 when t1.business_stage=4 then t1.estimate_contract_amount*0.75 else null end) as estimate_contract_amount	
		from
			(
			select
				customer_id,business_number,work_no,estimate_contract_amount,business_stage
			from
				csx_dw.ads_crm_r_m_business_customer
			where 
				month = substr(${hiveconf:current_end_day},1,6)
				and status=1
				and business_stage in ('3','4')
			) t1 
			left join
				(
				select
					customer_id,work_no,first_supervisor_work_no,first_supervisor_name,second_supervisor_code,second_supervisor_work_no,third_supervisor_code
				from
					csx_dw.dws_crm_w_a_customer
				where
					sdt = ${hiveconf:current_end_day}
				) t2 on t2.customer_id=t1.customer_id
		group by 
			t2.second_supervisor_work_no
		) e on e.second_supervisor_work_no=a.user_number
	--入职信息
	left join
		(
		select
			employee_code,employee_name,begin_date,end_date,emp_status
		from
			csx_dw.dws_basic_w_a_employee_org_m
		where
			sdt = ${hiveconf:current_end_day}
		) g on g.employee_code=a.user_number
	left join
		(
		select
			c.second_supervisor_work_no,
			count(distinct a.business_number) as cnt,
			sum(estimate_contract_amount) as estimate_contract_amount
		from
			(
			select 
				customer_id,business_number,customer_name,
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
			left join
				(
				select
					customer_id,work_no,first_supervisor_work_no,first_supervisor_name,second_supervisor_code,second_supervisor_work_no,third_supervisor_code
				from
					csx_dw.dws_crm_w_a_customer
				where
					sdt = ${hiveconf:current_end_day}
				) c on c.customer_id=a.customer_id
		group by 
			c.second_supervisor_work_no
		) h on h.second_supervisor_work_no=a.user_number
;

--=============================================================================================================================================================================
--兼岗 csx_ods.source_uc_w_a_user_position

select
	a.user_number,a.name,a.user_position,b.user_position
from
	( 
	select
		id,user_number,name,city_name,prov_name,user_position
	from
		csx_dw.dws_basic_w_a_user
	where
		sdt='20211108'
		and status = 0 
		and del_flag = '0'
		and user_position = 'SALES' 
		and prov_name not like '平台%'
		and channel in ('1','7')
		and name not rlike'A|B|C|M'
	group by 
		id,user_number,name,city_name,prov_name,user_position
	) a 
	left join
		(
		select
			user_id,user_position
		from
			csx_ods.source_uc_w_a_user_position
		where
			sdt='20211108'
			and user_position='CUSTOMER_SERVICE_MANAGER'
		group by 
			user_id,user_position
		) b on b.user_id=a.id
		