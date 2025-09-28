-- 由于标识错误 需剔除部分客户：'118849','118365','114646','118215','120645','115237','113387'
-- 销售主管激励案_Q3福利激励案_Q3福利毛利额 定价毛利额改为前端毛利额 20210906 
-- 贺仕文由销售员更改为主管

--==================================================================================================================================================================================
set current_start_day ='20210901';

set current_end_day ='20210929';

set last_month_start_day ='20210801';

set last_month_end_day ='20210831';

--01_销售员激励案_Q3福利激励案_百万精英奖

insert overwrite directory '/tmp/zhangyanpeng/20210918_01' row format delimited fields terminated by '\t'

select
	concat('20210701','-',${hiveconf:current_end_day}) as sdt_s,
	b.sales_region_name,
	b.sales_province_name,
	b.sales_city_name,
	coalesce(d.sales_supervisor_work_no,'') as sales_supervisor_work_no,
	coalesce(d.sales_supervisor_name,'') as sales_supervisor_name,
	a.work_no,
	a.sales_name,
	coalesce(c.begin_date,'') as begin_date,
	sum(a.sales_value) as sales_value,
	sum(a.front_profit) as front_profit,
	sum(a.front_profit)/abs(sum(a.sales_value)) as profit_rate,
	row_number() over(order by sum(a.sales_value) desc) as rn
from 
	(
	select 
		customer_no,work_no,sales_name,
		sum(sales_value)as sales_value,
		sum(profit) as profit,
		sum(front_profit) as front_profit
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20210701' and ${hiveconf:current_end_day}
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
		and business_type_code in ('2','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and sales_name not rlike 'A|B|C|M' --虚拟销售
		and customer_no not in ('118849','118365','114646','118215','120645','115237','113387') --剔除部分客户
		-- 20210918 剔除部分客户
		and customer_no not in ('117529','118693','118892','118897','118907','118911','118912','118913','118915','118918','119032','120123','121015','120704','119925','110931',
		'112735','118136','116205','117015','115936','109544','112088','108105','108201','110898','106306','106320','106330','106298','106325','117108','106321','106326','106283',
		'106299','106309','120024','119990','106805','118072') -- 20210918 剔除部分客户
		-- 20210923 剔除部分客户
		and customer_no not in ('121112','121109','120666','120360','110807','110807','110807','110807','110807','115971','120360','117255','117814','110807','115643')
	group by 
		customer_no,work_no,sales_name
	having
		sum(sales_value)>=10000
		and sum(front_profit)/abs(sum(sales_value))>0
	) a  
	left join   
		(
		select 
			customer_no,customer_name,sales_region_name,sales_province_name,sales_city_name
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt=${hiveconf:current_end_day}
		group by 
			customer_no,customer_name,sales_region_name,sales_province_name,sales_city_name
		) b on b.customer_no=a.customer_no
	join -- 员工信息
		(
		select 
			employee_code,employee_name,begin_date,leader_code,leader_name
		from 
			csx_dw.dws_basic_w_a_employee_org_m
		where 
			sdt = ${hiveconf:current_end_day}
			and emp_status='on'
		) c on c.employee_code=a.work_no
	left join
		(
		select
			sales_id,sales_name,work_no,position,sales_supervisor_id,sales_supervisor_work_no,sales_supervisor_name
		from
			(
			select
				id as sales_id,
				name as sales_name,
				user_number as work_no,
				user_position as position,
				-- 主管
				first_value(case when leader_user_position = 'SALES_MANAGER' then leader_id end, true) over(partition by id order by distance) as sales_supervisor_id,
				first_value(case when leader_user_position = 'SALES_MANAGER' then leader_name end, true) over(partition by id order by distance) as sales_supervisor_name,
				first_value(case when leader_user_position = 'SALES_MANAGER' then leader_user_number end, true) over(partition by id order by distance) as sales_supervisor_work_no,
				row_number() over(partition by id order by distance desc) as rank
			from 
				csx_dw.dwd_uc_w_a_user_adjust
			where 
				sdt = 'current'
			) tmp 
		where 
			tmp.rank = 1
			and sales_supervisor_work_no is not null
		group by 
			sales_id,sales_name,work_no,position,sales_supervisor_id,sales_supervisor_work_no,sales_supervisor_name
		) d on d.work_no=a.work_no	
group by 
	b.sales_region_name,
	b.sales_province_name,
	b.sales_city_name,
	coalesce(d.sales_supervisor_work_no,''),
	coalesce(d.sales_supervisor_name,''),
	a.work_no,
	a.sales_name,
	coalesce(c.begin_date,'')
;
	
--==================================================================================================================================================================================
--02_销售员激励案_Q3福利激励案_双节福利王

insert overwrite directory '/tmp/zhangyanpeng/20210918_02' row format delimited fields terminated by '\t'

select
	concat('20210701','-',${hiveconf:current_end_day}) as sdt_s,
	b.sales_region_name,
	b.sales_province_name,
	b.sales_city_name,
	coalesce(d.sales_supervisor_work_no,'') as sales_supervisor_work_no,
	coalesce(d.sales_supervisor_name,'') as sales_supervisor_name,
	a.work_no,
	a.sales_name,
	coalesce(c.begin_date,'') as begin_date,
	sum(a.sales_value) as sales_value,
	sum(a.front_profit) as front_profit,
	sum(a.front_profit)/abs(sum(a.sales_value)) as profit_rate,
	row_number() over(order by sum(a.sales_value) desc) as rn
from 
	(
	select 
		customer_no,work_no,sales_name,
		sum(sales_value)as sales_value,
		sum(profit) as profit,
		sum(front_profit) as front_profit
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20210701' and ${hiveconf:current_end_day}
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
		and business_type_code in ('2','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and sales_name not rlike 'A|B|C|M' --虚拟销售
		and customer_no not in ('118849','118365','114646','118215','120645','115237','113387') --剔除部分客户
		-- 20210918 剔除部分客户
		and customer_no not in ('117529','118693','118892','118897','118907','118911','118912','118913','118915','118918','119032','120123','121015','120704','119925','110931',
		'112735','118136','116205','117015','115936','109544','112088','108105','108201','110898','106306','106320','106330','106298','106325','117108','106321','106326','106283',
		'106299','106309','120024','119990','106805','118072') -- 20210918 剔除部分客户
		-- 20210923 剔除部分客户
		and customer_no not in ('121112','121109','120666','120360','110807','110807','110807','110807','110807','115971','120360','117255','117814','110807','115643')
	group by 
		customer_no,work_no,sales_name
	having
		sum(sales_value)>=10000
		and sum(front_profit)/abs(sum(sales_value))>0
	) a  
	left join   
		(
		select 
			customer_no,customer_name,sales_region_name,sales_province_name,sales_city_name
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt=${hiveconf:current_end_day}
		group by 
			customer_no,customer_name,sales_region_name,sales_province_name,sales_city_name
		) b on b.customer_no=a.customer_no
	join -- 员工信息
		(
		select 
			employee_code,employee_name,begin_date,leader_code,leader_name
		from 
			csx_dw.dws_basic_w_a_employee_org_m
		where 
			sdt = ${hiveconf:current_end_day}
			and emp_status='on'
		) c on c.employee_code=a.work_no
	left join
		(
		select
			sales_id,sales_name,work_no,position,sales_supervisor_id,sales_supervisor_work_no,sales_supervisor_name
		from
			(
			select
				id as sales_id,
				name as sales_name,
				user_number as work_no,
				user_position as position,
				-- 主管
				first_value(case when leader_user_position = 'SALES_MANAGER' then leader_id end, true) over(partition by id order by distance) as sales_supervisor_id,
				first_value(case when leader_user_position = 'SALES_MANAGER' then leader_name end, true) over(partition by id order by distance) as sales_supervisor_name,
				first_value(case when leader_user_position = 'SALES_MANAGER' then leader_user_number end, true) over(partition by id order by distance) as sales_supervisor_work_no,
				row_number() over(partition by id order by distance desc) as rank
			from 
				csx_dw.dwd_uc_w_a_user_adjust
			where 
				sdt = 'current'
			) tmp 
		where 
			tmp.rank = 1
			and sales_supervisor_work_no is not null
		group by 
			sales_id,sales_name,work_no,position,sales_supervisor_id,sales_supervisor_work_no,sales_supervisor_name
		) d on d.work_no=a.work_no	
group by 
	b.sales_region_name,
	b.sales_province_name,
	b.sales_city_name,
	coalesce(d.sales_supervisor_work_no,''),
	coalesce(d.sales_supervisor_name,''),
	a.work_no,
	a.sales_name,
	coalesce(c.begin_date,'')
;

--==================================================================================================================================================================================
--03_销售员激励案_Q3福利激励案_福利新人王

insert overwrite directory '/tmp/zhangyanpeng/20210918_03' row format delimited fields terminated by '\t'

select
	concat('20210701','-',${hiveconf:current_end_day}) as sdt_s,
	b.sales_region_name,
	b.sales_province_name,
	b.sales_city_name,
	coalesce(d.sales_supervisor_work_no,'') as sales_supervisor_work_no,
	coalesce(d.sales_supervisor_name,'') as sales_supervisor_name,
	a.work_no,
	a.sales_name,
	coalesce(c.begin_date,'') as begin_date,
	sum(a.sales_value) as sales_value,
	sum(a.front_profit) as front_profit,
	sum(a.front_profit)/abs(sum(a.sales_value)) as profit_rate,
	row_number() over(order by sum(a.sales_value) desc) as rn
from 
	(
	select 
		customer_no,work_no,sales_name,
		sum(sales_value)as sales_value,
		sum(profit) as profit,
		sum(front_profit) as front_profit
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20210701' and ${hiveconf:current_end_day}
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
		and business_type_code in ('2','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and sales_name not rlike 'A|B|C|M' --虚拟销售
		and customer_no not in ('118849','118365','114646','118215','120645','115237','113387') --剔除部分客户
		-- 20210918 剔除部分客户
		and customer_no not in ('117529','118693','118892','118897','118907','118911','118912','118913','118915','118918','119032','120123','121015','120704','119925','110931',
		'112735','118136','116205','117015','115936','109544','112088','108105','108201','110898','106306','106320','106330','106298','106325','117108','106321','106326','106283',
		'106299','106309','120024','119990','106805','118072') -- 20210918 剔除部分客户
		-- 20210923 剔除部分客户
		and customer_no not in ('121112','121109','120666','120360','110807','110807','110807','110807','110807','115971','120360','117255','117814','110807','115643')
	group by 
		customer_no,work_no,sales_name
	having
		sum(sales_value)>=10000
		and sum(front_profit)/abs(sum(sales_value))>0
	) a  
	left join   
		(
		select 
			customer_no,customer_name,sales_region_name,sales_province_name,sales_city_name
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt=${hiveconf:current_end_day}
		group by 
			customer_no,customer_name,sales_region_name,sales_province_name,sales_city_name
		) b on b.customer_no=a.customer_no
	join -- 员工信息
		(
		select 
			employee_code,employee_name,begin_date,leader_code,leader_name
		from 
			csx_dw.dws_basic_w_a_employee_org_m
		where 
			sdt = ${hiveconf:current_end_day}
			and emp_status='on'
			and begin_date>='20210701'
		) c on c.employee_code=a.work_no
	left join
		(
		select
			sales_id,sales_name,work_no,position,sales_supervisor_id,sales_supervisor_work_no,sales_supervisor_name
		from
			(
			select
				id as sales_id,
				name as sales_name,
				user_number as work_no,
				user_position as position,
				-- 主管
				first_value(case when leader_user_position = 'SALES_MANAGER' then leader_id end, true) over(partition by id order by distance) as sales_supervisor_id,
				first_value(case when leader_user_position = 'SALES_MANAGER' then leader_name end, true) over(partition by id order by distance) as sales_supervisor_name,
				first_value(case when leader_user_position = 'SALES_MANAGER' then leader_user_number end, true) over(partition by id order by distance) as sales_supervisor_work_no,
				row_number() over(partition by id order by distance desc) as rank
			from 
				csx_dw.dwd_uc_w_a_user_adjust
			where 
				sdt = 'current'
			) tmp 
		where 
			tmp.rank = 1
			and sales_supervisor_work_no is not null
		group by 
			sales_id,sales_name,work_no,position,sales_supervisor_id,sales_supervisor_work_no,sales_supervisor_name
		) d on d.work_no=a.work_no	
group by 
	b.sales_region_name,
	b.sales_province_name,
	b.sales_city_name,
	coalesce(d.sales_supervisor_work_no,''),
	coalesce(d.sales_supervisor_name,''),
	a.work_no,
	a.sales_name,
	coalesce(c.begin_date,'')
;


--==================================================================================================================================================================================
--08_销售主管激励案_Q3福利激励案_Q3福利销售额

insert overwrite directory '/tmp/zhangyanpeng/20210918_08' row format delimited fields terminated by '\t'

select
	concat('20210701','-',${hiveconf:current_end_day}) as sdt_s,
	b.sales_region_name,
	b.sales_province_name,
	b.sales_city_name,
	a.supervisor_work_no,
	a.supervisor_name,
	coalesce(c.begin_date,'') as begin_date,
	sum(a.sales_value) as sales_value,
	sum(a.profit) as profit,
	sum(a.profit)/abs(sum(a.sales_value)) as profit_rate,
	sum(a.front_profit) as front_profit,
	sum(a.front_profit)/abs(sum(a.sales_value)) as front_profit_rate,
	row_number() over(order by sum(a.sales_value) desc) as rn
from 
	(
	select 
		customer_no,supervisor_work_no,supervisor_name,
		sum(sales_value)as sales_value,
		sum(profit) as profit,
		sum(front_profit) as front_profit
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20210701' and ${hiveconf:current_end_day}
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
		and business_type_code in ('2','6')
		and supervisor_work_no !=''
		and customer_no not in ('118849','118365','114646','118215','120645','115237','113387') --剔除部分客户
		-- 20210918 剔除部分客户
		and customer_no not in ('117529','118693','118892','118897','118907','118911','118912','118913','118915','118918','119032','120123','121015','120704','119925','110931',
		'112735','118136','116205','117015','115936','109544','112088','108105','108201','110898','106306','106320','106330','106298','106325','117108','106321','106326','106283',
		'106299','106309','120024','119990','106805','118072') -- 20210918 剔除部分客户
		-- 20210923 剔除部分客户
		and customer_no not in ('121112','121109','120666','120360','110807','110807','110807','110807','110807','115971','120360','117255','117814','110807','115643')
	group by 
		customer_no,supervisor_work_no,supervisor_name
	having
		sum(sales_value)>=10000
		and sum(front_profit)/abs(sum(sales_value))>0
	) a  
	left join   
		(
		select 
			customer_no,customer_name,sales_region_name,sales_province_name,sales_city_name
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt=${hiveconf:current_end_day}
		group by 
			customer_no,customer_name,sales_region_name,sales_province_name,sales_city_name
		) b on b.customer_no=a.customer_no
	join -- 员工信息
		(
		select 
			employee_code,employee_name,begin_date
		from 
			csx_dw.dws_basic_w_a_employee_org_m
		where 
			sdt = ${hiveconf:current_end_day}
			and emp_status='on'
		) c on c.employee_code=a.supervisor_work_no
group by 
	b.sales_region_name,
	b.sales_province_name,
	b.sales_city_name,
	a.supervisor_work_no,
	a.supervisor_name,
	coalesce(c.begin_date,'')
;


--==================================================================================================================================================================================
--09_销售主管激励案_Q3福利激励案_Q3福利毛利额

insert overwrite directory '/tmp/zhangyanpeng/20210918_09' row format delimited fields terminated by '\t'

select
	concat('20210701','-',${hiveconf:current_end_day}) as sdt_s,
	b.sales_region_name,
	b.sales_province_name,
	b.sales_city_name,
	a.supervisor_work_no,
	a.supervisor_name,
	coalesce(c.begin_date,'') as begin_date,
	sum(a.sales_value) as sales_value,
	sum(a.profit) as profit,
	sum(a.profit)/abs(sum(a.sales_value)) as profit_rate,
	sum(a.front_profit) as front_profit,
	sum(a.front_profit)/abs(sum(a.sales_value)) as front_profit_rate,
	row_number() over(order by sum(a.front_profit) desc) as rn
from 
	(
	select 
		customer_no,supervisor_work_no,supervisor_name,
		sum(sales_value)as sales_value,
		sum(profit) as profit,
		sum(front_profit) as front_profit
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20210701' and ${hiveconf:current_end_day}
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
		and business_type_code in ('2','6')
		and supervisor_work_no !=''
		and customer_no not in ('118849','118365','114646','118215','120645','115237','113387') --剔除部分客户
		-- 20210918 剔除部分客户
		and customer_no not in ('117529','118693','118892','118897','118907','118911','118912','118913','118915','118918','119032','120123','121015','120704','119925','110931',
		'112735','118136','116205','117015','115936','109544','112088','108105','108201','110898','106306','106320','106330','106298','106325','117108','106321','106326','106283',
		'106299','106309','120024','119990','106805','118072') -- 20210918 剔除部分客户
		-- 20210923 剔除部分客户
		and customer_no not in ('121112','121109','120666','120360','110807','110807','110807','110807','110807','115971','120360','117255','117814','110807','115643')
	group by 
		customer_no,supervisor_work_no,supervisor_name
	having
		sum(sales_value)>=10000
		and sum(front_profit)/abs(sum(sales_value))>0
	) a  
	left join   
		(
		select 
			customer_no,customer_name,sales_region_name,sales_province_name,sales_city_name
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt=${hiveconf:current_end_day}
		group by 
			customer_no,customer_name,sales_region_name,sales_province_name,sales_city_name
		) b on b.customer_no=a.customer_no
	join -- 员工信息
		(
		select 
			employee_code,employee_name,begin_date
		from 
			csx_dw.dws_basic_w_a_employee_org_m
		where 
			sdt = ${hiveconf:current_end_day}
			and emp_status='on'
		) c on c.employee_code=a.supervisor_work_no
group by 
	b.sales_region_name,
	b.sales_province_name,
	b.sales_city_name,
	a.supervisor_work_no,
	a.supervisor_name,
	coalesce(c.begin_date,'')
;



--===================================================================================================================================================================
-- 明细

insert overwrite directory '/tmp/zhangyanpeng/20210918_13' row format delimited fields terminated by '\t'

select	
	a.smonth,a.province_name,a.customer_no,a.customer_name,a.work_no,a.sales_name,
	coalesce(b.leader_code,'') as leader_code,coalesce(b.leader_name,'') as leader_name,
	a.business_type_name,a.sales_value,a.profit,a.front_profit,a.profit_rate,a.front_profit_rate
from
	(
	select
		substr(sdt,1,6) as smonth,province_name,customer_no,customer_name,work_no,sales_name,
		-- supervisor_work_no,supervisor_name,
		business_type_name,
		sum(sales_value) as sales_value, --含税销售额
		sum(profit) as profit, --含税毛利额
		sum(front_profit) as front_profit, --前端含税毛利
		sum(profit)/abs(sum(sales_value)) as profit_rate,
		sum(front_profit)/abs(sum(sales_value)) as front_profit_rate
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt between '20210701' and ${hiveconf:current_end_day}
		and channel_code in ('1','7','9') 
		and (business_type_code in ('1','2','6') or (business_type_code in ('4') and supervisor_work_no in ('80886641','80972242')))
		--and supervisor_work_no !=''
		and customer_no not in ('118849','118365','114646','118215','120645','115237','113387') --剔除部分客户
		-- 20210918 剔除部分客户
		and customer_no not in ('117529','118693','118892','118897','118907','118911','118912','118913','118915','118918','119032','120123','121015','120704','119925','110931',
		'112735','118136','116205','117015','115936','109544','112088','108105','108201','110898','106306','106320','106330','106298','106325','117108','106321','106326','106283',
		'106299','106309','120024','119990','106805','118072') -- 20210918 剔除部分客户
		-- 20210923 剔除部分客户
		and customer_no not in ('121112','121109','120666','120360','110807','110807','110807','110807','110807','115971','120360','117255','117814','110807','115643')
	group by
		substr(sdt,1,6),province_name,customer_no,customer_name,work_no,sales_name,business_type_name
	) a 
	left join
		(
		select 
			employee_code,employee_name,begin_date,leader_code,leader_name
		from 
			csx_dw.dws_basic_w_a_employee_org_m
		where 
			sdt = ${hiveconf:current_end_day}
		) b on b.employee_code=a.work_no
;

