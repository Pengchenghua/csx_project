-- 由于标识错误 需剔除部分客户：'118849','118365','114646','118215','120645','115237','113387'
-- 销售主管激励案_Q3福利激励案_Q3福利毛利额 定价毛利额改为前端毛利额 20210906 
-- 贺仕文由销售员更改为主管
-- 20210918 去掉公司招标客户

--==================================================================================================================================================================================
set current_start_day ='20210901';

set current_end_day ='20210930';

set last_month_start_day ='20210801';

set last_month_end_day ='20210831';


--===================================================================================================================================================================
-- 明细

insert overwrite directory '/tmp/zhangyanpeng/20210813_13' row format delimited fields terminated by '\t'

select	
	a.smonth,a.province_name,a.customer_no,a.customer_name,a.work_no,a.sales_name,
	--coalesce(b.leader_code,'') as leader_code,coalesce(b.leader_name,'') as leader_name,
	supervisor_work_no,supervisor_name,
	a.business_type_name,a.sales_value,a.profit,a.front_profit,a.profit_rate,a.front_profit_rate
from
	(
	select
		substr(sdt,1,6) as smonth,province_name,customer_no,customer_name,work_no,sales_name,
		supervisor_work_no,supervisor_name,
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
		substr(sdt,1,6),province_name,customer_no,customer_name,work_no,sales_name,supervisor_work_no,supervisor_name,business_type_name
	) a 
	left join
		(
		select 
			employee_code,employee_name,begin_date,leader_code,leader_name
		from 
			csx_dw.dws_basic_w_a_employee_org_m
		where 
			--sdt = ${hiveconf:current_end_day}
			sdt = '20210922'
		) b on b.employee_code=a.work_no
;

--===================================================================================================================================================================
-- 人效绩优明细

insert overwrite directory '/tmp/zhangyanpeng/20210813_14' row format delimited fields terminated by '\t'

select
	b.sales_province_name,
	a.customer_no,
	b.customer_name,
	a.business_type_name,
	b.work_no,
	b.sales_name,
	b.first_supervisor_work_no,
	b.first_supervisor_name,
	a.sales_value,
	a.profit,
	a.front_profit,
	a.profit_rate,
	a.front_profit_rate
from
	(
	select
		customer_no,business_type_name,
		sum(sales_value) as sales_value, --含税销售额
		sum(profit) as profit, --含税毛利额
		sum(front_profit) as front_profit, --前端含税毛利
		sum(profit)/abs(sum(sales_value)) as profit_rate,
		sum(front_profit)/abs(sum(sales_value)) as front_profit_rate,
		count(distinct work_no) as sales_cnt,
		sum(sales_value)/count(distinct work_no) as avg_sales_value
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
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
		customer_no,business_type_name
	) a 
	left join
		(
		select
			customer_no,customer_name,work_no,sales_name,
			if(work_no='81095965',work_no,first_supervisor_work_no) as first_supervisor_work_no,
			if(work_no='81095965',sales_name,first_supervisor_name) as first_supervisor_name,
			sales_province_name
		from 
			csx_dw.dws_crm_w_a_customer
		where
			sdt='current'
		) b on b.customer_no=a.customer_no
;	
	
