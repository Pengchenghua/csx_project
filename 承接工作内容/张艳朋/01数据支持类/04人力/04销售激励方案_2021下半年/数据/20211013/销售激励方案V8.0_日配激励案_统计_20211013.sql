-- 由于标识错误 需剔除部分客户：'118849','118365','114646','118215','120645','115237','113387'
-- 销售主管激励案_Q3福利激励案_Q3福利毛利额 定价毛利额改为前端毛利额 20210906 
-- 贺仕文由销售员更改为主管
-- 20210918 去掉公司招标客户

--==================================================================================================================================================================================
set current_start_day ='20210901';

set current_end_day ='20210930';

set last_month_start_day ='20210801';

set last_month_end_day ='20210831';


--==========================================================================================
--04_销售员激励案_销售之星_日配激励案_拓客之星

insert overwrite directory '/tmp/zhangyanpeng/20210813_04' row format delimited fields terminated by '\t'

select
	concat(${hiveconf:current_start_day},'-',${hiveconf:current_end_day}) as period,
	sales_region_name,sales_province_name,sales_city_name,first_supervisor_work_no,first_supervisor_name,work_no,sales_name,
	sum(sales_value) as sales_value,
	sum(front_profit) as front_profit,
	sum(front_profit) / abs(sum(sales_value)) as front_profit_rate,
	count(distinct customer_no) as cust_count,
	sum(avg_sales_value) as avg_sales_value,
	row_number()over(order by count(distinct customer_no) desc,sum(sales_value) desc) as rn
from	
	(	
	select	
		a.customer_no,c.sales_region_name,c.sales_province_name,c.sales_city_name,c.first_supervisor_work_no,c.first_supervisor_name,c.work_no,c.sales_name,
		a.sales_value,a.profit,a.front_profit,a.profit_rate,
		a.sales_value / (datediff(b.normal_last_order_date,b.normal_first_order_date)+1) as avg_sales_value
	from	
		(
		select
			customer_no,
			sum(sales_value) as sales_value, --含税销售额
			sum(profit) as profit, --含税毛利额
			sum(front_profit) as front_profit, --前端含税毛利
			sum(profit)/abs(sum(sales_value)) as profit_rate
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
			and channel_code in ('1','7','9') 
			and business_type_code in ('1') 
			and customer_no not in ('118849','118365','114646','118215','120645','115237','113387') --剔除部分客户
			-- 20210918 剔除部分客户
			and customer_no not in ('117529','118693','118892','118897','118907','118911','118912','118913','118915','118918','119032','120123','121015','120704','119925','110931',
			'112735','118136','116205','117015','115936','109544','112088','108105','108201','110898','106306','106320','106330','106298','106325','117108','106321','106326','106283',
			'106299','106309','120024','119990','106805','118072') -- 20210918 剔除部分客户
			-- 20210923 剔除部分客户
			and customer_no not in ('121112','121109','120666','120360','110807','110807','110807','110807','110807','115971','120360','117255','117814','110807','115643')
			and order_no not in (
			'OM21092500022852','OM21092500018389','OM21091600021970','OM21091800011804','OM21093000003441','OM21081800009334','OM21073000033690','OM21081800016201',
			'OM21072800045844','OM21072700002361','OM21072800045333','OM21072700002378','OM21072700002277','OM21072700002338','OM21091100000260','OM21082300007563')
			-- 20211012 剔除部分客户
			and customer_no not in ('122230','115300','117197','121880','109529','121574')
		group by
			customer_no
		having
			sum(sales_value)>=30000 -- 月履约金额大于等于3万
			and sum(front_profit)/abs(sum(sales_value))>=0.03 --新履约客户的前端毛利率≥3%
		) a 
		join
			(
			select 
				customer_no, 
				to_date(from_unixtime(unix_timestamp(normal_first_order_date,'yyyyMMdd'))) as normal_first_order_date,
				to_date(from_unixtime(unix_timestamp(normal_last_order_date,'yyyyMMdd'))) as normal_last_order_date
			from 
				csx_dw.dws_crm_w_a_customer_active
			where 
				sdt = ${hiveconf:current_end_day}
				and normal_first_order_date between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}		
			) b on b.customer_no=a.customer_no
		join 
			(
			select 
				customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,sales_region_name,sales_province_name,sales_city_name
			from 
				csx_dw.dws_crm_w_a_customer
			where 
				--sdt = ${hiveconf:current_end_day}
				sdt = '20210922'
				and customer_no<>''	
				and sales_position = 'SALES'
			) c on c.customer_no=a.customer_no
		join -- 员工信息
			(
			select 
				employee_code,employee_name,begin_date
			from 
				csx_dw.dws_basic_w_a_employee_org_m
			where 
				--sdt = ${hiveconf:current_end_day}
				sdt = '20210922'
				and emp_status='on'
			) d on d.employee_code=c.work_no			
	where
		a.sales_value / (datediff(b.normal_last_order_date,b.normal_first_order_date)+1)>=1000 --筛选当月平均日出库金额>=1000元客户
	)tmp1	
group by
	concat_ws('-', ${hiveconf:current_start_day},${hiveconf:current_end_day}),
	sales_region_name,sales_province_name,sales_city_name,first_supervisor_work_no,first_supervisor_name,work_no,sales_name
; 


--==========================================================================================
--05_销售员激励案_销售之星_日配激励案_业绩之星

insert overwrite directory '/tmp/zhangyanpeng/20210813_05' row format delimited fields terminated by '\t'
	
select
	concat(${hiveconf:current_start_day},'-',${hiveconf:current_end_day}) as period,
	c.sales_region_name,c.sales_province_name,c.sales_city_name,
	d.leader_code,d.leader_name,
	c.work_no,c.sales_name,d.begin_date,
	a.sales_value,a.front_profit,a.front_profit_rate,coalesce(b.sales_value,0) as last_month_sales_value,
	a.sales_value-coalesce(b.sales_value,0) as growth,
	row_number()over(order by a.sales_value-coalesce(b.sales_value,0) desc) as rn
from	
	(
	select
		work_no,
		sum(sales_value) as sales_value, --含税销售额
		sum(profit) as profit, --含税毛利额
		sum(front_profit) as front_profit, --前端含税毛利
		sum(profit)/abs(sum(sales_value)) as profit_rate,
		sum(front_profit)/abs(sum(sales_value)) as front_profit_rate
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
		and channel_code in ('1','7','9') 
		and business_type_code in ('1') 
		and customer_no not in ('118849','118365','114646','118215','120645','115237','113387') --剔除部分客户
		-- 20210918 剔除部分客户
		and customer_no not in ('117529','118693','118892','118897','118907','118911','118912','118913','118915','118918','119032','120123','121015','120704','119925','110931',
		'112735','118136','116205','117015','115936','109544','112088','108105','108201','110898','106306','106320','106330','106298','106325','117108','106321','106326','106283',
		'106299','106309','120024','119990','106805','118072') -- 20210918 剔除部分客户	
		-- 20210923 剔除部分客户
		and customer_no not in ('121112','121109','120666','120360','110807','110807','110807','110807','110807','115971','120360','117255','117814','110807','115643')	
		and order_no not in (
		'OM21092500022852','OM21092500018389','OM21091600021970','OM21091800011804','OM21093000003441','OM21081800009334','OM21073000033690','OM21081800016201',
		'OM21072800045844','OM21072700002361','OM21072800045333','OM21072700002378','OM21072700002277','OM21072700002338','OM21091100000260','OM21082300007563')
		-- 20211012 剔除部分客户
		and customer_no not in ('122230','115300','117197','121880','109529','121574')		
	group by
		work_no
	having
		sum(front_profit)/abs(sum(sales_value))>=0.05
	) a 
	left join
		(
		select
			work_no,
			sum(sales_value) as sales_value, --含税销售额
			sum(profit) as profit, --含税毛利额
			sum(front_profit) as front_profit, --前端含税毛利
			sum(profit)/abs(sum(sales_value)) as profit_rate
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt between ${hiveconf:last_month_start_day} and ${hiveconf:last_month_end_day}
			and channel_code in ('1','7','9') 
			and business_type_code in ('1') 
			and customer_no not in ('118849','118365','114646','118215','120645','115237','113387') --剔除部分客户	
			-- 20210918 剔除部分客户
			and customer_no not in ('117529','118693','118892','118897','118907','118911','118912','118913','118915','118918','119032','120123','121015','120704','119925','110931',
			'112735','118136','116205','117015','115936','109544','112088','108105','108201','110898','106306','106320','106330','106298','106325','117108','106321','106326','106283',
			'106299','106309','120024','119990','106805','118072') -- 20210918 剔除部分客户	
			-- 20210923 剔除部分客户
			and customer_no not in ('121112','121109','120666','120360','110807','110807','110807','110807','110807','115971','120360','117255','117814','110807','115643')	
			and order_no not in (
			'OM21092500022852','OM21092500018389','OM21091600021970','OM21091800011804','OM21093000003441','OM21081800009334','OM21073000033690','OM21081800016201',
			'OM21072800045844','OM21072700002361','OM21072800045333','OM21072700002378','OM21072700002277','OM21072700002338','OM21091100000260','OM21082300007563')
			-- 20211012 剔除部分客户
			and customer_no not in ('122230','115300','117197','121880','109529','121574')			
		group by
			work_no
		) b on b.work_no=a.work_no
	join 
		(
		select 
			distinct work_no,sales_name, --first_supervisor_work_no,first_supervisor_name,
			sales_region_name,sales_province_name,sales_city_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			--sdt = ${hiveconf:current_end_day}
			sdt = '20210922'
			and customer_no<>''	
			and sales_position = 'SALES'
			and sales_province_name not like '%平台%'
			and sales_province_name not like '%BBC%'
		) c on c.work_no=a.work_no
	join
		(
		select
			employee_code,employee_name,begin_date,end_date,emp_status,leader_code,leader_name
		from
			csx_dw.dws_basic_w_a_employee_org_m
		where
			--sdt = ${hiveconf:current_end_day}
			sdt = '20210922'
			and emp_status='on'
		) d on d.employee_code=a.work_no
where
	a.sales_value-coalesce(b.sales_value,0)>=50000 --月环比增长销售额≥5万元
; 


--==========================================================================================
--06_销售员激励案_销售之星_日配激励案_毛利之星

insert overwrite directory '/tmp/zhangyanpeng/20210813_06' row format delimited fields terminated by '\t'
	
select
	concat( ${hiveconf:current_start_day},'-',${hiveconf:current_end_day}) as period,
	c.sales_region_name,c.sales_province_name,c.sales_city_name,
	d.leader_code,d.leader_name,
	c.work_no,c.sales_name,d.begin_date,
	a.sales_value,a.front_profit,a.front_profit_rate,coalesce(b.front_profit,0) as last_month_front_profit,
	a.front_profit-coalesce(b.front_profit,0) as growth,
	row_number()over(order by a.front_profit-coalesce(b.front_profit,0) desc) as rn
from	
	(
	select
		work_no,
		sum(sales_value) as sales_value, --含税销售额
		sum(profit) as profit, --含税毛利额
		sum(front_profit) as front_profit, --前端含税毛利
		sum(profit)/abs(sum(sales_value)) as profit_rate,
		sum(front_profit)/abs(sum(sales_value)) as front_profit_rate
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
		and channel_code in ('1','7','9') 
		and business_type_code in ('1') 
		and customer_no not in ('118849','118365','114646','118215','120645','115237','113387') --剔除部分客户	
		-- 20210918 剔除部分客户
		and customer_no not in ('117529','118693','118892','118897','118907','118911','118912','118913','118915','118918','119032','120123','121015','120704','119925','110931',
		'112735','118136','116205','117015','115936','109544','112088','108105','108201','110898','106306','106320','106330','106298','106325','117108','106321','106326','106283',
		'106299','106309','120024','119990','106805','118072') -- 20210918 剔除部分客户	
		-- 20210923 剔除部分客户
		and customer_no not in ('121112','121109','120666','120360','110807','110807','110807','110807','110807','115971','120360','117255','117814','110807','115643')	
		and order_no not in (
		'OM21092500022852','OM21092500018389','OM21091600021970','OM21091800011804','OM21093000003441','OM21081800009334','OM21073000033690','OM21081800016201',
		'OM21072800045844','OM21072700002361','OM21072800045333','OM21072700002378','OM21072700002277','OM21072700002338','OM21091100000260','OM21082300007563')
		-- 20211012 剔除部分客户
		and customer_no not in ('122230','115300','117197','121880','109529','121574')		
	group by
		work_no
	having
		sum(front_profit)>=0
	) a 
	left join
		(
		select
			work_no,
			sum(sales_value) as sales_value, --含税销售额
			sum(profit) as profit, --含税毛利额
			sum(front_profit) as front_profit, --前端含税毛利
			sum(profit)/abs(sum(sales_value)) as profit_rate
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt between ${hiveconf:last_month_start_day} and ${hiveconf:last_month_end_day}
			and channel_code in ('1','7','9') 
			and business_type_code in ('1') 
			and customer_no not in ('118849','118365','114646','118215','120645','115237','113387') --剔除部分客户
			-- 20210918 剔除部分客户
			and customer_no not in ('117529','118693','118892','118897','118907','118911','118912','118913','118915','118918','119032','120123','121015','120704','119925','110931',
			'112735','118136','116205','117015','115936','109544','112088','108105','108201','110898','106306','106320','106330','106298','106325','117108','106321','106326','106283',
			'106299','106309','120024','119990','106805','118072') -- 20210918 剔除部分客户
			-- 20210923 剔除部分客户
			and customer_no not in ('121112','121109','120666','120360','110807','110807','110807','110807','110807','115971','120360','117255','117814','110807','115643')
			and order_no not in (
			'OM21092500022852','OM21092500018389','OM21091600021970','OM21091800011804','OM21093000003441','OM21081800009334','OM21073000033690','OM21081800016201',
			'OM21072800045844','OM21072700002361','OM21072800045333','OM21072700002378','OM21072700002277','OM21072700002338','OM21091100000260','OM21082300007563')
			-- 20211012 剔除部分客户
			and customer_no not in ('122230','115300','117197','121880','109529','121574')
		group by
			work_no
		having
			sum(front_profit)>=0
		) b on b.work_no=a.work_no
	join 
		(
		select 
			distinct work_no,sales_name, -- first_supervisor_work_no,first_supervisor_name,
			sales_region_name,sales_province_name,sales_city_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			--sdt = ${hiveconf:current_end_day}
			sdt = '20210922'
			and customer_no<>''	
			and sales_position = 'SALES'
			and sales_province_name not like '%平台%'
			and sales_province_name not like '%BBC%'
		) c on c.work_no=a.work_no
	join
		(
		select
			employee_code,employee_name,begin_date,end_date,emp_status,leader_code,leader_name
		from
			csx_dw.dws_basic_w_a_employee_org_m
		where
			--sdt=${hiveconf:current_end_day}
			sdt = '20210922'
			and emp_status='on'
		) d on d.employee_code=a.work_no
where
	a.front_profit-coalesce(b.front_profit,0)>=30000 --月环比增长销售额≥3万元
; 


--==========================================================================================
--07_销售员激励案_销售之星_日配激励案_回款之星

insert overwrite directory '/tmp/zhangyanpeng/20210813_07' row format delimited fields terminated by '\t'

select
	${hiveconf:current_end_day} as period,
	c.sales_region_name,c.sales_province_name,c.sales_city_name,c.first_supervisor_work_no,c.first_supervisor_name,a.work_no,a.sales_name,d.begin_date,
	coalesce(e.payment_amount,0) as payment_amount,b.sales_type,a.overdue_coefficient,
	row_number()over(partition by b.sales_type order by a.overdue_coefficient asc,coalesce(e.payment_amount,0) desc) as rn
from
	(
	select
		work_no,sales_name,
		sum(receivable_amount) as receivable_amount,
		sum(overdue_coefficient_numerator) as overdue_coefficient_numerator,
		sum(overdue_coefficient_denominator) as overdue_coefficient_denominator,
		case when sum(receivable_amount) <= 1 then 0.00 
			else coalesce(round(if(sum(overdue_coefficient_numerator)<0,0,sum(overdue_coefficient_numerator))
				/sum(overdue_coefficient_denominator),6),0.00)
		end as overdue_coefficient -- 逾期系数
	from
		(
		select
			customer_no,customer_name,company_code,company_name,work_no,sales_name,province_code,province_name,city_code,city_name,
			receivable_amount,overdue_coefficient_numerator,overdue_coefficient_denominator
		from
			csx_dw.dws_sss_r_a_customer_accounts
		where
			sdt=${hiveconf:current_end_day}
		) t1
	group by 
		work_no,sales_name
	having
		case when sum(receivable_amount) <= 1 then 0.00 
			else coalesce(round(if(sum(overdue_coefficient_numerator)<0,0,sum(overdue_coefficient_numerator))
				/sum(overdue_coefficient_denominator),6),0.00)
		end<0.5	-- 逾期系数＜0.5的销售员参与逾期率排名
	) a
	join
		(
		select
			work_no,sales_name,
			sum(sales_value) as sales_value, --含税销售额
			case when sum(sales_value)>=1000000 then '100万+'
				when sum(sales_value)>=500000 then '50万-100万'
				when sum(sales_value)>=100000 then '10万-50万'
				else '其他'
			end as sales_type
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
			and channel_code in ('1','7','9') 
			and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and customer_no not in ('118849','118365','114646','118215','120645','115237','113387') --剔除部分客户
			-- 20210918 剔除部分客户
			and customer_no not in ('117529','118693','118892','118897','118907','118911','118912','118913','118915','118918','119032','120123','121015','120704','119925','110931',
			'112735','118136','116205','117015','115936','109544','112088','108105','108201','110898','106306','106320','106330','106298','106325','117108','106321','106326','106283',
			'106299','106309','120024','119990','106805','118072') -- 20210918 剔除部分客户
			-- 20210923 剔除部分客户
			and customer_no not in ('121112','121109','120666','120360','110807','110807','110807','110807','110807','115971','120360','117255','117814','110807','115643')
			and order_no not in (
			'OM21092500022852','OM21092500018389','OM21091600021970','OM21091800011804','OM21093000003441','OM21081800009334','OM21073000033690','OM21081800016201',
			'OM21072800045844','OM21072700002361','OM21072800045333','OM21072700002378','OM21072700002277','OM21072700002338','OM21091100000260','OM21082300007563')
			-- 20211012 剔除部分客户
			and customer_no not in ('122230','115300','117197','121880','109529','121574')
		group by
			work_no,sales_name
		having
			sum(sales_value)>=100000
		) b on b.work_no=a.work_no
	join 
		(
		select
			work_no,sales_name,first_supervisor_work_no,first_supervisor_name,sales_region_name,sales_province_name,sales_city_name
		from
			(
			select 
				work_no,sales_name,first_supervisor_work_no,first_supervisor_name,sales_region_name,sales_province_name,sales_city_name,
				row_number()over(partition by work_no order by create_time desc) as rn
			from 
				csx_dw.dws_crm_w_a_customer
			where 
				--sdt = ${hiveconf:current_end_day}
				sdt = '20210922'
				and customer_no<>''	
				and sales_position = 'SALES'
				and sales_province_name not like '%平台%'
				and sales_province_name not like '%BBC%'
			) t1
		where
			rn=1
		) c on c.work_no=a.work_no	
	join
		(
		select
			employee_code,employee_name,begin_date,end_date,emp_status
		from
			csx_dw.dws_basic_w_a_employee_org_m
		where
			--sdt=${hiveconf:current_end_day}
			sdt = '20210922'
			and emp_status='on'
		) d on d.employee_code=a.work_no
	left join
		(
		select
			b.work_no,
			sum(a.payment_amount) as payment_amount
		from
			(	
			select -- 回款
				customer_code,payment_amount,paid_time
			from
				csx_dw.dwd_sss_r_d_close_bill_account_record_20200908
			where 
				regexp_replace(substr(paid_time,1,10),'-','') >=${hiveconf:current_start_day} 
				and regexp_replace(substr(paid_time,1,10),'-','') <=${hiveconf:current_end_day}
				and is_deleted ='0'
				and money_back_id<>'0' --回款关联ID为0是微信支付、-1是退货系统核销
			) a 
			left join
				(
				select 
					customer_no,work_no
				from 
					csx_dw.dws_crm_w_a_customer
				where
					--sdt=${hiveconf:current_end_day}
					sdt = '20210922'
				group by 
					customer_no,work_no
				) b on b.customer_no=a.customer_code
		group by 
			b.work_no
		) e on e.work_no=a.work_no
;



--==========================================================================================
--10_销售主管激励案_主管竞赛_日配&福利&BBC_拓客绩优

insert overwrite directory '/tmp/zhangyanpeng/20210813_10' row format delimited fields terminated by '\t'

select
	concat(${hiveconf:current_start_day},'-',${hiveconf:current_end_day}) as period,
	sales_region_name,sales_province_name,sales_city_name,supervisor_work_no,supervisor_name,
	sum(sales_value) as sales_value,
	sum(front_profit) as front_profit,
	sum(front_profit) / abs(sum(sales_value)) as front_profit_rate,
	count(distinct customer_no) as cust_count,
	sum(avg_sales_value) as avg_sales_value,
	row_number()over(order by count(distinct customer_no) desc,sum(sales_value) desc) as rn
from	
	(	
	select
		customer_no,sales_region_name,sales_province_name,sales_city_name,supervisor_work_no,supervisor_name,
		sales_value,profit,front_profit,profit_rate,
		avg_sales_value
	from
		(
		select	-- 日配业务
			a.customer_no,c.sales_region_name,c.sales_province_name,c.sales_city_name,a.supervisor_work_no,a.supervisor_name,
			a.sales_value,a.profit,a.front_profit,a.profit_rate,
			a.sales_value / (datediff(b.normal_last_order_date,b.normal_first_order_date)+1) as avg_sales_value
		from	
			(
			select
				customer_no,if(work_no='81095965',work_no,supervisor_work_no) as supervisor_work_no,if(work_no='81095965',sales_name,supervisor_name) as supervisor_name,
				sum(sales_value) as sales_value, --含税销售额
				sum(profit) as profit, --含税毛利额
				sum(front_profit) as front_profit, --前端含税毛利
				sum(profit)/abs(sum(sales_value)) as profit_rate
			from 
				csx_dw.dws_sale_r_d_detail
			where 
				sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
				and channel_code in ('1','7','9') 
				and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
				and supervisor_work_no !=''
				and customer_no not in ('118849','118365','114646','118215','120645','115237','113387') --剔除部分客户
				-- 20210918 剔除部分客户
				and customer_no not in ('117529','118693','118892','118897','118907','118911','118912','118913','118915','118918','119032','120123','121015','120704','119925','110931',
				'112735','118136','116205','117015','115936','109544','112088','108105','108201','110898','106306','106320','106330','106298','106325','117108','106321','106326','106283',
				'106299','106309','120024','119990','106805','118072') -- 20210918 剔除部分客户
				-- 20210923 剔除部分客户
				and customer_no not in ('121112','121109','120666','120360','110807','110807','110807','110807','110807','115971','120360','117255','117814','110807','115643')
				and order_no not in (
				'OM21092500022852','OM21092500018389','OM21091600021970','OM21091800011804','OM21093000003441','OM21081800009334','OM21073000033690','OM21081800016201',
				'OM21072800045844','OM21072700002361','OM21072800045333','OM21072700002378','OM21072700002277','OM21072700002338','OM21091100000260','OM21082300007563')
				-- 20211012 剔除部分客户
				and customer_no not in ('122230','115300','117197','121880','109529','121574')
			group by
				customer_no,if(work_no='81095965',work_no,supervisor_work_no),if(work_no='81095965',sales_name,supervisor_name)
			having
				sum(sales_value)>=30000 -- 月履约金额大于等于3万
				and sum(front_profit)/abs(sum(sales_value))>=0.03 --新履约客户的前端毛利率≥3%
			) a 
			join
				(
				select 
					customer_no, 
					to_date(from_unixtime(unix_timestamp(normal_first_order_date,'yyyyMMdd'))) as normal_first_order_date,
					to_date(from_unixtime(unix_timestamp(normal_last_order_date,'yyyyMMdd'))) as normal_last_order_date
				from 
					csx_dw.dws_crm_w_a_customer_active
				where 
					sdt = ${hiveconf:current_end_day}
					and normal_first_order_date between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}		
				) b on b.customer_no=a.customer_no
			join 
				(
				select 
					customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,sales_region_name,sales_province_name,sales_city_name
				from 
					csx_dw.dws_crm_w_a_customer
				where 
					--sdt = ${hiveconf:current_end_day}
					sdt = '20210922'
					and customer_no<>''	
					--and sales_position = 'SALES'
				) c on c.customer_no=a.customer_no			
		where
			a.sales_value / (datediff(b.normal_last_order_date,b.normal_first_order_date)+1)>=1000 --筛选当月平均日出库金额>=1000元客户
			
		union all
		
		select	--福利业务
			a.customer_no,c.sales_region_name,c.sales_province_name,c.sales_city_name,a.supervisor_work_no,a.supervisor_name,
			a.sales_value,a.profit,a.front_profit,a.profit_rate,
			a.sales_value / (datediff(b.welfare_last_order_date,b.welfare_first_order_date)+1) as avg_sales_value
		from	
			(
			select
				customer_no,if(work_no='81095965',work_no,supervisor_work_no) as supervisor_work_no,if(work_no='81095965',sales_name,supervisor_name) as supervisor_name,
				sum(sales_value) as sales_value, --含税销售额
				sum(profit) as profit, --含税毛利额
				sum(front_profit) as front_profit, --前端含税毛利
				sum(profit)/abs(sum(sales_value)) as profit_rate
			from 
				csx_dw.dws_sale_r_d_detail
			where 
				sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
				and channel_code in ('1','7','9') 
				and business_type_code in ('2') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
				and supervisor_work_no !=''
				and customer_no not in ('118849','118365','114646','118215','120645','115237','113387') --剔除部分客户
				-- 20210918 剔除部分客户
				and customer_no not in ('117529','118693','118892','118897','118907','118911','118912','118913','118915','118918','119032','120123','121015','120704','119925','110931',
				'112735','118136','116205','117015','115936','109544','112088','108105','108201','110898','106306','106320','106330','106298','106325','117108','106321','106326','106283',
				'106299','106309','120024','119990','106805','118072') -- 20210918 剔除部分客户
				-- 20210923 剔除部分客户
				and customer_no not in ('121112','121109','120666','120360','110807','110807','110807','110807','110807','115971','120360','117255','117814','110807','115643')
				and order_no not in (
				'OM21092500022852','OM21092500018389','OM21091600021970','OM21091800011804','OM21093000003441','OM21081800009334','OM21073000033690','OM21081800016201',
				'OM21072800045844','OM21072700002361','OM21072800045333','OM21072700002378','OM21072700002277','OM21072700002338','OM21091100000260','OM21082300007563')
				-- 20211012 剔除部分客户
				and customer_no not in ('122230','115300','117197','121880','109529','121574')
			group by
				customer_no,if(work_no='81095965',work_no,supervisor_work_no),if(work_no='81095965',sales_name,supervisor_name)
			having
				sum(sales_value)>=10000 -- 月履约金额大于等于1万
				and sum(front_profit)/abs(sum(sales_value))>=0.03 --新履约客户的前端毛利率≥3%
			) a 
			join
				(
				select 
					customer_no, 
					to_date(from_unixtime(unix_timestamp(welfare_first_order_date,'yyyyMMdd'))) as welfare_first_order_date,
					to_date(from_unixtime(unix_timestamp(welfare_last_order_date,'yyyyMMdd'))) as welfare_last_order_date
				from 
					csx_dw.dws_crm_w_a_customer_active
				where 
					sdt = ${hiveconf:current_end_day}
					and welfare_first_order_date between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}		
				) b on b.customer_no=a.customer_no
			join 
				(
				select 
					customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,sales_region_name,sales_province_name,sales_city_name
				from 
					csx_dw.dws_crm_w_a_customer
				where 
					--sdt = ${hiveconf:current_end_day}
					sdt = '20210922'
					and customer_no<>''	
					--and sales_position = 'SALES'
				) c on c.customer_no=a.customer_no		
		--福利客户不限制日均履约金额
		--where
		--	a.sales_value / (datediff(b.welfare_last_order_date,b.welfare_first_order_date)+1)>=1000 --筛选当月平均日出库金额>=1000元客户
		union all
		
		select	-- BBC业务
			a.customer_no,c.sales_region_name,c.sales_province_name,c.sales_city_name,a.supervisor_work_no,a.supervisor_name,
			a.sales_value,a.profit,a.front_profit,a.profit_rate,
			a.sales_value / (datediff(b.bbc_last_order_date,b.bbc_first_order_date)+1) as avg_sales_value
		from	
			(
			select
				customer_no,if(work_no='81095965',work_no,supervisor_work_no) as supervisor_work_no,if(work_no='81095965',sales_name,supervisor_name) as supervisor_name,
				sum(sales_value) as sales_value, --含税销售额
				sum(profit) as profit, --含税毛利额
				sum(front_profit) as front_profit, --前端含税毛利
				sum(profit)/abs(sum(sales_value)) as profit_rate
			from 
				csx_dw.dws_sale_r_d_detail
			where 
				sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
				and channel_code in ('1','7','9') 
				and business_type_code in ('6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
				and supervisor_work_no !=''
				and customer_no not in ('118849','118365','114646','118215','120645','115237','113387') --剔除部分客户
				-- 20210918 剔除部分客户
				and customer_no not in ('117529','118693','118892','118897','118907','118911','118912','118913','118915','118918','119032','120123','121015','120704','119925','110931',
				'112735','118136','116205','117015','115936','109544','112088','108105','108201','110898','106306','106320','106330','106298','106325','117108','106321','106326','106283',
				'106299','106309','120024','119990','106805','118072') -- 20210918 剔除部分客户
				-- 20210923 剔除部分客户
				and customer_no not in ('121112','121109','120666','120360','110807','110807','110807','110807','110807','115971','120360','117255','117814','110807','115643')
				and order_no not in (
				'OM21092500022852','OM21092500018389','OM21091600021970','OM21091800011804','OM21093000003441','OM21081800009334','OM21073000033690','OM21081800016201',
				'OM21072800045844','OM21072700002361','OM21072800045333','OM21072700002378','OM21072700002277','OM21072700002338','OM21091100000260','OM21082300007563')
				-- 20211012 剔除部分客户
				and customer_no not in ('122230','115300','117197','121880','109529','121574')
			group by
				customer_no,if(work_no='81095965',work_no,supervisor_work_no),if(work_no='81095965',sales_name,supervisor_name)
			having
				-- sum(sales_value)>=30000 -- 月履约金额大于等于3万
				sum(front_profit)/abs(sum(sales_value))>=0.03 --新履约客户的前端毛利率≥3%
			) a 
			join
				(
				select 
					customer_no, 
					to_date(from_unixtime(unix_timestamp(bbc_first_order_date,'yyyyMMdd'))) as bbc_first_order_date,
					to_date(from_unixtime(unix_timestamp(bbc_last_order_date,'yyyyMMdd'))) as bbc_last_order_date
				from 
					csx_dw.dws_crm_w_a_customer_active
				where 
					sdt = ${hiveconf:current_end_day}
					and bbc_first_order_date between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}		
				) b on b.customer_no=a.customer_no
			join 
				(
				select 
					customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,sales_region_name,sales_province_name,sales_city_name
				from 
					csx_dw.dws_crm_w_a_customer
				where 
					--sdt = ${hiveconf:current_end_day}
					sdt = '20210922'
					and customer_no<>''	
					--and sales_position = 'SALES'
				) c on c.customer_no=a.customer_no			
		where
			a.sales_value / (datediff(b.bbc_last_order_date,b.bbc_first_order_date)+1)>=700 --筛选当月平均日出库金额>=700元客户
		
		union all
		select	-- 城市服务商业务
			a.customer_no,c.sales_region_name,c.sales_province_name,c.sales_city_name,a.supervisor_work_no,a.supervisor_name,
			a.sales_value,a.profit,a.front_profit,a.profit_rate,
			a.sales_value / (datediff(b.last_order_date,b.first_order_date)+1) as avg_sales_value
		from	
			(
			select
				customer_no,if(work_no='81095965',work_no,supervisor_work_no) as supervisor_work_no,if(work_no='81095965',sales_name,supervisor_name) as supervisor_name,
				sum(sales_value) as sales_value, --含税销售额
				sum(profit) as profit, --含税毛利额
				sum(front_profit) as front_profit, --前端含税毛利
				sum(profit)/abs(sum(sales_value)) as profit_rate
			from 
				csx_dw.dws_sale_r_d_detail
			where 
				sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
				and channel_code in ('1','7','9') 
				and business_type_code in ('4') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
				and supervisor_work_no in ('80886641','80972242')
				and supervisor_work_no !=''
				and customer_no not in ('118849','118365','114646','118215','120645','115237','113387') --剔除部分客户
				-- 20210918 剔除部分客户
				and customer_no not in ('117529','118693','118892','118897','118907','118911','118912','118913','118915','118918','119032','120123','121015','120704','119925','110931',
				'112735','118136','116205','117015','115936','109544','112088','108105','108201','110898','106306','106320','106330','106298','106325','117108','106321','106326','106283',
				'106299','106309','120024','119990','106805','118072') -- 20210918 剔除部分客户
				-- 20210923 剔除部分客户
				and customer_no not in ('121112','121109','120666','120360','110807','110807','110807','110807','110807','115971','120360','117255','117814','110807','115643')
				and order_no not in (
				'OM21092500022852','OM21092500018389','OM21091600021970','OM21091800011804','OM21093000003441','OM21081800009334','OM21073000033690','OM21081800016201',
				'OM21072800045844','OM21072700002361','OM21072800045333','OM21072700002378','OM21072700002277','OM21072700002338','OM21091100000260','OM21082300007563')
				-- 20211012 剔除部分客户
				and customer_no not in ('122230','115300','117197','121880','109529','121574')
			group by
				customer_no,if(work_no='81095965',work_no,supervisor_work_no),if(work_no='81095965',sales_name,supervisor_name)
			having
				sum(sales_value)>=30000 -- 月履约金额大于等于3万
				and sum(front_profit)/abs(sum(sales_value))>=0.03 --新履约客户的前端毛利率≥3%
			) a 
			join
				(
				select 
					customer_no, 
					to_date(from_unixtime(unix_timestamp(first_order_date,'yyyyMMdd'))) as first_order_date,
					to_date(from_unixtime(unix_timestamp(last_order_date,'yyyyMMdd'))) as last_order_date
				from 
					csx_dw.dws_crm_w_a_customer_active
				where 
					sdt = ${hiveconf:current_end_day}
					and first_order_date between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}		
				) b on b.customer_no=a.customer_no
			join 
				(
				select 
					customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,sales_region_name,sales_province_name,sales_city_name
				from 
					csx_dw.dws_crm_w_a_customer
				where 
					--sdt = ${hiveconf:current_end_day}
					sdt = '20210922'
					and customer_no<>''	
					--and sales_position = 'SALES'
				) c on c.customer_no=a.customer_no			
		where
			a.sales_value / (datediff(b.last_order_date,b.first_order_date)+1)>=1000 --筛选当月平均日出库金额>=1000元客户
		)t1
		join -- 员工信息
			(
			select 
				employee_code,employee_name,begin_date
			from 
				csx_dw.dws_basic_w_a_employee_org_m
			where 
				--sdt = ${hiveconf:current_end_day}
				sdt = '20210922'
				and emp_status='on'
			) tmp1 on tmp1.employee_code=t1.supervisor_work_no
	)t2
group by
	concat_ws('-', ${hiveconf:current_start_day},${hiveconf:current_end_day}),
	sales_region_name,sales_province_name,sales_city_name,supervisor_work_no,supervisor_name
; 


--==========================================================================================
--11_销售主管激励案_绩优主管竞赛_日配&福利&BBC_业绩绩优主管

insert overwrite directory '/tmp/zhangyanpeng/20210813_11' row format delimited fields terminated by '\t'
	
select
	concat(${hiveconf:current_start_day},'-',${hiveconf:current_end_day}) as period,
	c.sales_region_name,c.sales_province_name,c.sales_city_name,a.supervisor_work_no,a.supervisor_name,d.begin_date,
	a.sales_value,a.front_profit,a.front_profit_rate,coalesce(b.sales_value,0) as last_month_sales_value,
	a.sales_value-coalesce(b.sales_value,0) as growth,
	row_number()over(order by a.sales_value-coalesce(b.sales_value,0) desc) as rn
from	
	(
	select
		if(work_no='81095965',work_no,supervisor_work_no) as supervisor_work_no,if(work_no='81095965',sales_name,supervisor_name) as supervisor_name,
		sum(sales_value) as sales_value, --含税销售额
		sum(profit) as profit, --含税毛利额
		sum(front_profit) as front_profit, --前端含税毛利
		sum(profit)/abs(sum(sales_value)) as profit_rate,
		sum(front_profit)/abs(sum(sales_value)) as front_profit_rate
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
		and channel_code in ('1','7','9') 
		and (business_type_code in ('1','2','6') or (business_type_code in ('4') and supervisor_work_no in ('80886641','80972242')))
		and supervisor_work_no !=''
		and customer_no not in ('118849','118365','114646','118215','120645','115237','113387') --剔除部分客户
		-- 20210918 剔除部分客户
		and customer_no not in ('117529','118693','118892','118897','118907','118911','118912','118913','118915','118918','119032','120123','121015','120704','119925','110931',
		'112735','118136','116205','117015','115936','109544','112088','108105','108201','110898','106306','106320','106330','106298','106325','117108','106321','106326','106283',
		'106299','106309','120024','119990','106805','118072') -- 20210918 剔除部分客户
		-- 20210923 剔除部分客户
		and customer_no not in ('121112','121109','120666','120360','110807','110807','110807','110807','110807','115971','120360','117255','117814','110807','115643')
		and order_no not in (
		'OM21092500022852','OM21092500018389','OM21091600021970','OM21091800011804','OM21093000003441','OM21081800009334','OM21073000033690','OM21081800016201',
		'OM21072800045844','OM21072700002361','OM21072800045333','OM21072700002378','OM21072700002277','OM21072700002338','OM21091100000260','OM21082300007563')
		-- 20211012 剔除部分客户
		and customer_no not in ('122230','115300','117197','121880','109529','121574')
	group by
		if(work_no='81095965',work_no,supervisor_work_no),if(work_no='81095965',sales_name,supervisor_name)
	having
		sum(front_profit)/abs(sum(sales_value))>=0.05
	) a 
	left join
		(
		select
			if(work_no='81095965',work_no,supervisor_work_no) as supervisor_work_no,if(work_no='81095965',sales_name,supervisor_name) as supervisor_name,
			sum(sales_value) as sales_value, --含税销售额
			sum(profit) as profit, --含税毛利额
			sum(front_profit) as front_profit, --前端含税毛利
			sum(profit)/abs(sum(sales_value)) as profit_rate
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt between ${hiveconf:last_month_start_day} and ${hiveconf:last_month_end_day}
			and channel_code in ('1','7','9') 
			and business_type_code in ('1','2','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and supervisor_work_no !=''
			and customer_no not in ('118849','118365','114646','118215','120645','115237','113387') --剔除部分客户
			-- 20210918 剔除部分客户
			and customer_no not in ('117529','118693','118892','118897','118907','118911','118912','118913','118915','118918','119032','120123','121015','120704','119925','110931',
			'112735','118136','116205','117015','115936','109544','112088','108105','108201','110898','106306','106320','106330','106298','106325','117108','106321','106326','106283',
			'106299','106309','120024','119990','106805','118072') -- 20210918 剔除部分客户
			-- 20210923 剔除部分客户
			and customer_no not in ('121112','121109','120666','120360','110807','110807','110807','110807','110807','115971','120360','117255','117814','110807','115643')
			and order_no not in (
			'OM21092500022852','OM21092500018389','OM21091600021970','OM21091800011804','OM21093000003441','OM21081800009334','OM21073000033690','OM21081800016201',
			'OM21072800045844','OM21072700002361','OM21072800045333','OM21072700002378','OM21072700002277','OM21072700002338','OM21091100000260','OM21082300007563')
			-- 20211012 剔除部分客户
			and customer_no not in ('122230','115300','117197','121880','109529','121574')
		group by
			if(work_no='81095965',work_no,supervisor_work_no),if(work_no='81095965',sales_name,supervisor_name)
		) b on b.supervisor_work_no=a.supervisor_work_no
	join 
		(
		select
			first_supervisor_work_no,first_supervisor_name,sales_region_name,sales_province_name,sales_city_name
		from
			(
			select 
				first_supervisor_work_no,first_supervisor_name,sales_region_name,sales_province_name,sales_city_name,
				row_number()over(partition by first_supervisor_work_no order by create_time desc) as rn
			from 
				csx_dw.dws_crm_w_a_customer
			where 
				--sdt = ${hiveconf:current_end_day}
				sdt = '20210922'
				and customer_no<>''	
				--and sales_position = 'SALES'
				and sales_province_name not like '%平台%'
				and sales_province_name not like '%BBC%'
			) t1
		where
			rn=1
		) c on c.first_supervisor_work_no=a.supervisor_work_no
	join
		(
		select
			employee_code,employee_name,begin_date,end_date,emp_status
		from
			csx_dw.dws_basic_w_a_employee_org_m
		where
			--sdt=${hiveconf:current_end_day}
			sdt = '20210922'
			and emp_status='on'
		) d on d.employee_code=a.supervisor_work_no
where
	a.sales_value-coalesce(b.sales_value,0)>=50000 --月环比增长销售额≥5万元
; 

--==========================================================================================
--12_销售主管激励案_绩优主管竞赛_日配&福利&BBC_人效绩优主管

insert overwrite directory '/tmp/zhangyanpeng/20210813_12' row format delimited fields terminated by '\t'
	
select
	concat(${hiveconf:current_start_day},'-',${hiveconf:current_end_day}) as period,
	a.sales_region_name,a.sales_province_name,a.sales_city_name,a.first_supervisor_work_no,a.first_supervisor_name,d.begin_date,
	a.sales_value,a.front_profit,a.front_profit_rate,a.sales_cnt,
	a.avg_sales_value,
	row_number()over(order by a.avg_sales_value desc,a.sales_value desc) as rn
from	
	(
	select
		c.first_supervisor_work_no,c.first_supervisor_name,c.sales_region_name,c.sales_province_name,c.sales_city_name,
		sum(a.sales_value) as sales_value, --含税销售额
		sum(a.profit) as profit, --含税毛利额
		sum(a.front_profit) as front_profit, --前端含税毛利
		sum(a.profit)/abs(sum(a.sales_value)) as profit_rate,
		sum(a.front_profit)/abs(sum(a.sales_value)) as front_profit_rate,
		count(distinct a.work_no) as sales_cnt,
		sum(a.sales_value)/count(distinct a.work_no) as avg_sales_value
	from
		(
		select
			sdt,work_no,sales_value,profit,front_profit
		from
			csx_dw.dws_sale_r_d_detail
		where 
			sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
			and channel_code in ('1','7','9') 
			and (business_type_code in ('1','2','6') or (business_type_code in ('4') and supervisor_work_no in ('80886641','80972242')))
			and supervisor_work_no !=''
			and customer_no not in ('118849','118365','114646','118215','120645','115237','113387') --剔除部分客户
			-- 20210918 剔除部分客户
			and customer_no not in ('117529','118693','118892','118897','118907','118911','118912','118913','118915','118918','119032','120123','121015','120704','119925','110931',
			'112735','118136','116205','117015','115936','109544','112088','108105','108201','110898','106306','106320','106330','106298','106325','117108','106321','106326','106283',
			'106299','106309','120024','119990','106805','118072') -- 20210918 剔除部分客户
			-- 20210923 剔除部分客户
			and customer_no not in ('121112','121109','120666','120360','110807','110807','110807','110807','110807','115971','120360','117255','117814','110807','115643')
			and order_no not in (
			'OM21092500022852','OM21092500018389','OM21091600021970','OM21091800011804','OM21093000003441','OM21081800009334','OM21073000033690','OM21081800016201',
			'OM21072800045844','OM21072700002361','OM21072800045333','OM21072700002378','OM21072700002277','OM21072700002338','OM21091100000260','OM21082300007563')
			-- 20211012 剔除部分客户
			and customer_no not in ('122230','115300','117197','121880','109529','121574')
		) as a
		join 
			(
			select
				work_no,first_supervisor_work_no,first_supervisor_name,sales_region_name,sales_province_name,sales_city_name
			from
				(
				select 
					work_no,if(work_no='81095965',work_no,first_supervisor_work_no) as first_supervisor_work_no,if(work_no='81095965',sales_name,first_supervisor_name) as first_supervisor_name,
					--first_supervisor_work_no,first_supervisor_name,
					sales_region_name,sales_province_name,sales_city_name,
					row_number()over(partition by work_no order by create_time desc) as rn
				from 
					csx_dw.dws_crm_w_a_customer
				where 
					--sdt = ${hiveconf:current_end_day}
					sdt = '20210922'
					and customer_no<>''	
					and sales_province_name not like '%平台%'
					and sales_province_name not like '%BBC%'
				) t1
			where
				rn=1
			) c on c.work_no=a.work_no
	group by
		c.first_supervisor_work_no,c.first_supervisor_name,c.sales_region_name,c.sales_province_name,c.sales_city_name
	) a
	join
		(
		select
			employee_code,employee_name,begin_date,end_date,emp_status
		from
			csx_dw.dws_basic_w_a_employee_org_m
		where
			--sdt=${hiveconf:current_end_day}
			sdt = '20210922'
			and emp_status='on'
		) d on d.employee_code=a.first_supervisor_work_no
where
	months_between(from_unixtime(unix_timestamp(${hiveconf:current_end_day},'yyyyMMdd')),from_unixtime(unix_timestamp(begin_date,'yyyyMMdd')))>=2 --司龄
;




--===================================================================================================================================================================
-- 明细

insert overwrite directory '/tmp/zhangyanpeng/20210813_13' row format delimited fields terminated by '\t'

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
		and order_no not in (
		'OM21092500022852','OM21092500018389','OM21091600021970','OM21091800011804','OM21093000003441','OM21081800009334','OM21073000033690','OM21081800016201',
		'OM21072800045844','OM21072700002361','OM21072800045333','OM21072700002378','OM21072700002277','OM21072700002338','OM21091100000260','OM21082300007563')
		-- 20211012 剔除部分客户
		and customer_no not in ('122230','115300','117197','121880','109529','121574')
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
		and order_no not in (
		'OM21092500022852','OM21092500018389','OM21091600021970','OM21091800011804','OM21093000003441','OM21081800009334','OM21073000033690','OM21081800016201',
		'OM21072800045844','OM21072700002361','OM21072800045333','OM21072700002378','OM21072700002277','OM21072700002338','OM21091100000260','OM21082300007563')
		-- 20211012 剔除部分客户
		and customer_no not in ('122230','115300','117197','121880','109529','121574')
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
	
