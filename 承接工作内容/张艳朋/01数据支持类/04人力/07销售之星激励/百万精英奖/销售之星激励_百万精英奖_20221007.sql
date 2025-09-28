--==================================================================================================================================================================================
set current_start_day ='20220801';

set current_end_day ='20220930';

--set last_month_start_day ='20210801';

--set last_month_end_day ='20210831';

--01_销售员激励案_Q3福利激励案_百万精英奖

insert overwrite directory '/tmp/zhangyanpeng/20220815_01' row format delimited fields terminated by '\t'

select
	concat(${hiveconf:current_start_day},'-',${hiveconf:current_end_day}) as sdt_s,
	b.sales_region_name,
	b.province_name,
	b.city_group_name,
	b.work_no,
	b.sales_name,
	sum(a.sales_value) as sales_value,
	sum(a.profit) as profit,
	sum(a.profit)/abs(sum(a.sales_value)) as profit_rate,
	row_number() over(order by sum(a.sales_value) desc) as rn,
	sum(fl_sales_value) as fl_sales_value,
	sum(bbc_sales_value) as bbc_sales_value,
	sum(fl_profit) as fl_profit,
	sum(bbc_profit) as bbc_profit
from 
	(
	select 
		customer_no,
		sum(sales_value)as sales_value,
		sum(profit) as profit,
		sum(case when business_type_code in ('2') then sales_value else 0 end) as fl_sales_value,
		sum(case when business_type_code in ('6') then sales_value else 0 end) as bbc_sales_value,
		sum(case when business_type_code in ('2') then profit else 0 end) as fl_profit,
		sum(case when business_type_code in ('6') then profit else 0 end) as bbc_profit		
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
		and business_type_code in ('2','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and goods_code not in ('8718','8708','8649')
	group by 
		customer_no
	having
		sum(sales_value)>=10000
		and sum(profit)/abs(sum(sales_value))>0.03
	) a  
	left join   
		(
		select 
			customer_no,customer_name,sales_region_name,province_name,city_group_name,work_no,sales_name
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt=${hiveconf:current_end_day}
		) b on b.customer_no=a.customer_no
	--join -- 员工信息
	--	(
	--	select 
	--		employee_code,employee_name,begin_date,leader_code,leader_name
	--	from 
	--		csx_dw.dws_basic_w_a_employee_org_m
	--	where 
	--		sdt = ${hiveconf:current_end_day}
	--		and emp_status='on'
	--	) c on c.employee_code=a.work_no
group by 
	b.sales_region_name,
	b.province_name,
	b.city_group_name,
	b.work_no,
	b.sales_name
having
	sum(a.sales_value)>=1000000
;

--===================================================================================================================================================================
-- 明细

insert overwrite directory '/tmp/zhangyanpeng/20220815_02' row format delimited fields terminated by '\t'

select 
	sdt_s, sales_region_name,province_name,city_group_name,customer_no,customer_name,work_no,sales_name,sales_value,
	profit,profit_rate,rn,fl_sales_value,bbc_sales_value,fl_profit,bbc_profit
from
	(
	select
		concat(${hiveconf:current_start_day},'-',${hiveconf:current_end_day}) as sdt_s,
		b.sales_region_name,
		b.province_name,
		b.city_group_name,
		a.customer_no,
		b.customer_name,
		b.work_no,
		b.sales_name,
		a.sales_value,
		a.profit,
		a.profit/abs(a.sales_value) as profit_rate,
		row_number() over(order by a.sales_value desc) as rn,
		fl_sales_value,
		bbc_sales_value,
		fl_profit,
		bbc_profit,
		sum(a.sales_value)over(partition by b.work_no) as sales_total_sales_value
	from 
		(
		select 
			customer_no,
			sum(sales_value)as sales_value,
			sum(profit) as profit,
			sum(case when business_type_code in ('2') then sales_value else 0 end) as fl_sales_value,
			sum(case when business_type_code in ('6') then sales_value else 0 end) as bbc_sales_value,
			sum(case when business_type_code in ('2') then profit else 0 end) as fl_profit,
			sum(case when business_type_code in ('6') then profit else 0 end) as bbc_profit		
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
			and business_type_code in ('2','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and goods_code not in ('8718','8708','8649')
		group by 
			customer_no
		having
			sum(sales_value)>=10000
			and sum(profit)/abs(sum(sales_value))>0.03
		) a  
		left join   
			(
			select 
				customer_no,customer_name,sales_region_name,province_name,city_group_name,work_no,sales_name
			from 
				csx_dw.dws_crm_w_a_customer 
			where 
				sdt=${hiveconf:current_end_day}
			) b on b.customer_no=a.customer_no
		--join -- 员工信息
		--	(
		--	select 
		--		employee_code,employee_name,begin_date,leader_code,leader_name
		--	from 
		--		csx_dw.dws_basic_w_a_employee_org_m
		--	where 
		--		sdt = ${hiveconf:current_end_day}
		--		and emp_status='on'
		--	) c on c.employee_code=a.work_no
	) a 
where
	sales_total_sales_value>=1000000
order by 
	rn
;
