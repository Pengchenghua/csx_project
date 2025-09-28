-- 由于标识错误 需剔除部分客户：'118849','118365','114646','118215','120645','115237','113387'
-- 销售主管激励案_Q3福利激励案_Q3福利毛利额 定价毛利额改为前端毛利额 20210906 
-- 贺仕文由销售员更改为主管

--==================================================================================================================================================================================
set current_start_day ='20220801';

set current_end_day ='20220904';

--set last_month_start_day ='20210801';

--set last_month_end_day ='20210831';

--01_销售员激励案_Q3福利激励案_百万精英奖

insert overwrite directory '/tmp/zhangyanpeng/20220815_03' row format delimited fields terminated by '\t'

select
	concat(${hiveconf:current_start_day},'-',${hiveconf:current_end_day}) as sdt_s,
	b.sales_region_name,
	b.province_name,
	b.city_group_name,
	a.work_no,
	a.sales_name,
	c.begin_date,
	sum(a.sales_value) as sales_value,
	sum(a.profit) as profit,
	sum(a.profit)/abs(sum(a.sales_value)) as profit_rate,
	row_number() over(order by sum(a.sales_value) desc) as rn
from 
	(
	select 
		customer_no,work_no,sales_name,
		sum(sales_value)as sales_value,
		sum(profit) as profit
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
		and business_type_code in ('2','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and goods_code not in ('8718','8708','8649')
	group by 
		customer_no,work_no,sales_name
	having
		sum(sales_value)>=10000
		and sum(profit)/abs(sum(sales_value))>0.03
	) a  
	left join   
		(
		select 
			customer_no,customer_name,sales_region_name,province_name,city_group_name
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt=${hiveconf:current_end_day}
		) b on b.customer_no=a.customer_no
	join -- 员工信息
		(
		select 
			employee_code,employee_name,begin_date,leader_code,leader_name
		from 
			csx_dw.dws_basic_w_a_employee_org_m
		where 
			sdt = ${hiveconf:current_end_day}
			and begin_date>='20220501'
		) c on c.employee_code=a.work_no
	left join
		(
		select 
			customer_no,min(sdt) as first_order_date
		from 
			csx_dw.dws_sale_r_d_detail 
		where
			sdt>='20190101'
			and sdt <= ${hiveconf:current_end_day}
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
			and business_type_code in ('2','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and goods_code not in ('8718','8708','8649')
		group by 
			customer_no
		) d on d.customer_no=a.customer_no
where
	d.first_order_date>=c.begin_date
group by 
	b.sales_region_name,
	b.province_name,
	b.city_group_name,
	a.work_no,
	a.sales_name,
	c.begin_date
;

--===================================================================================================================================================================
-- 明细

insert overwrite directory '/tmp/zhangyanpeng/20220815_04' row format delimited fields terminated by '\t'

select
	concat(${hiveconf:current_start_day},'-',${hiveconf:current_end_day}) as sdt_s,
	b.sales_region_name,
	b.province_name,
	b.city_group_name,
	a.customer_no,
	b.customer_name,
	a.work_no,
	a.sales_name,
	c.begin_date,
	d.first_order_date,
	a.sales_value,
	a.profit,
	a.profit/abs(a.sales_value) as profit_rate,
	row_number() over(order by a.sales_value desc) as rn
from 
	(
	select 
		customer_no,work_no,sales_name,
		sum(sales_value)as sales_value,
		sum(profit) as profit
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
		and business_type_code in ('2','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and goods_code not in ('8718','8708','8649')
	group by 
		customer_no,work_no,sales_name
	having
		sum(sales_value)>=10000
		and sum(profit)/abs(sum(sales_value))>0.03
	) a  
	left join   
		(
		select 
			customer_no,customer_name,sales_region_name,province_name,city_group_name
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt=${hiveconf:current_end_day}
		) b on b.customer_no=a.customer_no
	join -- 员工信息
		(
		select 
			employee_code,employee_name,begin_date,leader_code,leader_name
		from 
			csx_dw.dws_basic_w_a_employee_org_m
		where 
			sdt = ${hiveconf:current_end_day}
			and begin_date>='20220501'
		) c on c.employee_code=a.work_no
	left join
		(
		select 
			customer_no,min(sdt) as first_order_date
		from 
			csx_dw.dws_sale_r_d_detail 
		where
			sdt>='20190101'
			and sdt <= ${hiveconf:current_end_day}
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
			and business_type_code in ('2','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and goods_code not in ('8718','8708','8649')
		group by 
			customer_no
		) d on d.customer_no=a.customer_no
where
	d.first_order_date>=c.begin_date
	and a.work_no in('81173939','81170039','81170726','81177029','81175663','81171836')
;


