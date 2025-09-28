--==================================================================================================================================================================================
--销售员-百万精英奖、双节福利王

set current_day =regexp_replace(date_sub(current_date,1),'-','');

select
	concat('20210701-',${hiveconf:current_day}) as sdt_s,
	b.sales_region_name,
	b.sales_province_name,
	b.sales_city_name,
	coalesce(b.first_supervisor_work_no,'') as first_supervisor_work_no,
	coalesce(b.first_supervisor_name,'') as first_supervisor_name,
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
		sdt between '20210701' and ${hiveconf:current_day}
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
		and business_type_code in ('2','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and sales_name not rlike 'A|B|C|M' --虚拟销售
	group by 
		customer_no,work_no,sales_name
	having
		sum(sales_value)>=10000
		and sum(front_profit)/abs(sum(sales_value))>0
	) a  
	left join   
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,sales_region_name,sales_province_name,sales_city_name
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt='current'
		group by 
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,sales_region_name,sales_province_name,sales_city_name
		) b on b.customer_no=a.customer_no
	left join -- 员工信息
		(
		select 
			employee_code,employee_name,begin_date
		from 
			csx_dw.dws_basic_w_a_employee_org_m
		where 
			sdt = ${hiveconf:current_day}
		) c on c.employee_code=a.work_no
group by 
	b.sales_region_name,
	b.sales_province_name,
	b.sales_city_name,
	coalesce(b.first_supervisor_work_no,''),
	coalesce(b.first_supervisor_name,''),
	a.work_no,
	a.sales_name,
	coalesce(c.begin_date,'')

	


--==================================================================================================================================================================================
--销售主管-Q3福利销售额

set current_day =regexp_replace(date_sub(current_date,1),'-','');

select
	concat('20210701-',${hiveconf:current_day}) as sdt_s,
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
		sdt between '20210701' and ${hiveconf:current_day}
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
		and business_type_code in ('2','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and supervisor_work_no !=''
	group by 
		customer_no,supervisor_work_no,supervisor_name
	having
		sum(sales_value)>=10000
		and sum(front_profit)/abs(sum(sales_value))>0
	) a  
	left join   
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,sales_region_name,sales_province_name,sales_city_name
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt='current'
		group by 
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,sales_region_name,sales_province_name,sales_city_name
		) b on b.customer_no=a.customer_no
	left join -- 员工信息
		(
		select 
			employee_code,employee_name,begin_date
		from 
			csx_dw.dws_basic_w_a_employee_org_m
		where 
			sdt = ${hiveconf:current_day}
		) c on c.employee_code=a.supervisor_work_no
group by 
	b.sales_region_name,
	b.sales_province_name,
	b.sales_city_name,
	a.supervisor_work_no,
	a.supervisor_name,
	coalesce(c.begin_date,'')



--==================================================================================================================================================================================
--销售主管-Q3福利毛利额

set current_day =regexp_replace(date_sub(current_date,1),'-','');

select
	concat('20210701-',${hiveconf:current_day}) as sdt_s,
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
	row_number() over(order by sum(a.profit) desc) as rn
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
		sdt between '20210701' and ${hiveconf:current_day}
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
		and business_type_code in ('2','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and supervisor_work_no !=''
	group by 
		customer_no,supervisor_work_no,supervisor_name
	having
		sum(sales_value)>=10000
		and sum(front_profit)/abs(sum(sales_value))>0
	) a  
	left join   
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,sales_region_name,sales_province_name,sales_city_name
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt='current'
		group by 
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,sales_region_name,sales_province_name,sales_city_name
		) b on b.customer_no=a.customer_no
	left join -- 员工信息
		(
		select 
			employee_code,employee_name,begin_date
		from 
			csx_dw.dws_basic_w_a_employee_org_m
		where 
			sdt = ${hiveconf:current_day}
		) c on c.employee_code=a.supervisor_work_no
group by 
	b.sales_region_name,
	b.sales_province_name,
	b.sales_city_name,
	a.supervisor_work_no,
	a.supervisor_name,
	coalesce(c.begin_date,'')

--==========================================================================================
-- 明细

insert overwrite directory '/tmp/zhangyanpeng/20210802_linshi_1' row format delimited fields terminated by '\t' 

select 
	sdt,region_name,province_name,city_name,work_no,sales_name,supervisor_work_no,supervisor_name,customer_no,customer_name,business_type_name,order_no,goods_code,goods_name,
	sum(sales_value)as sales_value,
	sum(profit) as profit,
	sum(front_profit) as front_profit
from 
	csx_dw.dws_sale_r_d_detail 
where 
	sdt between '20210701' and '20210801'
	and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
	and business_type_code in ('2','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
	and sales_name not rlike 'A|B|C|M' --虚拟销售
group by 
	sdt,region_name,province_name,city_name,work_no,sales_name,supervisor_work_no,supervisor_name,customer_no,customer_name,business_type_name,order_no,goods_code,goods_name
		
		
		
--==========================================================================================

drop table csx_tmp.tmp_cust_sale_Q3;
create table csx_tmp.tmp_cust_sale_Q3
as 
select 
	a.province_name,a.city_group_name,a.customer_no,
	b.customer_name,b.attribute attribute_by,c.attribute attribute_sy,
	case when a.smonth='本月' and b.attribute='贸易客户' and d.month_profit_rate<=0.015 then '贸易客户贸易业务'
		 when a.smonth='本月' and b.attribute<>'贸易客户' and a.order_profit_rate<=0.015 and a.sales_value >100000 and d.count_days<=2 then '其他客户贸易业务'
		else '常规' end customer_group,
	sum(case when a.smonth='本月' then a.sales_value end) M_sales_value,
	sum(case when a.smonth='本月' then a.profit end) M_profit,
	sum(case when a.smonth='上月环比' then a.sales_value end) H_sales_value,
	sum(case when a.smonth='上月环比' then a.profit end) H_profit,
	sum(case when a.smonth<>'本月' then a.sales_value end) H_month_sales_value,
	sum(case when a.smonth<>'本月' then a.profit end) H_month_profit,
	${hiveconf:i_sdate_11} sdt
from
	(
	select	
		province_code,province_name,city_group_code,city_group_name,customer_no,coalesce(origin_order_no,order_no) order_no,
		case when (sdt>=${hiveconf:i_sdate_12} and sdt<=${hiveconf:i_sdate_11}) then '本月' 
			when (sdt>=${hiveconf:i_sdate_22} and sdt<=${hiveconf:i_sdate_21}) then '上月环比'
		else '上月' end smonth,	
		sum(sales_value)sales_value,
		--sum(sales_cost)sales_cost,
		sum(profit)profit,
		sum(profit)/sum(sales_value) order_profit_rate,
		sum(front_profit)front_profit
	from 
		csx_dw.dws_sale_r_d_customer_sale
	where 
		sdt>=${hiveconf:i_sdate_22} and sdt<=${hiveconf:i_sdate_11}
		and sales_type in ('sapqyg','sapgc','qyg','sc','bbc') 
		and province_name not like '平台%'
		and channel in('1','7')
	group by 
		province_code,province_name,city_group_code,city_group_name,customer_no,coalesce(origin_order_no,order_no),
		case when (sdt>=${hiveconf:i_sdate_12} and sdt<=${hiveconf:i_sdate_11}) then '本月' 
			when (sdt>=${hiveconf:i_sdate_22} and sdt<=${hiveconf:i_sdate_21}) then '上月环比'
		else '上月' end 
	)a
	left join 
		(
		select 
			customer_no,customer_name,third_supervisor_name,first_supervisor_name,attribute
		from 
			csx_dw.dws_crm_w_a_customer_m_v1
		where 
			sdt = ${hiveconf:i_sdate_11}
			and customer_no<>''
		)b on b.customer_no=a.customer_no
	left join 
		(
		select 
			customer_no,customer_name,third_supervisor_name,first_supervisor_name,attribute
		from 
			csx_dw.dws_crm_w_a_customer_m_v1
		where 
			sdt = ${hiveconf:i_sdate_23}
			and customer_no<>''
		)c on c.customer_no=a.customer_no
	left join --客户本月下单次数、月整体毛利率
		(
		select	
			province_code,province_name,city_group_code,city_group_name,customer_no,'本月' smonth,	
			count(distinct sdt) count_days,
			sum(profit)/sum(sales_value) month_profit_rate
		from 
			csx_dw.dws_sale_r_d_customer_sale
		where 
			sdt>=${hiveconf:i_sdate_12} and sdt<=${hiveconf:i_sdate_11}
			and sales_type in ('sapqyg','sapgc','qyg','sc','bbc') 
			and province_name not like '平台%'
			and channel in('1','7')
		group by 
			province_code,province_name,city_group_code,city_group_name,customer_no
		)d on d.customer_no=a.customer_no
group by 
	a.province_name,a.city_group_name,a.customer_no,b.customer_name,b.attribute,c.attribute,
	case when a.smonth='本月' and b.attribute='贸易客户' and d.month_profit_rate<=0.015 then '贸易客户贸易业务'
		 when a.smonth='本月' and b.attribute<>'贸易客户' and a.order_profit_rate<=0.015 and a.sales_value >100000 and d.count_days<=2 then '其他客户贸易业务'
	else '常规' end;
	


