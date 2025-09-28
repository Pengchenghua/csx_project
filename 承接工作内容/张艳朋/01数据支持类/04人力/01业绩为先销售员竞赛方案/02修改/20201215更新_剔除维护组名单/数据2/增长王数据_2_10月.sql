--==================================================================================================================
-- 销售员 增长王 业绩增长

select
	concat_ws('-', substr('20201001',5,4),substr('20201031',5,4)) as statistic_month,
	a.province_name,a.city_real,a.supervisor_work_no,a.supervisor_name,a.work_no,a.sales_name,
	a.sales_value,
	a.front_profit,
	a.front_profit/a.sales_value as front_profit_rate,
	b.sales_value as sales_value_2,
	b.front_profit as front_profit_2,
	b.front_profit/b.sales_value as front_profit_rate_2,
	a.sales_value-coalesce(b.sales_value,0) as month_growth
from
	(
	select
		province_name,city_real,supervisor_work_no,supervisor_name,work_no,sales_name,
		sum(sales_value) as sales_value,
		sum(profit) as profit,
		sum(front_profit) as front_profit
	from 
		csx_dw.dws_sale_r_d_customer_sale
	where 
		sdt between '20201001' and '20201031'
		and channel in ('1','7','9') --1大客户2商超4大宗5供应链食百6供应链生鲜7企业购9业务代理
		and province_name not like '平台%'	
		and first_category_code in ('21', '23') --21企事业单位 22商贸批发 23餐饮企业 24食品加工企业 25个人及其他
		and attribute_code in (1, 2) --客户属性 1:日配客户 2:福利客户 3:贸易客户 4:战略客户 5:合伙人客户
		and customer_no not in ('114473','114486','114586','114642','114690','114702','114722','114736')
	group by 
		province_name,city_real,supervisor_work_no,supervisor_name,work_no,sales_name
	) a 
	left join
		(
		select
			work_no,
			sum(sales_value) as sales_value,
			sum(profit) as profit,
			sum(front_profit) as front_profit
		from 
			csx_dw.dws_sale_r_d_customer_sale
		where 
			sdt between '20200901' and '20200930'
			and channel in ('1','7','9') --1大客户2商超4大宗5供应链食百6供应链生鲜7企业购9业务代理
			and province_name not like '平台%'	
			and first_category_code in ('21', '23') --21企事业单位 22商贸批发 23餐饮企业 24食品加工企业 25个人及其他
			and attribute_code in (1, 2) --客户属性 1:日配客户 2:福利客户 3:贸易客户 4:战略客户 5:合伙人客户
			and customer_no not in ('114473','114486','114586','114642','114690','114702','114722','114736')
		group by 
			work_no
		) as b on b.work_no=a.work_no
where
	a.sales_value-coalesce(b.sales_value,0)>=50000
	and a.front_profit/a.sales_value >0.05 --整体当月前端毛利>5%
order by 
	a.sales_value-coalesce(b.sales_value,0) desc
	
	
