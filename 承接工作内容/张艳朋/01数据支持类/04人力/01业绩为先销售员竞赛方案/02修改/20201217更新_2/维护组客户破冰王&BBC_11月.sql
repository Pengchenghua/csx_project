--===============================================================================================
-- 销售员 破冰王 新开客户

-- 昨日
set i_sdate ='2020-11-30';
set i_sdate_11 =regexp_replace('2020-11-30','-','');

--昨日月1日
set i_sdate_12 =regexp_replace(trunc('2020-11-30','MM'),'-','');

select
	concat_ws('-', substr(${hiveconf:i_sdate_12},5,4),substr(${hiveconf:i_sdate_11},5,4)) as statistic_month,
	province_name,city_real,supervisor_work_no,supervisor_name,work_no,sales_name,
	sum(sales_value) as sales_value,
	sum(front_profit) as front_profit,
	sum(front_profit) / sum(sales_value) as front_profit_rate,
	count(distinct customer_no) as cust_count,
	sum(avg_sales_value) as avg_sales_value
from	
	(	
	select	
		first_date,customer_no,province_name,city_real,supervisor_work_no,supervisor_name,work_no,sales_name,
		sum(sales_value) as sales_value,sum(front_profit) as front_profit,
		sum(sales_value) / (${hiveconf:i_sdate_11}-first_date+1) as avg_sales_value
	from	
		(
		select
			b.first_date,a.customer_no,a.province_name,a.city_real,a.supervisor_work_no,a.supervisor_name,a.work_no,
			a.sales_name,a.order_no,a.sales_value,a.profit,a.front_profit,a.profit_rate
		from
			(
			select
				customer_no,province_name,city_real,supervisor_work_no,supervisor_name,work_no,sales_name,order_no,
				sum(sales_value) as sales_value, --含税销售额
				sum(profit) as profit, --含税毛利额
				sum(front_profit) as front_profit, --前端含税毛利
				sum(profit)/sum(sales_value) as profit_rate
			from 
				csx_dw.dws_sale_r_d_customer_sale
			where 
				sdt between ${hiveconf:i_sdate_12} and ${hiveconf:i_sdate_11}
				and channel in ('1', '7') --1大客户2商超4大宗5供应链食百6供应链生鲜7企业购9业务代理
				and province_name not like '平台%'	
				and first_category_code in ('21', '23') --21企事业单位 22商贸批发 23餐饮企业 24食品加工企业 25个人及其他
				and attribute_code in (1, 2) --客户属性 1:日配客户 2:福利客户 3:贸易客户 4:战略客户 5:合伙人客户
				and customer_no in ('114629','114675','114715','114718','114720','114812','114833','114857','114881','114904','114911','114922','114933','115053','115145','115202')
			group by
				customer_no,province_name,city_real,supervisor_work_no,supervisor_name,work_no,sales_name,order_no
			) a 
			left join
				(
				select 
					customer_no, 
					first_order_date as first_date
				from 
					csx_dw.ads_crm_w_a_customer_active_info
				where 
					sdt = ${hiveconf:i_sdate_11}
					and first_order_date between ${hiveconf:i_sdate_12} and ${hiveconf:i_sdate_11}
				group by
					customer_no,first_order_date			
				) b on b.customer_no=a.customer_no
			--left join 
			--	(
			--	select 
			--		customer_no
			--	from 
			--		csx_dw.dws_crm_w_a_customer_m_v1
			--	where 
			--		sdt between ${hiveconf:i_sdate_12} and ${hiveconf:i_sdate_11}
			--		and customer_no<>''	
			--		and position = 'SALES'
			--	group by
			--		customer_no
			--	) c on c.customer_no=a.customer_no
			left join --客户本月下单次数、月整体毛利率
				(
				select	
					customer_no,
					count(distinct sdt) count_days
				from 
					csx_dw.dws_sale_r_d_customer_sale
				where 
					sdt between ${hiveconf:i_sdate_12} and ${hiveconf:i_sdate_11}
					and sales_type in ('sapqyg','sapgc','qyg','sc','bbc') 
					and province_name not like '平台%'
					and channel in('1','7')
				group by 
					customer_no
				) d on d.customer_no=a.customer_no				
		where
			b.customer_no is not null
			--and c.customer_no is not null
			and (profit_rate>0.015 or sales_value<=100000 or d.count_days>2) --剔贸易单 1、客户为非贸易客户：订单毛利率≤1.5%，订单金额10万以上的订单，当月下单天数≤2；2、客户是贸易客户，购买商品的目的是用于批发贸易.且订单毛利率≤1.5%
		) tmp1
	group by
		first_date,customer_no,province_name,city_real,supervisor_work_no,supervisor_name,work_no,sales_name
	having
		sum(sales_value) / (${hiveconf:i_sdate_11}-first_date+1)>=1000 --筛选当月平均日出库金额>=1000元客户
	)tmp2	
group by
	concat_ws('-', substr(${hiveconf:i_sdate_12},5,4),substr(${hiveconf:i_sdate_11},5,4)),
	province_name,city_real,supervisor_work_no,supervisor_name,work_no,sales_name
having
	sum(front_profit) / sum(sales_value)>0.03 --筛选整体新履约客户的前端毛利率>3%
order by
	cust_count desc, 
	sales_value desc; 



--===============================================================================================
-- 销售员 BBC突破 BBC业绩突破
-- 昨日
set i_sdate ='2020-11-30';
set i_sdate_11 =regexp_replace('2020-11-30','-','');

--昨日月1日
set i_sdate_12 =regexp_replace(trunc('2020-11-30','MM'),'-','');

select
	concat_ws('-', substr(${hiveconf:i_sdate_12},5,4),substr(${hiveconf:i_sdate_11},5,4)) as statistic_month,
	province_name,city_real,supervisor_work_no,supervisor_name,work_no,sales_name,
	sum(sales_value) as sales_value,
	sum(front_profit) as front_profit,
	sum(front_profit)/sum(sales_value) as front_profit_rate,
	count(distinct customer_no) as cust_count,
	sum(avg_sales_value) as avg_sales_value
from	
	(
	select
		b.first_date,a.customer_no,a.province_name,a.city_real,a.supervisor_work_no,a.supervisor_name,a.work_no,a.sales_name,
		a.sales_value,a.front_profit,a.sales_value/(${hiveconf:i_sdate_11}-b.first_date+1) as avg_sales_value
	from
		(
		select
			customer_no,province_name,city_real,supervisor_work_no,supervisor_name,work_no,sales_name,
			sum(sales_value) as sales_value, --含税销售额
			sum(profit) as profit, --含税毛利额
			sum(front_profit) as front_profit, --前端含税毛利
			sum(profit)/sum(sales_value) as profit_rate
		from 
			csx_dw.dws_sale_r_d_customer_sale
		where 
			sdt between ${hiveconf:i_sdate_12} and ${hiveconf:i_sdate_11}
			and channel in ('7') --1大客户2商超4大宗5供应链食百6供应链生鲜7企业购9业务代理
			and province_name not like '平台%'
			and first_category_code in ('21', '23') --21企事业单位 22商贸批发 23餐饮企业 24食品加工企业 25个人及其他
			and attribute_code in (1, 2) --客户属性 1:日配客户 2:福利客户 3:贸易客户 4:战略客户 5:合伙人客户
			and customer_no in ('114629','114675','114715','114718','114720','114812','114833','114857','114881','114904','114911','114922','114933','115053','115145','115202')
		group by
			customer_no,province_name,city_real,supervisor_work_no,supervisor_name,work_no,sales_name
		) a 
		left join
			(
			select 
				customer_no, 
				bbc_first_order_date as first_date
			from 
				csx_dw.ads_crm_w_a_customer_active_info
			where 
				sdt = ${hiveconf:i_sdate_11}
				and bbc_first_order_date between ${hiveconf:i_sdate_12} and ${hiveconf:i_sdate_11}
			group by
				customer_no,bbc_first_order_date			
			) b on b.customer_no=a.customer_no
		--left join 
		--	(
		--	select 
		--		customer_no
		--	from 
		--		csx_dw.dws_crm_w_a_customer_m_v1
		--	where 
		--		sdt between ${hiveconf:i_sdate_12} and ${hiveconf:i_sdate_11}
		--		and customer_no<>''	
		--		and position = 'SALES'
		--	group by
		--		customer_no
		--	) c on c.customer_no=a.customer_no			
	where
		b.customer_no is not null
		--and c.customer_no is not null
		and a.sales_value/(${hiveconf:i_sdate_11}-b.first_date+1)>=700 --筛选当月平均日履约金额>=700元客户
	) tmp1
group by
	concat_ws('-', substr(${hiveconf:i_sdate_12},5,4),substr(${hiveconf:i_sdate_11},5,4)),
	province_name,city_real,supervisor_work_no,supervisor_name,work_no,sales_name
having
	sum(front_profit)/sum(sales_value)>0.05
order by
	cust_count desc, 
	sales_value desc; 
