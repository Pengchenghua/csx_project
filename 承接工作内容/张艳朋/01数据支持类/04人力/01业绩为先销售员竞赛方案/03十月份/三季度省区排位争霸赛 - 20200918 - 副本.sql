--★★★第3季度省区排位争霸赛业绩PK日报
--★★★按照省区、城市的B端+BBC业绩维度的本月及环比月业绩，合伙人依据客户属性区分

--20200918 贸易单规则: 
--1、客户为非贸易客户：订单毛利率≤1.5%，订单金额10万以上的订单，当月下单天数≤2 的.此类业务都定义为贸易业务.
--2、客户是贸易客户，购买商品的目的是用于批发贸易.且订单毛利率≤1.5%.此类业务都定义为贸易业务.

-- 昨日、昨日月1日， 上月同日，上月1日，上月最后一日
--select ${hiveconf:i_sdate_11},${hiveconf:i_sdate_12},${hiveconf:i_sdate_21},${hiveconf:i_sdate_22},${hiveconf:i_sdate_23};

set i_sdate_11 =regexp_replace(date_sub(current_date,1),'-','');
set i_sdate_12 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','');

set i_sdate_21 =concat(substr(regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-',''),1,6),
					if(date_sub(current_date,1)=last_day(date_sub(current_date,1))
					,substr(regexp_replace(last_day(add_months(trunc(date_sub(current_date,1),'MM'),-1)),'-',''),7,2)
					,substr(regexp_replace(date_sub(current_date,1),'-',''),7,2)));	
set i_sdate_22 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');
					
set i_sdate_23 =regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','');


-- 客户销售结果
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
	(select	province_code,province_name,city_group_code,city_group_name,customer_no,coalesce(origin_order_no,order_no) order_no,
	case when (sdt>=${hiveconf:i_sdate_12} and sdt<=${hiveconf:i_sdate_11}) then '本月' 
		when (sdt>=${hiveconf:i_sdate_22} and sdt<=${hiveconf:i_sdate_21}) then '上月环比'
		else '上月' end smonth,	
		sum(sales_value)sales_value,
		--sum(sales_cost)sales_cost,
		sum(profit)profit,
		sum(profit)/sum(sales_value) order_profit_rate,
		sum(front_profit)front_profit
	from csx_dw.dws_sale_r_d_customer_sale
	where sdt>=${hiveconf:i_sdate_22} and sdt<=${hiveconf:i_sdate_11}
	and sales_type in ('sapqyg','sapgc','qyg','sc','bbc') 
	and province_name not like '平台%'
	and channel in('1','7')
	group by province_code,province_name,city_group_code,city_group_name,customer_no,coalesce(origin_order_no,order_no),
	case when (sdt>=${hiveconf:i_sdate_12} and sdt<=${hiveconf:i_sdate_11}) then '本月' 
		when (sdt>=${hiveconf:i_sdate_22} and sdt<=${hiveconf:i_sdate_21}) then '上月环比'
		else '上月' end )a
left join 
	(select customer_no,customer_name,third_supervisor_name,first_supervisor_name,attribute
	from csx_dw.dws_crm_w_a_customer_m_v1
	where sdt = ${hiveconf:i_sdate_11}
	and customer_no<>''
	)b on b.customer_no=a.customer_no
left join 
	(select customer_no,customer_name,third_supervisor_name,first_supervisor_name,attribute
	from csx_dw.dws_crm_w_a_customer_m_v1
	where sdt = ${hiveconf:i_sdate_23}
	and customer_no<>''
	)c on c.customer_no=a.customer_no
left join --客户本月下单次数、月整体毛利率
	(select	province_code,province_name,city_group_code,city_group_name,customer_no,'本月' smonth,	
		count(distinct sdt) count_days,
		sum(profit)/sum(sales_value) month_profit_rate
	from csx_dw.dws_sale_r_d_customer_sale
	where sdt>=${hiveconf:i_sdate_12} and sdt<=${hiveconf:i_sdate_11}
	and sales_type in ('sapqyg','sapgc','qyg','sc','bbc') 
	and province_name not like '平台%'
	and channel in('1','7')
	group by province_code,province_name,city_group_code,city_group_name,customer_no
	--having count(distinct sdt)>2
	)d on d.customer_no=a.customer_no
group by a.province_name,a.city_group_name,a.customer_no,b.customer_name,b.attribute,c.attribute,
		case when a.smonth='本月' and b.attribute='贸易客户' and d.month_profit_rate<=0.015 then '贸易客户贸易业务'
		 when a.smonth='本月' and b.attribute<>'贸易客户' and a.order_profit_rate<=0.015 and a.sales_value >100000 and d.count_days<=2 then '其他客户贸易业务'
		else '常规' end;
	


