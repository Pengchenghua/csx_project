
set sdt_yes=regexp_replace(date_sub(current_date,1), '-', '');
-- 上季度第一天
set sdt_2qua=regexp_replace(add_months(to_date(concat(date_format(date_sub(current_date,1),'y'),'-',floor((cast(date_format(date_sub(current_date,1),'M') as int)+2)/3)*3-2,'-',date_format(trunc(date_sub(current_date,1),'MM'),'dd'))),-3), '-', '');

-- 本季度第一天
set sdt_qua=regexp_replace(to_date(concat(date_format(date_sub(current_date,1),'y'),'-',floor((cast(date_format(date_sub(current_date,1),'M') as int)+2)/3)*3-2,'-',date_format(trunc(date_sub(current_date,1),'MM'),'dd'))), '-', '');
set sdt_qua_date=to_date(concat(date_format(date_sub(current_date,1),'y'),'-',floor((cast(date_format(date_sub(current_date,1),'M') as int)+2)/3)*3-2,'-',date_format(trunc(date_sub(current_date,1),'MM'),'dd'))); 
set sdt_qua_last_date=last_day(date_add(to_date(concat(date_format(date_sub(current_date,1),'y'),'-',floor((cast(date_format(date_sub(current_date,1),'M') as int)+2)/3)*3-2,'-',date_format(trunc(date_sub(current_date,1),'MM'),'dd'))),75));
set sdt_yes_date=date_sub(current_date,1);

insert overwrite directory '/tmp/zhangyanpeng/20221012_01_01' row format delimited fields terminated by '\t'

select
	a.customer_no,
	b.customer_name,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	b.sales_region_name,
	b.province_name,
	b.city_group_name,
	a.quarter_new,
	e.business_type_name,
	a.sales_value,
	a.excluding_tax_sales,
	a.profit,
	a.excluding_tax_profit,
	e.yw_first_order_date,
	(case when e.yw_first_order_date<'20210101' then '20年及之前' else e.yw_first_order_quarter end) as yw_first_order_quarter,  
	d.first_order_date,
	(case when d.first_order_date<'20210101' then '20年及之前' else d.first_order_quarter end) as first_order_quarter  
from
	(
	select
		customer_no,
		business_type_code,
		business_type_name,
		count(distinct sdt) as cust_sales_cnt,
		sum(sales_value) sales_value,
		sum(excluding_tax_sales) as excluding_tax_sales,
		sum(profit) profit,
		sum(excluding_tax_profit) as excluding_tax_profit,
		sum(profit) / abs(sum(sales_value)) profit_rate,
		concat(substr(add_months(sales_time,-3),1,4),(floor(substr(add_months(sales_time,-3),6,2)/3.1))+1) as last_quarter,
		concat(substr(to_date(sales_time),1,4),(floor(substr(to_date(sales_time),6,2)/3.1))+1) quarter,

		(datediff(${hiveconf:sdt_qua_last_date},${hiveconf:sdt_qua_date})+1)/(datediff(${hiveconf:sdt_yes_date},${hiveconf:sdt_qua_date})+1) as qua_date_bl,
		(case when regexp_replace(${hiveconf:sdt_qua_last_date}, '-', '')=${hiveconf:sdt_yes} then 1 else 0 end) as if_last_day,
		concat(substr(to_date(sales_time),1,4),'Q',(floor(substr(to_date(sales_time),6,2)/3.1))+1) quarter_new 
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20210101' and sdt<='20220930' 
		and channel_code in ('1', '7', '9') and business_type_code in ('1','2','3','4','5','6')
	group by 
		customer_no,business_type_code,business_type_name, 
		concat(substr(to_date(sales_time),1,4),(floor(substr(to_date(sales_time),6,2)/3.1))+1),
		concat(substr(add_months(sales_time,-3),1,4),(floor(substr(add_months(sales_time,-3),6,2)/3.1))+1),
		(datediff(${hiveconf:sdt_qua_last_date},${hiveconf:sdt_qua_date})+1)/(datediff(${hiveconf:sdt_yes_date},${hiveconf:sdt_qua_date})+1),
		(case when regexp_replace(${hiveconf:sdt_qua_last_date}, '-', '')=${hiveconf:sdt_yes} then 1 else 0 end),
		concat(substr(to_date(sales_time),1,4),'Q',(floor(substr(to_date(sales_time),6,2)/3.1))+1)  
	) a 
	left join
		(
		select
			* 
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
		) b on a.customer_no = b.customer_no
	left join
		(
		select 
			customer_no, first_order_date, last_order_date,
			concat(substr(first_order_date,1,4),'Q',(floor(substr(first_order_date,5,2)/3.1))+1) first_order_quarter  
		from 
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt='current'
		) d on a.customer_no = d.customer_no
	left join 
		(
		select 
			customer_no,
			business_type_code,
			business_type_name,
			first_order_date as yw_first_order_date,
			concat(substr(first_order_date,1,4),'Q',(floor(substr(first_order_date,5,2)/3.1))+1) yw_first_order_quarter  
		from 
			csx_dw.dws_crm_w_a_customer_business_active  
		where 
			sdt='current' 
			and business_type_code in ('1','2','3','4','5','6')
		) e on a.customer_no = e.customer_no and a.business_type_code = e.business_type_code