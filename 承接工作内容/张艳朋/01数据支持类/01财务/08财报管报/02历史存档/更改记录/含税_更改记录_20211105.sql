SELECT
	COALESCE(t1.region_name, t2.region_name) AS region_name,
	COALESCE(t1.province_name, t2.province_name) AS province_name,
	COALESCE(t1.city_group_name, t2.city_group_name) AS city_group_name,
	t2.sales_value,
	t2.sales_value_B,
	t2.sales_value_M,
	t2.sales_value_BBC,
	t2.sales_cost,
	t2.sales_cost_B,
	t2.sales_cost_M,
	t2.sales_cost_BBC,
	t2.profit,
	t2.profit_B,
	t2.profit_M,
	t2.profit_BBC,
	t2.count_cust,
	t2.count_cust_xqy,
	t2.count_cust_xcj,
	t2.count_cust_rp,
	t2.count_cust_fl,
	t2.count_cust_my,
	t2.count_cust_sm,
	t2.sale_cust_rp,
	t2.sale_cust_fl,
	t2.sale_cust_my,
	t2.sale_cust_sm
from
	(
	select 
		distinct region_code,region_name,province_code,province_name,city_group_code,city_group_name 
	from 
		data_center_report.dws_sale_w_a_area_belong
	where 
		city_group_code <> '' 
		and city_group_code <> '100000' 
		and city_group_code <> '11'
	) t1 
	left join
		(
		select 
			region_name,case when substr(sdt,1,6)<='202107' and province_name like '江苏%' then '江苏省' else province_name end province_name,
			if(province_name='福建省',substr(city_group_name,1,3),city_group_name) city_group_name,
			sum(sales_value)/10000 sales_value,
			sum(if(channel_code in('1','9'),sales_value,0))/10000 sales_value_B,
			sum(if(channel_name like '商超%',sales_value,0))/10000 sales_value_M,
			sum(if(channel_name like '企业购%',sales_value,0))/10000 sales_value_BBC,
			sum(sales_cost)/10000 sales_cost,
			sum(if(channel_code in('1','9'),sales_cost,0))/10000 sales_cost_B,
			sum(if(channel_name like '商超%',sales_cost,0))/10000 sales_cost_M,
			sum(if(channel_name like '企业购%',sales_cost,0))/10000 sales_cost_BBC,
			sum(profit)/10000 profit,
			sum(if(channel_code in('1','9'),profit,0))/10000 profit_B,
			sum(if(channel_name like '商超%',profit,0))/10000 profit_M,
			sum(if(channel_name like '企业购%',profit,0))/10000 profit_BBC,
			count(distinct  case when channel_code in('1','7','9') then customer_no end) count_cust,
			count(distinct  case when channel_code in('1','7','9') and is_new_sign='是' then customer_no end) count_cust_xqy,
			count(distinct case when channel_code in('1','7','9') and is_new_sale='是' then customer_no end) count_cust_xcj,
			count(distinct  case when channel_code in('1','7','9') and (attribute_code='1' or attribute_code is null) then customer_no end) count_cust_rp,
			count(distinct  case when channel_code in('1','7','9') and attribute_code='2' then customer_no end) count_cust_fl,
			count(distinct case when channel_code in('1','7','9') and attribute_code='3' then customer_no end) count_cust_my,
			count(distinct case when channel_code in('1','7','9') and attribute_code not in('1','2','3') then customer_no end) count_cust_sm,
			sum(case when channel_code in('1','7','9') and (attribute_code='1' or attribute_code is null) then sales_value end)/10000 sale_cust_rp,
			sum(case when channel_code in('1','7','9') and attribute_code='2' then sales_value end)/10000 sale_cust_fl,
			sum(case when channel_code in('1','7','9') and attribute_code='3' then sales_value end)/10000 sale_cust_my,
			sum(case when channel_code in('1','7','9') and attribute_code not in('1','2','3') then sales_value end)/10000 sale_cust_sm
		from 
			csx_data_market.report_sale_r_d_achievement
		where 
			substr(sdt,1,6)='${EDATE}'
			and (region_name not like '%大宗%' and region_name not like '%供应链%')
		group by 
			region_name,case when substr(sdt,1,6)<='202107' and province_name like '江苏%' then '江苏省' else province_name end,
			if(province_name='福建省',substr(city_group_name,1,3),city_group_name)
		) t2 ON t1.region_name = t2.region_name AND t1.province_name = t2.province_name AND t1.city_group_name = t2.city_group_name;