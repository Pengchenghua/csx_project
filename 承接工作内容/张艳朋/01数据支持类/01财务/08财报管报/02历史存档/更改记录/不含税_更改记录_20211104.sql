select 
	region_name,case when substr(sdt,1,6)<='202107' and province_name like '江苏%' then '江苏省' else province_name end province_name,
	if(province_name='福建省',substr(city_group_name,1,3),city_group_name) city_group_name,
	sum(no_tax_sales)/10000 sales_value,
	sum(if(channel_code in('1','9'),no_tax_sales,0))/10000 sales_value_B,
	sum(if(channel_name like '商超%',no_tax_sales,0))/10000 sales_value_M,
	sum(if(channel_name like '企业购%',no_tax_sales,0))/10000 sales_value_BBC,
	(sum(no_tax_sales)-sum(no_tax_profit))/10000 sales_cost,
	sum(if(channel_code in('1','9'),no_tax_sales-no_tax_profit,0))/10000 sales_cost_B,
	sum(if(channel_name like '商超%',no_tax_sales-no_tax_profit,0))/10000 sales_cost_M,
	sum(if(channel_name like '企业购%',no_tax_sales-no_tax_profit,0))/10000 sales_cost_BBC,
	sum(no_tax_profit)/10000 profit,
	sum(if(channel_code in('1','9'),no_tax_profit,0))/10000 profit_B,
	sum(if(channel_name like '商超%',no_tax_profit,0))/10000 profit_M,
	sum(if(channel_name like '企业购%',no_tax_profit,0))/10000 profit_BBC,
	count(distinct  case when channel_code in('1','7','9') then customer_no end) count_cust,
	count(distinct  case when channel_code in('1','7','9') and is_new_sign='是' then customer_no end) count_cust_xqy,
	count(distinct case when channel_code in('1','7','9') and is_new_sale='是' then customer_no end) count_cust_xcj,
	count(distinct  case when channel_code in('1','7','9') and (attribute_code='1' or attribute_code is null) then customer_no end) count_cust_rp,
	count(distinct  case when channel_code in('1','7','9') and attribute_code='2' then customer_no end) count_cust_fl,
	count(distinct case when channel_code in('1','7','9') and attribute_code='3' then customer_no end) count_cust_my,
	count(distinct case when channel_code in('1','7','9') and attribute_code not in('1','2','3') then customer_no end) count_cust_sm,
	sum(case when channel_code in('1','7','9') and (attribute_code='1' or attribute_code is null) then no_tax_sales end)/10000 sale_cust_rp,
	sum(case when channel_code in('1','7','9') and attribute_code='2' then no_tax_sales end)/10000 sale_cust_fl,
	sum(case when channel_code in('1','7','9') and attribute_code='3' then no_tax_sales end)/10000 sale_cust_my,
	sum(case when channel_code in('1','7','9') and attribute_code not in('1','2','3') then no_tax_sales end)/10000 sale_cust_sm
from 
	csx_data_market.report_sale_r_d_achievement
where 
	substr(sdt,1,6)='${EDATE}'
	and (region_name not like '%大宗%' and region_name not like '%供应链%')
group by 
	region_name,case when substr(sdt,1,6)<='202107' and province_name like '江苏%' then '江苏省' else province_name end,
	if(province_name='福建省',substr(city_group_name,1,3),city_group_name);