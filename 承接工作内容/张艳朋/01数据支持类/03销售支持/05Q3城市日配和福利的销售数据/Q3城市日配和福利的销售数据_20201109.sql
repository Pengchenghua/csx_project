select
	b.region_name,
	a.province_name,
	a.city_group_name,
	case when a.channel ='7' then 'BBC'	
		when a.channel in ('1') and attribute='合伙人客户' then '城市服务商' 
		when a.channel in ('1') and attribute='贸易客户'  then '贸易客户' 
		when a.channel in ('1') and order_kind='WELFARE' then '福利单'  
		when a.channel in ('1') and attribute not in('合伙人客户','贸易客户') and (order_kind<>'WELFARE'or order_kind is null) then '日配单'	 
		else '其他' end sale_group, 	
	sum(a.sales_value) sales_value,
	sum(a.profit) profit
from 
	(
	select 
		channel,province_code,province_name,city_group_name,sdt,substr(sdt,1,6) smonth,
		customer_no,attribute,order_kind,
		sum(sales_value)as sales_value,
		sum(profit)as profit
	from 
		csx_dw.dws_sale_r_d_customer_sale 
	where 
		sdt between '20200701' and '20200930'
		and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
		and channel in('1','7','9')
		--and attribute not in('合伙人客户','贸易客户')
		and province_name not like '平台%'
	group by 
		channel,province_code,province_name,city_group_name,sdt,substr(sdt,1,6),
		customer_no,attribute,order_kind
	)a 
	left join
		(
		select 
			province_code,province_name,region_code,region_name 
		from 
			csx_dw.dim_area 
		where 
			area_rank=13
		group by
			province_code,province_name,region_code,region_name 
		) b on b.province_code=a.province_code	
group by 
	b.region_name,
	a.province_name,
	a.city_group_name,
	case when a.channel ='7' then 'BBC'	
		when a.channel in ('1') and attribute='合伙人客户' then '城市服务商' 
		when a.channel in ('1') and attribute='贸易客户'  then '贸易客户' 
		when a.channel in ('1') and order_kind='WELFARE' then '福利单'  
		when a.channel in ('1') and attribute not in('合伙人客户','贸易客户') and (order_kind<>'WELFARE'or order_kind is null) then '日配单'	 
		else '其他' end