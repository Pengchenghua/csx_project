select 
	substr(sdt,1,6) as smonth, 
	province_name, 
	sum(sales_value) as sales_value,
	sum(profit) as profit,
	sum(profit)/sum(sales_value) as profit_rate
from 
	csx_dw.dws_sale_r_d_customer_sale
where 
	sdt between '20200601' and '20201031'
	and channel in ('7')
	and province_name not like '平台%'
	and province_name='福建省'
group by 
	substr(sdt,1,6),province_name
order by 
	smonth