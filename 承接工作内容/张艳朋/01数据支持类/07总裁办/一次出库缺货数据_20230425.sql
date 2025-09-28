select
	*
from
	csx_data_market.report_oms_out_of_stock_goods_1d 
where
	delivery_date between '2023-03-01' and '2023-04-24'
	and performance_city_name in('北京市','合肥市','莆田市','深圳市')
	-- and customer_code in ('126377','223402','178615','104924','123443','123490','123870','123920','106226','120428','129131','124175','130721','129092','125686','121054',
	-- '113698','105750','121466','123028','121308','224221','224423','113574','129481','105703','129471','129818','129811','129806','129796','129794','127054')
select
	*
from
	csx_report.csx_report_oms_out_of_stock_goods_2m
where
	delivery_date between '20230301' and '20230424'
	and performance_city_name in('北京市','合肥市','莆田市','深圳市')
	-- and customer_code in ('126377','223402','178615','104924','123443','123490','123870','123920','106226','120428','129131','124175','130721','129092','125686','121054',
	-- '113698','105750','121466','123028','121308','224221','224423','113574','129481','105703','129471','129818','129811','129806','129796','129794','127054')
	
select
	performance_province_name,performance_city_name,customer_code,delivery_date,count(goods_code) as sku_cnt,count(case when is_out_of_stock=1 then goods_code else null end) as qh_sku_cnt
from
	csx_report.csx_report_oms_out_of_stock_goods_2m
where
	delivery_date between '20230301' and '20230424'
	and performance_city_name in('北京市','合肥市','莆田市','深圳市')
	-- and customer_code in ('126377','223402','178615','104924','123443','123490','123870','123920','106226','120428','129131','124175','130721','129092','125686','121054',
	-- '113698','105750','121466','123028','121308','224221','224423','113574','129481','105703','129471','129818','129811','129806','129796','129794','127054')
group by 
	performance_province_name,performance_city_name,customer_code,delivery_date