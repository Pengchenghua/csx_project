--====================================================================================================================================
--三级分类维度
select
	province_name,
	city_group_name,
	dc_code,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	sum(a.sales_value) as sales_value,
	sum(a.profit) as profit,
	sum(a.profit)/abs(sum(a.sales_value)) as profit_rate,
	count(distinct a.customer_no) by_cust_count,
	count(distinct a.goods_code) by_goods_count
from 
	(
	select 
		province_name,city_group_name,dc_code,customer_no,channel,goods_code,
		sum(sales_value)as sales_value,
		sum(profit)as profit
	from 
		csx_dw.dws_sale_r_d_customer_sale 
	where 
		sdt between '20200622' and '20201221'
		and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
		and channel in('1','9') -- 只要日配+福利
		and attribute not in('合伙人客户','贸易客户') -- 只要日配+福利
		and province_name not like '平台%'
		and dc_code ='W0A7' --仓库
	group by 
		province_name,city_group_name,dc_code,customer_no,channel,goods_code
	)a  
	left join   
		(
		select 
			goods_id,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name
		from 
			csx_dw.dws_basic_w_a_csx_product_m
		where 
			sdt ='current'
		) c on a.goods_code=c.goods_id
group by 
	province_name,
	city_group_name,
	dc_code,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name
;







--====================================================================================================================================
--二级分类维度
select
	province_name,
	city_group_name,
	dc_code,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	sum(a.sales_value) as sales_value,
	sum(a.profit) as profit,
	sum(a.profit)/abs(sum(a.sales_value)) as profit_rate,
	count(distinct a.customer_no) by_cust_count,
	count(distinct a.goods_code) by_goods_count
from 
	(
	select 
		province_name,city_group_name,dc_code,customer_no,channel,goods_code,
		sum(sales_value)as sales_value,
		sum(profit)as profit
	from 
		csx_dw.dws_sale_r_d_customer_sale 
	where 
		sdt between '20200622' and '20201221'
		and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
		and channel in('1','9') -- 只要日配+福利
		and attribute not in('合伙人客户','贸易客户') -- 只要日配+福利
		and province_name not like '平台%'
		and dc_code ='W0A7' --仓库
	group by 
		province_name,city_group_name,dc_code,customer_no,channel,goods_code
	)a  
	left join   
		(
		select 
			goods_id,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name
		from 
			csx_dw.dws_basic_w_a_csx_product_m
		where 
			sdt ='current'
		) c on a.goods_code=c.goods_id
group by 
	province_name,
	city_group_name,
	dc_code,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name
;