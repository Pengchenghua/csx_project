--====================================================================================================================================
--三级分类维度
select
	province_name,
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
	count(distinct a.customer_no) as customer_rate,
	count(distinct a.goods_code) by_goods_count
from 
	(
	select 
		province_name,dc_code,customer_no,channel_code,goods_code,
		sum(sales_value)as sales_value,
		sum(profit)as profit
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20201201' and '20210113'
		and channel_code in('1','7','9')
		and business_type_code in('1','2') -- 只要日配+福利  业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
		and dc_code ='W0A6'
	group by 
		province_name,dc_code,customer_no,channel_code,goods_code
	)a  
	left join   
		(
		select 
			goods_id,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name
		from 
			csx_dw.dws_basic_w_a_csx_product_m
		where 
			sdt ='current'
		group by 
			goods_id,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name
		) c on a.goods_code=c.goods_id
group by 
	province_name,
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
	dc_code,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	sum(a.sales_value) as sales_value,
	sum(a.profit) as profit,
	sum(a.profit)/abs(sum(a.sales_value)) as profit_rate,
	count(distinct a.customer_no) as customer_rate,
	count(distinct a.goods_code) by_goods_count
from 
	(
	select 
		province_name,dc_code,customer_no,channel_code,goods_code,
		sum(sales_value)as sales_value,
		sum(profit)as profit
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20201201' and '20210113'
		and channel_code in('1','7','9')
		and business_type_code in('1','2') -- 只要日配+福利  业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
		and dc_code ='W0A6'
	group by 
		province_name,dc_code,customer_no,channel_code,goods_code
	)a  
	left join   
		(
		select 
			goods_id,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name
		from 
			csx_dw.dws_basic_w_a_csx_product_m
		where 
			sdt ='current'
		group by 
			goods_id,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name
		) c on a.goods_code=c.goods_id
group by 
	province_name,
	dc_code,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name
