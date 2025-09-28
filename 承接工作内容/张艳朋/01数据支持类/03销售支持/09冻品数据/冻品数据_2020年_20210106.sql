--====================================================================================================================================
--三级分类维度
select
	province_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	sum(a.sales_value) as sales_value,
	sum(a.profit) as profit,
	sum(a.profit)/abs(sum(a.sales_value)) as profit_rate
from 
	(
	select 
		province_name,goods_code,
		sum(sales_value)as sales_value,
		sum(profit)as profit
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20200101' and '20201231'
		and business_type_code not in ('4') -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
	group by 
		province_name,goods_code
	)a 
	left join   
		(
		select 
			goods_id,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name
		from 
			csx_dw.dws_basic_w_a_csx_product_m
		where 
			sdt ='20201231'
		group by 
			goods_id,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name
		) c on a.goods_code=c.goods_id
group by 
	province_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name
