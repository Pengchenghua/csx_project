--====================================================================================================================================
--三级分类维度
select
	'20200727-20210126' as period,
	t1.province_name,
	t1.dc_code,
	t1.classify_large_code,
	t1.classify_large_name,
	t1.classify_middle_code,
	t1.classify_middle_name,
	t1.classify_small_code,
	t1.classify_small_name,
	t1.sales_value,
	t1.profit,
	t1.profit_rate,
	t1.customer_cnt/t2.customer_cnt as customer_prorate,
	t1.by_goods_count
from
	(
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
		count(distinct a.customer_no) as customer_cnt,
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
			sdt between '20200727' and '20210126'
			and channel_code in('1','7','9')
			and business_type_code in('1','2') -- 只要日配+福利  业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
			and dc_code in ('W0N1','W0A5','W0N0','W0W7','W0A2')
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
	) t1
	left join   
		(
		select 
			dc_code,count(distinct customer_no) as customer_cnt
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between '20200727' and '20210126'
			and channel_code in('1','7','9')
			and business_type_code in('1','2') -- 只要日配+福利  业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
			and dc_code in ('W0N1','W0A5','W0N0','W0W7','W0A2')
		group by 
			dc_code
		) t2 on t2.dc_code=t1.dc_code	
;







--====================================================================================================================================
--二级分类维度
select
	'20200727-20210126' as period,
	t1.province_name,
	t1.dc_code,
	t1.classify_large_code,
	t1.classify_large_name,
	t1.classify_middle_code,
	t1.classify_middle_name,
	t1.sales_value,
	t1.profit,
	t1.profit_rate,
	t1.customer_cnt/t2.customer_cnt as customer_prorate,
	t1.by_goods_count
from
	(
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
		count(distinct a.customer_no) as customer_cnt,
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
			sdt between '20200727' and '20210126'
			and channel_code in('1','7','9')
			and business_type_code in('1','2') -- 只要日配+福利  业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
			and dc_code in ('W0N1','W0A5','W0N0','W0W7','W0A2')
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
	) t1
	left join   
		(
		select 
			dc_code,count(distinct customer_no) as customer_cnt
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between '20200727' and '20210126'
			and channel_code in('1','7','9')
			and business_type_code in('1','2') -- 只要日配+福利  业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
			and dc_code in ('W0N1','W0A5','W0N0','W0W7','W0A2')
		group by 
			dc_code
		) t2 on t2.dc_code=t1.dc_code	
