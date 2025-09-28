--=============================================================================================================================================
select
	*
from
	(
	select
		t1.province_name,
		t1.goods_code,
		t2.goods_name,
		sum(t1.sales_value) as sales_value,
		sum(t1.profit) as profit,
		sum(t1.profit)/abs(sum(t1.sales_value)) as profit_prorate,
		sum(t1.sales_cost) as sales_cost,
		sum(t1.middle_office_cost) as middle_office_cost,
		sum(t1.front_profit) as front_profit,
		row_number() over(order by sum(t1.sales_value) desc) as rn
	from
		(
		select 
			province_name,goods_code,sales_value,profit,sales_cost,middle_office_cost,front_profit
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt >= '20200101'
			and sdt <= '20201231'
			and channel_code in ('1','7','9')
			and business_type_code !='4'
			and province_name='浙江省'
		) as t1
		left join 
			(
			select 
				goods_id,goods_name,department_id,department_name,classify_large_name,classify_middle_name,classify_small_name
			from 
				csx_dw.dws_basic_w_a_csx_product_m
			where 
				sdt = 'current'
			) t2 on t2.goods_id = t1.goods_code
	where
		t2.classify_small_name='净菜'
	group by 
		t1.province_name,
		t1.goods_code,
		t2.goods_name
	) tmp1 
where
	rn<=200


	
--=============================================================================================================================================
select
	*
from
	(
	select
		t1.province_name,
		t1.goods_code,
		t2.goods_name,
		sum(t1.sales_value) as sales_value,
		sum(t1.profit) as profit,
		sum(t1.profit)/abs(sum(t1.sales_value)) as profit_prorate,
		sum(t1.sales_cost) as sales_cost,
		sum(t1.middle_office_cost) as middle_office_cost,
		sum(t1.front_profit) as front_profit,
		row_number() over(order by sum(t1.sales_value) desc) as rn
	from
		(
		select 
			province_name,goods_code,sales_value,profit,sales_cost,middle_office_cost,front_profit
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt >= '20200101'
			and sdt <= '20201231'
			and channel_code in ('1','7','9')
			and business_type_code !='4'
			and province_name='重庆市'
		) as t1
		left join 
			(
			select 
				goods_id,goods_name,department_id,department_name,classify_large_name,classify_middle_name,classify_small_name
			from 
				csx_dw.dws_basic_w_a_csx_product_m
			where 
				sdt = 'current'
			) t2 on t2.goods_id = t1.goods_code
	where
		t2.classify_small_name='净菜'
	group by 
		t1.province_name,
		t1.goods_code,
		t2.goods_name
	) tmp1 
where
	rn<=200
	
	
	
--=============================================================================================================================================
select
	*
from
	(
	select
		t1.province_name,
		t1.goods_code,
		t2.goods_name,
		sum(t1.sales_value) as sales_value,
		sum(t1.profit) as profit,
		sum(t1.profit)/abs(sum(t1.sales_value)) as profit_prorate,
		sum(t1.sales_cost) as sales_cost,
		sum(t1.middle_office_cost) as middle_office_cost,
		sum(t1.front_profit) as front_profit,
		row_number() over(order by sum(t1.sales_value) desc) as rn
	from
		(
		select 
			province_name,goods_code,sales_value,profit,sales_cost,middle_office_cost,front_profit
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt >= '20200101'
			and sdt <= '20201231'
			and channel_code in ('1','7','9')
			and business_type_code !='4'
			and province_name='安徽省'
		) as t1
		left join 
			(
			select 
				goods_id,goods_name,department_id,department_name,classify_large_name,classify_middle_name,classify_small_name
			from 
				csx_dw.dws_basic_w_a_csx_product_m
			where 
				sdt = 'current'
			) t2 on t2.goods_id = t1.goods_code
	where
		t2.classify_small_name='净菜'
	group by 
		t1.province_name,
		t1.goods_code,
		t2.goods_name
	) tmp1 
where
	rn<=200
	
	
	
--=============================================================================================================================================
select
	*
from
	(
	select
		t1.province_name,
		t1.goods_code,
		t2.goods_name,
		sum(t1.sales_value) as sales_value,
		sum(t1.profit) as profit,
		sum(t1.profit)/abs(sum(t1.sales_value)) as profit_prorate,
		sum(t1.sales_cost) as sales_cost,
		sum(t1.middle_office_cost) as middle_office_cost,
		sum(t1.front_profit) as front_profit,
		row_number() over(order by sum(t1.sales_value) desc) as rn
	from
		(
		select 
			province_name,goods_code,sales_value,profit,sales_cost,middle_office_cost,front_profit
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt >= '20210101'
			and sdt <= '20210225'
			and channel_code in ('1','7','9')
			and business_type_code !='4'
			and province_name='重庆市'
		) as t1
		left join 
			(
			select 
				goods_id,goods_name,department_id,department_name,classify_large_name,classify_middle_name,classify_small_name
			from 
				csx_dw.dws_basic_w_a_csx_product_m
			where 
				sdt = 'current'
			) t2 on t2.goods_id = t1.goods_code
	where
		t2.classify_small_name='净菜'
	group by 
		t1.province_name,
		t1.goods_code,
		t2.goods_name
	) tmp1 
where
	rn<=200
	
	
	
--=============================================================================================================================================
select
	*
from
	(
	select
		t1.province_name,
		t1.goods_code,
		t2.goods_name,
		sum(t1.sales_value) as sales_value,
		sum(t1.profit) as profit,
		sum(t1.profit)/abs(sum(t1.sales_value)) as profit_prorate,
		sum(t1.sales_cost) as sales_cost,
		sum(t1.middle_office_cost) as middle_office_cost,
		sum(t1.front_profit) as front_profit,
		row_number() over(order by sum(t1.sales_value) desc) as rn
	from
		(
		select 
			province_name,goods_code,sales_value,profit,sales_cost,middle_office_cost,front_profit
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt >= '20210101'
			and sdt <= '20210225'
			and channel_code in ('1','7','9')
			and business_type_code !='4'
			and province_name='四川省'
		) as t1
		left join 
			(
			select 
				goods_id,goods_name,department_id,department_name,classify_large_name,classify_middle_name,classify_small_name
			from 
				csx_dw.dws_basic_w_a_csx_product_m
			where 
				sdt = 'current'
			) t2 on t2.goods_id = t1.goods_code
	where
		t2.classify_small_name='净菜'
	group by 
		t1.province_name,
		t1.goods_code,
		t2.goods_name
	) tmp1 
where
	rn<=200
	

	
	
	



	



