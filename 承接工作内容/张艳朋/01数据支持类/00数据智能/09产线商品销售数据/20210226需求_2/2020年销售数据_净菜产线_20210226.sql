-- ===================================================================================================================

insert overwrite directory '/tmp/zhangyanpeng/20210112_line_data_b_tmp' row format delimited fields terminated by '\t'	
		
--=============================================================================================================================================

select
	t1.region_name,
	t1.province_name,
	t1.channel_type,
	sum(t1.sales_value) as sales_value,
	sum(t1.profit) as profit,
	sum(t1.profit)/abs(sum(t1.sales_value)) as profit_prorate,
	sum(t1.sales_cost) as sales_cost,
	sum(t1.middle_office_cost) as middle_office_cost,
	sum(t1.front_profit) as front_profit
from
	(
	select 
		region_name,province_name,goods_code,
		sales_value,profit,sales_cost,middle_office_cost,front_profit,case when channel_code in ('1','7','9') then 'B' when channel_code='2' then 'M' else '其他' end as channel_type
    from 
		csx_dw.dws_sale_r_d_detail
    where 
		sdt >= '20200101'
		and sdt <= '20201231'
		and channel_code in ('1','2','7','9')
		and business_type_code !='4'
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
	t1.region_name,
	t1.province_name,
	t1.channel_type	
	
	
	
	
--=============================================================================================================================================
select
	*
from
	(
	select
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
			goods_code,sales_value,profit,sales_cost,middle_office_cost,front_profit
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt >= '20200101'
			and sdt <= '20201231'
			and channel_code in ('1','2','7','9')
			and business_type_code !='4'
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
		t1.goods_code,
		t2.goods_name
	) tmp1 
where
	rn<=200




