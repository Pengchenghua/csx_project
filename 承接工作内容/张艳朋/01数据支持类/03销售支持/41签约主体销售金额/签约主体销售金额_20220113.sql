insert overwrite directory '/tmp/zhangyanpeng/20220113_01' row format delimited fields terminated by '\t' 

select
	a.shop_company_code,
	d.name,
	a.customer_no,
	c.customer_name,
	b.classify_large_name,
	b.classify_middle_name,
	sum(sales_value) as sales_value
from
	(
	select
		customer_no,shop_company_code,shop_company_name,goods_code,sales_value
	from
		csx_dw.dws_sale_r_d_detail
	where
		sdt between '20190101' and '20220112'
		and channel_code in ('1','7','9')
		and shop_company_code in ('2115','2121')
	) a 
	left join
		(
		select
			goods_id,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name
		from 
			csx_dw.dws_basic_w_a_csx_product_m
		where 
			sdt = 'current'
		) b on a.goods_code = b.goods_id
	left join 
		(
		select 
			customer_no,customer_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt='current'
		) c on a.customer_no=c.customer_no
	left join
		(
		select
			code,name
		from
			csx_dw.dws_basic_w_a_company_code
		where
			sdt='current'
		) d on a.shop_company_code=d.code
group by 
	a.shop_company_code,
	d.name,
	a.customer_no,
	c.customer_name,
	b.classify_large_name,
	b.classify_middle_name	
		
	