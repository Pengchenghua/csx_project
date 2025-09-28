--====================================================================================================================================
-- 采购
insert overwrite directory '/tmp/zhangyanpeng/20210205_sale_huaxi_b' row format delimited fields terminated by '\t'

select
	a.region_name,
	a.dc_province_name,
	a.dc_city_name,
	a.smonth,
	c.channel_name,
	a.business_type_name,
	a.customer_no,
	c.customer_name,
	c.attribute_desc,
	b.department_id,
	b.department_name,
	sum(a.sales_value) as sales_value,
	sum(a.profit) as profit,
	sum(a.profit)/abs(sum(a.sales_value)) as profit_prorate,
	sum(a.excluding_tax_sales) as excluding_tax_sales,
	sum(a.excluding_tax_profit) as excluding_tax_profit,
	sum(a.excluding_tax_profit)/abs(sum(a.excluding_tax_sales)) as excluding_tax_profit_prorate
from
	(
	select
		region_name,dc_province_name,dc_city_name,substr(sdt,1,6)smonth,business_type_name,customer_no,work_no,goods_code,
		sales_value,profit,excluding_tax_sales,excluding_tax_profit
	from
		csx_dw.dws_sale_r_d_detail
	where
		sdt>='20190101' and sdt<='20191231'
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and region_name='华西大区'
	) as a
	left join --商品信息
		(
		select
			goods_id,goods_name,unit_name,brand_name,classify_large_code,classify_large_name,classify_middle_code,
			classify_middle_name,classify_small_code,classify_small_name,department_id,department_name
		from
			csx_dw.dws_basic_w_a_csx_product_m
		where
			sdt = 'current'
		group by 
			goods_id,goods_name,unit_name,brand_name,classify_large_code,classify_large_name,classify_middle_code,
			classify_middle_name,classify_small_code,classify_small_name,department_id,department_name				
		) as b on b.goods_id=a.goods_code
	left join -- 客户信息
		(
		select 
			customer_no,customer_name,channel_name,first_category_name,second_category_name,third_category_name,attribute_desc
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt='current'
		group by 
			customer_no,customer_name,channel_name,first_category_name,second_category_name,third_category_name,attribute_desc
		) as c on c.customer_no=a.customer_no
group by 
	a.region_name,
	a.dc_province_name,
	a.dc_city_name,
	a.smonth,
	c.channel_name,
	a.business_type_name,
	a.customer_no,
	c.customer_name,
	c.attribute_desc,
	b.department_id,
	b.department_name	
	
	
	
	
--====================================================================================================================================
-- 采购
insert overwrite directory '/tmp/zhangyanpeng/20210205_sale_huaxi_m' row format delimited fields terminated by '\t'

select
	a.region_name,
	a.dc_province_name,
	a.dc_city_name,
	a.smonth,
	'商超' as channel_name,
	a.business_type_name,
	a.customer_no,
	c.shop_name,
	'商超' as attribute_desc,
	b.department_id,
	b.department_name,
	sum(a.sales_value) as sales_value,
	sum(a.profit) as profit,
	sum(a.profit)/abs(sum(a.sales_value)) as profit_prorate,
	sum(a.excluding_tax_sales) as excluding_tax_sales,
	sum(a.excluding_tax_profit) as excluding_tax_profit,
	sum(a.excluding_tax_profit)/abs(sum(a.excluding_tax_sales)) as excluding_tax_profit_prorate
from
	(
	select
		region_name,dc_province_name,dc_city_name,substr(sdt,1,6)smonth,business_type_name,customer_no,work_no,goods_code,
		sales_value,profit,excluding_tax_sales,excluding_tax_profit
	from
		csx_dw.dws_sale_r_d_detail
	where
		sdt>='20190101' and sdt<='20191231'
		and channel_code in('2') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and region_name='华西大区'
	) as a
	left join --商品信息
		(
		select
			goods_id,goods_name,unit_name,brand_name,classify_large_code,classify_large_name,classify_middle_code,
			classify_middle_name,classify_small_code,classify_small_name,department_id,department_name
		from
			csx_dw.dws_basic_w_a_csx_product_m
		where
			sdt = 'current'
		group by 
			goods_id,goods_name,unit_name,brand_name,classify_large_code,classify_large_name,classify_middle_code,
			classify_middle_name,classify_small_code,classify_small_name,department_id,department_name				
		) as b on b.goods_id=a.goods_code
	left join -- 客户信息
		(
		select 
			shop_id,shop_name
		from 
			csx_dw.dws_basic_w_a_csx_shop_m
		where 
			sdt='current'
		group by 
			shop_id,shop_name
		) as c on concat('S',c.shop_id)=a.customer_no
group by 
	a.region_name,
	a.dc_province_name,
	a.dc_city_name,
	a.smonth,
	--c.channel_name,
	a.business_type_name,
	a.customer_no,
	c.shop_name,
	--c.attribute_desc,
	b.department_id,
	b.department_name	
	
	
	
	
	

				