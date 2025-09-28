--====================================================================================================================================
-- 采购
insert overwrite directory '/tmp/zhangyanpeng/20210203_sale_beijing' row format delimited fields terminated by '\t'

select
	a.region_name,
	a.dc_province_name,
	a.dc_city_name,
	a.smonth,
	c.channel_name,
	a.business_type_name,
	a.perform_dc_code,
	a.perform_dc_name,
	a.customer_no,
	c.customer_name,
	c.sign_time,
	a.attribute_desc,
	c.first_category_name,
	c.second_category_name,
	c.third_category_name,
	a.work_no,
	a.sales_name,
	b.department_id,
	b.department_name,
	b.classify_large_name,
	b.classify_middle_name,
	a.goods_code,
	b.goods_name,
	b.unit_name,
	--a.sales_price,
	--a.cost_price,
	--a.purchase_price,
	--a.middle_office_price,
	a.sales_qty,
	a.sales_value,
	a.profit,
	a.profit_prorate,
	a.excluding_tax_sales,
	a.excluding_tax_profit,
	a.excluding_tax_profit_prorate
from
	(
	select
		region_name,dc_province_name,dc_city_name,substr(sdt,1,6)smonth,business_type_name,perform_dc_code,perform_dc_name,customer_no,work_no,sales_name,attribute_desc,goods_code,
		-- sales_price,cost_price,purchase_price,middle_office_price,
		sum(sales_qty) as sales_qty,
		sum(sales_value) as sales_value,
		sum(profit) as profit,
		sum(profit)/abs(sum(sales_value)) as profit_prorate,
		sum(excluding_tax_sales) as excluding_tax_sales,
		sum(excluding_tax_profit) as excluding_tax_profit,
		sum(excluding_tax_profit)/abs(sum(excluding_tax_sales)) as excluding_tax_profit_prorate
	from
		csx_dw.dws_sale_r_d_detail
	where
		sdt>='20200101' and sdt<='20201231'
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and province_name='北京市'
	group by 
		region_name,dc_province_name,dc_city_name,substr(sdt,1,6),business_type_name,perform_dc_code,perform_dc_name,customer_no,work_no,sales_name,attribute_desc,goods_code		
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
			customer_no,customer_name,channel_name,regexp_replace(to_date(sign_time),'-','') as sign_time,first_category_name,second_category_name,third_category_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt='current'
		group by 
			customer_no,customer_name,channel_name,regexp_replace(to_date(sign_time),'-',''),first_category_name,second_category_name,third_category_name
		) as c on c.customer_no=a.customer_no
	
				