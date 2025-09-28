--============================================================================================================================================================
-- 华西大区销售数据
insert overwrite directory '/tmp/zhangyanpeng/20210415_linshi_1' row format delimited fields terminated by '\t' 
select
	c.province_name,
	a.city_group_name,
	a.customer_no,
	b.customer_name,
	b.work_no,
	b.sales_name,
	e.classify_middle_name,
	sum(sales_value) as sales_value
from
	(
	select
		region_code,province_code,city_group_name,substr(sdt,1,6) as s_sdt,goods_code,customer_no,business_type_name,sales_value
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt >= '20200301' 
		and sdt <= '20210301'
		and channel_code in ('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		-- and region_name='华西大区'
	) as a
	left join 
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt='current'
		group by 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name
		) b on a.customer_no=b.customer_no
	left join 
		(
		select 
			province_code,province_name,region_code,region_name
		from 
			csx_dw.dws_sale_w_a_area_belong
		group by 
			province_code,province_name,region_code,region_name
		) c on c.province_code=a.province_code	
	left join 
		(
		select 
			customer_no,customer_name,first_order_date
		from 
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt='current'
		group by 
			customer_no,customer_name,first_order_date
		) d on d.customer_no=a.customer_no	
	left join 
		(
		select 
			goods_id,goods_name,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name
		from 
			csx_dw.dws_basic_w_a_csx_product_m
		where 
			sdt='current'
		group by 
			goods_id,goods_name,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name
		) e on e.goods_id=a.goods_code	
where
	e.classify_middle_code='B0603' -- 管理中类 食用油类
group by 
	c.province_name,
	a.city_group_name,
	a.customer_no,
	b.customer_name,
	b.work_no,
	b.sales_name,
	e.classify_middle_name
		