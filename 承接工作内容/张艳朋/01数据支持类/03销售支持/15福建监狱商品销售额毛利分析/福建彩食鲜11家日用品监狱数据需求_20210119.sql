--====================================================================================================================================
-- 福建监狱数据需求
insert overwrite directory '/tmp/zhangyanpeng/20210119_prison_fujian' row format delimited fields terminated by '\t'

select
	a.smonth,
	a.customer_no,
	b.customer_name,
	a.goods_code,
	c.goods_name,
	c.department_id,
	c.department_name,
	c.classify_large_name,
	c.classify_middle_name,
	c.classify_small_name,
	a.sales_qty,
	a.sales_value,
	a.profit	
from
	(
	select
		substr(sdt,1,6) as smonth,
		customer_no,
		goods_code,
		sum(sales_qty) as sales_qty,
		sum(sales_value)as sales_value,	
		sum(profit)as profit
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20190701' and '20201231'
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and customer_no in ('105150','105156','105164','105165','105177','105181','105182','106423','106721','106805','107404') 
	group by 
		substr(sdt,1,6),
		customer_no,
		goods_code
	) as a 
	left join
		(
		select
			customer_no,customer_name,first_category_name,second_category_name,third_category_name
		from
			csx_dw.dws_crm_w_a_customer
		where
			sdt='current'
		group by 
			customer_no,customer_name,first_category_name,second_category_name,third_category_name
		) as b on b.customer_no=a.customer_no
	left join
		(
		select
			goods_id,goods_name,department_id,department_name,classify_large_name,classify_middle_name,classify_small_name
		from
			csx_dw.dws_basic_w_a_csx_product_m
		where
			sdt='current'
		group by 
			goods_id,goods_name,department_id,department_name,classify_large_name,classify_middle_name,classify_small_name
		) as c on c.goods_id=a.goods_code