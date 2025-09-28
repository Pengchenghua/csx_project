-- 昨日最新
--set current_day = '20211202';

-- 开始日期
set current_start_day = '20220501';

-- 结束日期
set current_end_day = '20220731';

--set province_name = '上海市';
set dc_code='W0P8';


--商品明细
insert overwrite directory '/tmp/zhangyanpeng/20220725_01_01' row format delimited fields terminated by '\t' 

select
	a.dc_code,
	c.classify_large_name,
	c.classify_middle_name,
	--c.classify_small_name,
	c.category_small_name,
	c.brand_name,
	a.goods_code,
	c.goods_name,
	c.unit_name,
	c.standard,
	d.product_status_name,
	case when d.stock_properties='1' then '是' else '否' end as is_beihuo_goods,
	sum(a.sales_value) sales_value,
	sum(a.sales_qty) sales_qty,
	sum(a.profit)/abs(sum(a.sales_value)) as profit_rate,
	count(distinct a.customer_no) customer_cnt,
	count(distinct a.sdt) as day_cnt,
	count(a.goods_code) as goods_cnt,
	sum(case when a.sdt>='20220701' and a.sdt<='20220731' then a.sales_value else 0 end) as last_month_sales_value,
	count(distinct case when a.sdt>='20220701' and a.sdt<='20220731' then a.customer_no else null end) as last_month_customer_cnt
from 
	(
	select 
		sdt,province_name,goods_code,customer_no,dc_code,order_no,sales_value,sales_qty,profit
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
		and channel_code in ('1','7','9')
		and business_type_code ='1' -- 仅日配 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
		--and province_name=${hiveconf:province_name}
		and dc_code=${hiveconf:dc_code}
		--and customer_no not in ('118376','123311','128102','124003','126113','126259','127043','126069')
	)a  
	left join
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			sales_region_code,sales_region_name,sales_province_code,province_name,city_group_code,city_group_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = ${hiveconf:current_end_day}
		) b on b.customer_no=a.customer_no		
	left join   --商品表
		(
		select 
			goods_id,goods_name,unit_name,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name,
			category_small_code,category_small_name,brand_name,standard
		from 
			csx_dw.dws_basic_w_a_csx_product_m
		where 
			sdt =${hiveconf:current_end_day}
		) c on a.goods_code=c.goods_id
	left join
		(
		select
			distinct shop_code,product_code,product_status_name,stock_properties,stock_properties_name --1存储 2货到即配
		from
			csx_dw.dws_basic_w_a_csx_product_info 
		where 
			sdt = 'current'
		) d on d.shop_code=a.dc_code and d.product_code=a.goods_code
group by 
	a.dc_code,
	c.classify_large_name,
	c.classify_middle_name,
	--c.classify_small_name,
	c.category_small_name,
	c.brand_name,
	a.goods_code,
	c.goods_name,
	c.unit_name,
	c.standard,
	d.product_status_name,
	case when d.stock_properties='1' then '是' else '否' end
;

--客户明细
insert overwrite directory '/tmp/zhangyanpeng/20220725_01_02' row format delimited fields terminated by '\t' 

select
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	a.customer_no,
	b.customer_name,
	a.goods_code,
	c.goods_name,
	sum(a.sales_value) sales_value,
	sum(a.sales_qty) sales_qty,
	count(distinct a.sdt) as day_cnt,
	count(a.goods_code) goods_cnt,
	sum(a.profit) as profit
from 
	(
	select 
		sdt,province_name,goods_code,customer_no,order_no,sales_value,sales_qty,profit
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
		and channel_code in ('1','7','9')
		and business_type_code ='1' -- 仅日配 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
		--and province_name=${hiveconf:province_name}
		and dc_code=${hiveconf:dc_code}
		--and customer_no not in ('118376','123311','128102','124003','126113','126259','127043','126069')
	)a 
	left join
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			sales_region_code,sales_region_name,sales_province_code,province_name,city_group_code,city_group_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = ${hiveconf:current_end_day}
		) b on b.customer_no=a.customer_no
	left join   --商品表
		(
		select 
			goods_id,goods_name,unit_name,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name,
			category_small_code,category_small_name
		from 
			csx_dw.dws_basic_w_a_csx_product_m
		where 
			sdt =${hiveconf:current_end_day}
		) c on a.goods_code=c.goods_id
group by 
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	a.customer_no,
	b.customer_name,
	a.goods_code,
	c.goods_name
;



		