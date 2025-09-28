-- 昨日最新
set current_day = '20220221';

-- 开始日期
set current_start_day = '20210801';

-- 结束日期
set current_end_day = '20220131';

insert overwrite directory '/tmp/zhangyanpeng/20220222_goods_evaluation_01' row format delimited fields terminated by '\t' 

-- 省区+商品

select
	t1.province_name,
	t1.city_group_name,
	t1.classify_large_name,
	t1.classify_middle_name,
	t1.classify_small_name,
	t1.brand_name,
	t1.spu_goods_name,
	t1.price_belt_type,
	t1.goods_code,
	t1.goods_name,
	t1.unit_name,
	t1.standard,
	t1.sales_value,
	t1.sales_qty,
	t1.profit_rate,
	t1.cnt_days,
	t1.by_cust_count/t3.customer_cnt as penetration_rate
from
	(
	select
		b.province_name,
		b.city_group_name,
		c.classify_large_name,
		c.classify_middle_name,
		c.classify_small_name,
		c.brand_name,
		c.spu_goods_name,
		c.price_belt_type,		
		a.goods_code,
		c.goods_name,
		c.unit_name,
		c.standard,
		sum(a.sales_value) sales_value,
		sum(a.sales_qty) sales_qty,
		sum(profit)/abs(sum(a.sales_value)) as profit_rate,
		count(distinct a.sdt) as cnt_days,
		count(distinct a.customer_no) by_cust_count,
		sum(profit) as profit
	from 
		(
		select 
			sdt,province_name,city_group_name,goods_code,customer_no,sales_value,sales_qty,profit,dc_code
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
			and channel_code in ('1','7','9')
			and business_type_code ='1' -- 仅日配 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
			and dc_code in ('W0A3','W0A8','W0A7','W0A6','W0N0','W0A2')
		)a 
		left join
			(
			select 
				customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
				sales_region_code,sales_region_name,province_code,province_name,city_group_code,city_group_name,business_type_name
			from 
				csx_dw.dws_crm_w_a_customer
			where 
				sdt = 'current'
			)b on b.customer_no=a.customer_no			
		left join   --商品表
			(
			select 
				goods_id,goods_name,unit_name,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name,
				category_small_code,category_small_name,brand_name,spu_goods_name,price_belt_type,standard
			from 
				csx_dw.dws_basic_w_a_csx_product_m
			where 
				sdt =${hiveconf:current_end_day}
			) c on a.goods_code=c.goods_id
	group by 
		b.province_name,
		b.city_group_name,
		c.classify_large_name,
		c.classify_middle_name,
		c.classify_small_name,
		c.brand_name,
		c.spu_goods_name,
		c.price_belt_type,		
		a.goods_code,
		c.goods_name,
		c.unit_name,
		c.standard
	) as t1
	left join   --下单客户数
		(
		select 
			b.province_name,
			b.city_group_name,
			count(distinct a.customer_no) as customer_cnt
		from 
			(
			select
				customer_no
			from
				csx_dw.dws_sale_r_d_detail 
			where 
				sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
				and channel_code in ('1','7','9')
				and business_type_code ='1' -- 仅日配 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
				and dc_code in ('W0A3','W0A8','W0A7','W0A6','W0N0','W0A2')
			)a 
			left join
				(
				select 
					customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
					sales_region_code,sales_region_name,province_code,province_name,city_group_code,city_group_name,business_type_name
				from 
					csx_dw.dws_crm_w_a_customer
				where 
					sdt = 'current'
				)b on b.customer_no=a.customer_no	
		group by 
			b.province_name,b.city_group_name
		) t3 on t1.province_name=t3.province_name and t1.city_group_name=t3.city_group_name
;