-- 昨日最新
set current_day = '20211202';

-- 开始日期
set current_start_day = '20210901';

-- 结束日期
set current_end_day = '20211130';

insert overwrite directory '/tmp/zhangyanpeng/20211025_goods_evaluation_01' row format delimited fields terminated by '\t' 

select
	t1.province_name,
	t1.classify_large_name,
	t1.classify_middle_name,
	t1.classify_small_name,
	t1.goods_code,
	t1.goods_name,
	t1.unit_name,
	t1.category_small_code,
	t1.category_small_name,
	t1.sales_value,
	t1.sales_qty,
	t1.cnt_days,
	t1.by_cust_count,
	t1.by_cust_count/t3.customer_cnt as penetration_rate,
	t1.cnt_goods,
	t1.profit
from
	(
	select
		a.province_name,
		c.classify_large_name,
		c.classify_middle_name,
		c.classify_small_name,
		a.goods_code,
		c.goods_name,
		c.unit_name,
		c.category_small_code,
		c.category_small_name,
		sum(a.sales_value) sales_value,
		sum(a.sales_qty) sales_qty,
		count(distinct a.sdt) as cnt_days,
		count(distinct a.customer_no) by_cust_count,
		count(a.goods_code) as cnt_goods,
		sum(profit) as profit
	from 
		(
		select 
			sdt,province_name,goods_code,customer_no,sales_value,sales_qty,profit
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
			and channel_code in ('1','7','9')
			and business_type_code ='1' -- 仅日配 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
			and province_name='北京市'
			and customer_no in ('123614','123626','123620','123428','123543','123443','122929','123004','122990','122771','122611','122565','121191','121125','120385',
			'120354','120120','118925','115849','115812','114887','112276','106458')
			--and dc_code='W0A7'
		)a  
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
		a.province_name,
		c.classify_large_name,
		c.classify_middle_name,
		c.classify_small_name,
		a.goods_code,
		c.goods_name,
		c.unit_name,
		c.category_small_code,
		c.category_small_name
	) as t1
	left join   --下单客户数
		(
		select 
			province_name,
			count(distinct customer_no) as customer_cnt
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
			and channel_code in ('1','7','9')
			and business_type_code ='1' -- 仅日配 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
			and province_name='北京市'
			and customer_no in ('123614','123626','123620','123428','123543','123443','122929','123004','122990','122771','122611','122565','121191','121125','120385',
			'120354','120120','118925','115849','115812','114887','112276','106458')
			--and dc_code='W0A7'
		group by 
			province_name
		) t3 on t1.province_name=t3.province_name
		