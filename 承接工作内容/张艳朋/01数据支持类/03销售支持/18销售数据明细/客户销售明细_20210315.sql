-- ================================================================================================================
-- 客户销售明细

-- 昨日最新
set current_day = '20210314';

-- 开始日期
set current_start_day = '20200101';

-- 结束日期
set current_end_day = '20210228';

insert overwrite directory '/tmp/zhangyanpeng/20210315_customer_sales_tmp' row format delimited fields terminated by '\t' 

select
	concat(${hiveconf:current_start_day},'-',${hiveconf:current_end_day}) as period,
	a.region_name,
	a.province_name,
	a.s_sdt,
	c.classify_large_name,
	c.classify_middle_name,
	c.classify_small_name,
	a.customer_no,
	b.customer_name,
	a.goods_code,
	c.goods_name,
	c.unit_name,
	a.sales_value,
	a.profit,
	a.front_profit
from
	(
	select 
		region_name,province_name,customer_no,goods_code,substr(sdt,1,6) as s_sdt,
		sum(sales_value) as sales_value,
		sum(profit) as profit,
		sum(front_profit) as front_profit
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and customer_no='106599'
	group by 
		region_name,province_name,customer_no,goods_code,substr(sdt,1,6)
	) a
	left join
		(
		select 
			customer_no,customer_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and customer_no<>''	
		group by
			customer_no,customer_name
		) b on b.customer_no = a.customer_no
	left join
		(
		select 
			goods_id,goods_name,unit_name,classify_large_name,classify_middle_name,classify_small_name
		from 
			csx_dw.dws_basic_w_a_csx_product_m
		where 
			sdt ='current'
		group by
			goods_id,goods_name,unit_name,classify_large_name,classify_middle_name,classify_small_name
		) c on c.goods_id=a.goods_code
		


