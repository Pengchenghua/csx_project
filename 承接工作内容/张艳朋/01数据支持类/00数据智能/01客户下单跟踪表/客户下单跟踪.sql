		
--B端客户订单明细		
select
	a.sdt as `日期`,
	a.dc_code as `DC编码`,
	a.dc_name as `DC名称`,
	a.dc_city_name as `DC所在城市`,
	a.customer_no as `客户编号`,
	a.customer_name as `客户名称`,
	a.child_customer_no as `子客户编号`,
	a.child_customer_name as `子客户名称`,
	c.base_kilometre as `公里数`,
	a.order_no as `订单号`,
	a.sales_value as `订单金额`
from
	(
	select
		sdt,dc_code,dc_name,dc_city_name,customer_no,customer_name,child_customer_no,child_customer_name,concat("'",order_no) as order_no,sum(sales_value) as sales_value
	from
		csx_dw.dws_sale_r_d_customer_sale
	where
		sdt between '20200726' and '20200825'
		and item_channel_code in ('1','9') --客户渠道：大客户+业务代理
		and attribute_code <>5 --客户属性：剔除合伙人
		and (order_mode <>1 or dc_province_name = '四川省') --配送类型剔除直送，但要保留四川直送
	group by
		sdt,dc_code,dc_name,dc_city_name,customer_no,customer_name,child_customer_no,child_customer_name,order_no
	) as a
	left join
		(
		select
			warehouse_code,customer_code,parent_customer_code,base_kilometre
		from
			(
			select
				warehouse_code,customer_code,parent_customer_code,base_kilometre,
				row_number() over(partition by warehouse_code,customer_code,parent_customer_code order by update_time desc) as rn
			from
				csx_ods.source_tms_w_a_customer
			) as b
		where
			b.rn=1
		) as c on c.warehouse_code=a.dc_code and c.customer_code=a.child_customer_no
		
		

--B端客户订单每日汇总		
select
	a.sdt as `发货日期`,
	a.dc_code as `物流DC编码`,
	a.dc_name as `物流DC名称`,
	a.dc_city_name as `物流DC所在城市`,
	a.customer_no as `客户编号`,
	a.customer_name as `客户名称`,
	a.child_customer_no as `子客户编号`,
	a.child_customer_name as `子客户名称`,
	c.base_kilometre as `公里数`,
	a.order_cnt as `订单数`,
	a.sales_value as `订单金额`
from
	(
	select
	    sdt,
		dc_code,
		dc_name,
		dc_city_name,
		customer_no,
		customer_name,
		child_customer_no,
		child_customer_name,
		sum(sales_value) as sales_value,
		count(distinct order_no) as order_cnt
	from
		csx_dw.dws_sale_r_d_customer_sale
	where
		sdt between '20200726' and '20200825'
		and item_channel_code in ('1','9') --客户渠道：大客户+业务代理
		and attribute_code <>5 --客户属性：剔除合伙人
		and (order_mode <>1 or dc_province_name = '四川省') --配送类型剔除直送，但要保留四川直送
	group by
	    sdt,dc_code,dc_name,dc_city_name,customer_no,customer_name,child_customer_no,child_customer_name
	) as a
	--公里数	
	left join
		(
		select
			warehouse_code,customer_code,parent_customer_code,base_kilometre
		from
			(
			select
				warehouse_code,customer_code,parent_customer_code,base_kilometre,
				row_number() over(partition by warehouse_code,customer_code,parent_customer_code order by update_time desc) as rn
			from
				csx_ods.source_tms_w_a_customer
			) as b
		where
			b.rn=1
		) as c on c.warehouse_code=a.dc_code and c.customer_code=a.child_customer_no
