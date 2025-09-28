select 
	case when a.channel ='7' then 'BBC'	
		when a.channel in ('1','9') and e.attribute='合伙人客户' then '城市服务商' 
		when a.channel in ('1','9') and e.attribute='贸易客户'  then '贸易客户' 
		when a.channel in ('1','9') and a.order_kind='WELFARE' then '福利单'  
		when a.channel in ('1','9') and e.attribute not in('合伙人客户','贸易客户') and a.order_kind<>'WELFARE' then '日配单' 	 
		else '其他' end sale_group,
	a.channel,     --战报渠道编码
	a.channel_name,     --战报渠道名称
	a.province_code,     --战报省区编码
	a.province_name,     --战报省区名称
	a.city_group_code,     --战报城市组编码
	a.city_group_name,     --战报城市组名称
	a.dc_code,     --库存地点编码
	a.dc_name,     --库存地点名称
	a.customer_no,     --客户编号
	e.customer_name,     --客户名称
	e.first_category,     --客户一级分类
	e.second_category,     --客户二级分类
	e.attribute,     --客户属性
	a.origin_order_no,     --原订单号
	a.order_no,     --订单号
	a.credential_no,     --成本核算凭证号
	a.goods_code,     --商品编号
	a.is_factory_goods_name,		--是否工厂商品
	a.cost_price,     --商品进价（成本价-单价）
	a.purchase_price,     --采购价格-单价
	a.middle_office_price middle_report_price,     --中台报价-单价
	--a.sales_price,     --正常含税售价
	a.promotion_price price,     --商品促销售价-即商品销售单价
	a.sales_value,     --含税销售额
	--a.sales_cost,     --含税销售成本
	a.profit,     --含税毛利
	a.front_profit,--`前端毛利`
	--a.excluding_tax_sales,     --不含税销售额
	--a.excluding_tax_cost,     --不含税销售成本
	--a.excluding_tax_profit,     --不含税毛利
	a.sdt sales_date,
	a.sales_qty,     --销售数量		
	b.qty,     -- '操作数量',
	b.amt,     -- '操作金额',
	round(b.batch_price,6) batch_price,     -- '入库成本价',
	round(b.fact_price,6) fact_price     -- '原材料价格',
from 
	(
	select 
		dc_code,     --库存地点编码
		dc_name,     --库存地点名称
		customer_no,     --客户编号
		origin_order_no,
		order_no,     --订单号
		credential_no,     --成本核算凭证号
		goods_code,     --商品编号
		sales_qty,     --销售数量
		cost_price,     --商品进价（成本价-单价）
		purchase_price,     --采购价格-单价
		middle_office_price,     --中台报价-单价
		sales_price,     --正常含税售价
		promotion_price,     --商品促销售价-即商品销售单价
		sales_value,     --含税销售额
		sales_cost,     --含税销售成本
		profit,     --含税毛利
		front_profit,--`前端毛利`
		--excluding_tax_sales,     --不含税销售额
		--excluding_tax_cost,     --不含税销售成本
		--excluding_tax_profit,     --不含税毛利
		channel,     --战报渠道编码
		channel_name,     --战报渠道名称
		province_code,     --战报省区编码
		province_name,     --战报省区名称
		--city_code,     --战报城市编码
		--city_name,     --战报城市名称
		city_group_code,     --战报城市组编码
		city_group_name,     --战报城市组名称
		is_factory_goods_name,   --是否工厂商品
		order_kind,
		sdt	
	from 
		csx_dw.dws_sale_r_d_customer_sale
	where 
		sdt>='20201201'
		and sdt<='20201224'
		and channel in('1','7','9')
	)a
	left join 
		(
		select 
			b.product_code,
			b.credential_no,
			--d.goods_type_name,     --商品类型名称--不能加，因为部分商品有维护多种类型（组合型、分解型）
			sum(b.qty) qty,     -- '操作数量',
			sum(b.amt) amt,     -- '操作金额',
			sum(b.amt)/sum(b.qty)  batch_price,     -- '入库成本价',
			sum(d.fact_price*b.qty)/sum(b.qty) fact_price     -- '原材料价格',
		from
			(
			select 
				product_code,     -- '商品编码',
				out_order,     -- '出库顺序',
				qty,     -- '操作数量',
				amt,     -- '操作金额',
				price,     -- '操作单价',
				batch_no,     -- '成本批次号',
				credential_no,     -- '操作明细的凭证号',
				credential_item_id,     -- '凭证明细id',
				move_type     -- '移动类型'
			from 
				csx_dw.dwd_cas_r_d_accounting_stock_log_item
			where 
				sdt>='20201201'
				and (in_or_out='1' or(in_or_out='0' and move_type in('108A')))
			)b	
			left join 
				(
				select 
					substr(source_order_no,1,2) source,  --来源单号的前两位
					product_code,     -- '商品编码',
					batch_no,     -- '成本批次号',
					source_order_no,     -- '对应凭证的来源单号',
					credential_no,     -- '操作明细的凭证号',
					move_type,    -- '移动类型'
					price_no_tax *(1+tax_rate/100) batch_price    -- '含税单价'
				from 
					csx_dw.dwd_cas_r_d_accounting_batch_create_log
				where 
					sdt>='20201201'
				)c on b.batch_no=c.batch_no
			left join 
				(	
				select
					produce_date,     --生产时间
					order_code,     --工单编号
					workshop_code,     --车间编码
					workshop_name,     --车间名称
					location_code,
					location_name,
					line_code,
					line_name,
					goods_code,     --商品编码
					--goods_type_name,     --商品类型名称
					--product_code,     --物料编码
					--product_name,     --物料名称
					--product_unit,     --物料单位
					mrp_prop_value,     --物料MRP属性值
					--goods_plan_receive_qty,     --商品计划数量
					--fact_qty,     --原料数量（领料-退料）即：实际产量物料计划使用数量（reality_receive_qty/fact_qty为出品率,1-出品率为损耗率）
					sum(goods_reality_receive_qty) goods_reality_receive_qty,     --商品实际产量		
					sum(fact_values) fact_values,     --原料金额	
					sum(fact_values)/sum(goods_reality_receive_qty) as fact_price
				from  
					csx_dw.dws_mms_r_a_factory_order 
				where 
					sdt>='20201201'
					and mrp_prop_key in('3061','3010')		--加工厂-主原料
				group by 
					produce_date,order_code,workshop_code,workshop_name,location_code,location_name,line_code,line_name,goods_code,mrp_prop_value 
				)d on c.source_order_no=d.order_code and d.goods_code=c.product_code
		group by 
			b.product_code,b.credential_no
		)b on a.credential_no=b.credential_no and a.goods_code=b.product_code	
	join 
		(
		select 
			distinct customer_no,customer_name,sales_province,sales_city,first_category,second_category,attribute
		from 
			csx_dw.dws_crm_w_a_customer_m_v1 
		where 
			sdt='20201224'
		--and (attribute_code <>'5' or attribute_code is null)
		) e on e.customer_no=a.customer_no	
;