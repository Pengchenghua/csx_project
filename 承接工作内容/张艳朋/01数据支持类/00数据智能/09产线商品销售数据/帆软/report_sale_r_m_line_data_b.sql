-- ===================================================================================================================
-- B端
select
	a.region_name,a.province_name,a.city_group_name,a.sdt,a.channel_name,a.business_type_name,a.perform_dc_code,a.perform_dc_name,a.customer_no,a.customer_name,a.sign_date,
	a.attribute_name,coalesce(b.line_code,'') as line_code,coalesce(b.line_name,'') as line_name,a.first_category_name,a.second_category_name,a.third_category_name,a.work_no,a.sales_name,a.department_code,a.department_name,
	a.classify_large_name,a.classify_middle_name,a.goods_code,a.goods_name,a.unit,
	sum(a.sales_qty) as sales_qty,
	sum(sales_value) as sales_value,
	sum(profit)as profit,
	sum(profit)/abs(sum(sales_value)) as profit_rate,
	sum(a.excluding_tax_sales) as excluding_tax_sales,
	sum(a.excluding_tax_profit) as excluding_tax_profit,
	sum(a.excluding_tax_profit)/abs(sum(a.excluding_tax_sales)) as excluding_tax_profit_rate
from 
	(
	select
		region_name,dc_province_name,city_group_name,dc_code,dc_name,customer_no,customer_name,origin_order_no,order_no,-- split(id,'&')[0] as credential_no,
		SPLIT_PART(id,'&',1) as credential_no,
		goods_code,goods_name,sales_value,profit,channel_name,province_code,province_name,is_factory_goods,order_category_name,sdt,business_type_name,perform_dc_code,
		perform_dc_name,regexp_replace(to_date(sign_time),'-','') as sign_date,attribute_name,first_category_name,second_category_name,
		third_category_name,work_no,sales_name,department_code,department_name,classify_large_name,classify_middle_name,unit,sales_qty,excluding_tax_sales,excluding_tax_profit
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>=regexp_replace(to_date(trunc(now(), 'MONTH')),'-','') -- 本月第一天
		and sdt<=regexp_replace(to_date(date_sub(now(),1)),'-','') -- 昨天
		and channel_code in('1','7','9')
		and region_name='华西大区'
	)a
	left join 
		(
		select 
			b.goods_code,
			b.credential_no,
			--d.location_code,
			--d.location_name,
			d.line_code,
			d.line_name
		from
			(
			select
				goods_code,     -- '商品编码',
				out_order,     -- '出库顺序',
				batch_no,     -- '成本批次号',
				credential_no,     -- '操作明细的凭证号',
				credential_item_id,     -- '凭证明细id',
				move_type,    -- '移动类型'	
				source_order_no
			from
				(
				select 
					goods_code,     -- '商品编码',
					out_order,     -- '出库顺序',
					batch_no,     -- '成本批次号',
					credential_no,     -- '操作明细的凭证号',
					credential_item_id,     -- '凭证明细id',
					move_type,     -- '移动类型'
					source_order_no, -- 对应凭证的来源单号
					row_number() over(partition by goods_code,credential_no order by out_order desc) as rn
				from 
					csx_dw.dws_wms_r_d_batch_detail
				where 
					sdt>=regexp_replace(to_date(add_months(now(), -6)),'-','') -- 往前推6个月
					and move_type in ('107A', '108A')
				)tmp1
			where
				rn=1
			)b	
			left join 
				(	
				select
					order_code,     --工单编号
					--location_code,
					--location_name,
					line_code,
					line_name,
					goods_code    --商品编码
				from  
					csx_dw.dws_mms_r_a_factory_order 
				where 
					sdt>=regexp_replace(to_date(add_months(now(), -6)),'-','') -- 往前推6个月
					and mrp_prop_key in('3061','3010')		--加工厂-主原料
				group by 
					order_code,line_code,line_name,goods_code
				) d on d.order_code=b.source_order_no and d.goods_code=b.goods_code
		where
			d.line_code is not null
		group by 
			b.goods_code,b.credential_no,d.line_code,d.line_name
		)b on a.credential_no=b.credential_no and a.goods_code=b.goods_code
--where
--	b.line_code is not null
group by 
	a.region_name,a.province_name,a.city_group_name,a.sdt,a.channel_name,a.business_type_name,a.perform_dc_code,a.perform_dc_name,a.customer_no,a.customer_name,a.sign_date,
	a.attribute_name,coalesce(b.line_code,''),coalesce(b.line_name,''),a.first_category_name,a.second_category_name,a.third_category_name,a.work_no,a.sales_name,a.department_code,a.department_name,
	a.classify_large_name,a.classify_middle_name,a.goods_code,a.goods_name,a.unit
	
	
