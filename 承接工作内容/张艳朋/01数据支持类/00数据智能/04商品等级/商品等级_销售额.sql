select
	t1.dc_province_name,
	t1.goods_code,
	t1.goods_name,
	t1.sales_value,
	t1.rn,
	coalesce(t2.five_day_remark_cnt,0) as five_day_remark_flag,
	if(t3.goods_code is not null,'是','否') as five_day_sales_flag
from
	(
	select
		dc_province_name,
		goods_code,
		goods_name,
		sales_value,
		rn
	from
		(
		select
			dc_province_name,
			goods_code,
			goods_name,
			sales_value,
			row_number() over(partition by dc_province_name order by sales_value desc) as rn
		from
			(
			select
				a.dc_province_name,
				a.goods_code,
				a.goods_name,
				sum(a.sales_value) as sales_value
			from
				(
				select
					dc_province_name,
					order_no,
					goods_code,
					goods_name,
					sum(sales_value) as sales_value
				from 
					csx_dw.dws_sale_r_d_customer_sale
				where 
					sdt >= '20200901' 
					and sdt <= '20200915'
					and channel in ('1','7','9') 
					and is_self_sale = 1
				group by 
					dc_province_name,order_no,goods_code,goods_name
				) a
				left join
					(
					select
						order_no, 
						refund_no, 
						goods_code, 
						spec_remarks, 
						buyer_remarks
					from 
						csx_dw.dwd_csms_r_d_yszx_order_detail_new
					where 
						(sdt >= '20200901' OR sdt = '19990101')
						and (spec_remarks <> '' OR buyer_remarks <> '') 
						and (item_status is null or item_status <> 0)
					) b on a.order_no = coalesce(b.refund_no, b.order_no) and a.goods_code = b.goods_code
				left join
					(
					select
						goods_id,
						division_code
					from 
						csx_dw.dws_basic_w_a_csx_product_m
					where 
						sdt = 'current'
					) c on a.goods_code = c.goods_id
			where 
				c.division_code in ('10','11') --部类编号
				--and b.order_no is not null
			group by
				a.dc_province_name,
				a.goods_code,
				a.goods_name
			) t1
		) t2 
	where
		rn<=20
	) t1
	left join
		(
		select
			a.dc_province_name,
			a.goods_code,
			a.goods_name,
			count(a.goods_code) as five_day_remark_cnt
		from
			(
			select
				dc_province_name,
				order_no,
				goods_code,
				goods_name
			from 
				csx_dw.dws_sale_r_d_customer_sale
			where 
				sdt >= '20200925' 
				and sdt <= '20200929'
				and channel in ('1','7','9') 
				and is_self_sale = 1
			group by 
				dc_province_name,order_no,goods_code,goods_name
			) a
			left join
				(
				select
					order_no, 
					refund_no, 
					goods_code, 
					spec_remarks, 
					buyer_remarks
				from 
					csx_dw.dwd_csms_r_d_yszx_order_detail_new
				where 
					(sdt >= '20200901' OR sdt = '19990101')
					and (spec_remarks <> '' OR buyer_remarks <> '') 
					and (item_status is null or item_status <> 0)
				) b on a.order_no = coalesce(b.refund_no, b.order_no) and a.goods_code = b.goods_code
			left join
				(
				select
					goods_id,
					division_code
				from 
					csx_dw.dws_basic_w_a_csx_product_m
				where 
					sdt = 'current'
				) c on a.goods_code = c.goods_id
		where
			c.division_code in ('10','11')
			and b.order_no is not null
		group by
			a.dc_province_name,
			a.goods_code,
			a.goods_name
		) t2 on t2.dc_province_name=t1.dc_province_name and t2.goods_code=t1.goods_code
	left join
		(
		select
			a.dc_province_name,
			a.goods_code,
			a.goods_name
		from
			(
			select
				dc_province_name,
				order_no,
				goods_code,
				goods_name
			from 
				csx_dw.dws_sale_r_d_customer_sale
			where 
				sdt >= '20200925' 
				and sdt <= '20200929'
				and channel in ('1','7','9') 
				and is_self_sale = 1
			group by 
				dc_province_name,order_no,goods_code,goods_name
			) a
			left join
				(
				select
					order_no, 
					refund_no, 
					goods_code, 
					spec_remarks, 
					buyer_remarks
				from 
					csx_dw.dwd_csms_r_d_yszx_order_detail_new
				where 
					(sdt >= '20200901' OR sdt = '19990101')
					and (spec_remarks <> '' OR buyer_remarks <> '') 
					and (item_status is null or item_status <> 0)
				) b on a.order_no = coalesce(b.refund_no, b.order_no) and a.goods_code = b.goods_code
			left join
				(
				select
					goods_id,
					division_code
				from 
					csx_dw.dws_basic_w_a_csx_product_m
				where 
					sdt = 'current'
				) c on a.goods_code = c.goods_id
		where
			c.division_code in ('10','11')
			--and b.order_no is not null
		group by
			a.dc_province_name,
			a.goods_code,
			a.goods_name
		) t3 on t3.dc_province_name=t1.dc_province_name and t3.goods_code=t1.goods_code