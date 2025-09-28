create table csx_analyse_tmp.csx_analyse_tmp_sale_detail_supplier
as 
select 
	a.order_code,a.order_date,a.delivery_date,a.delivery_type_name,a.business_type_name,a.customer_code,a.customer_name,a.sub_customer_code,a.sub_customer_name,a.goods_code,a.goods_name,
	a.sale_qty,a.unit_name,a.sale_price,a.sale_amt,c.supplier_code,c.supplier_name
from 
	(
    select 
		split(id,'&')[0] as credential_no,
		-- split_part(id, '&',1) as credential_no,
		business_type_name,customer_code,customer_name,sub_customer_code,sub_customer_name,order_code,regexp_replace(to_date(order_time),'-','') as order_date,
		regexp_replace(to_date(delivery_time),'-','') as delivery_date,delivery_type_name,sale_price,unit_name,
		goods_code,goods_name,sale_amt,profit,sale_qty
    from 
		-- csx_dw.dws_sale_r_d_detail 
		csx_dws.csx_dws_sale_detail_di
    where 
		sdt>='20211001' and sdt<='20221221'
		-- and to_date(delivery_time) between '2022-12-01' and '2022-12-21'
		-- and channel_code in ('1', '7', '9')
		-- and business_type_code ='4'
		-- and customer_code in ('115352','117103','119695','118411','120700','125128','127804','120176','116569','121934','124553','127372','130328','130257','116655')
	)a 
	--批次操作明细表
	left join 
		(
		select
			credential_no,wms_order_no,goods_code
		from 
			-- csx_dw.dws_wms_r_d_batch_detail
			csx_dws.csx_dws_wms_batch_detail_di
		where 
			sdt >= '20211001'
		group by 
			credential_no,wms_order_no,goods_code
		)b on b.credential_no = a.credential_no and b.goods_code = a.goods_code
	--入库明细
	left join 
		(
		select 
			supplier_code,supplier_name,order_code,goods_code
		from 
			-- csx_dw.dws_wms_r_d_entry_detail
			csx_dws.csx_dws_wms_entry_detail_di
		where 
			sdt >= '20211001' 
			-- or sdt = '19990101'
		group by 
			supplier_code,supplier_name,order_code,goods_code
		)c on c.order_code = b.wms_order_no and b.goods_code = c.goods_code
where
	c.supplier_code='20044273'
;


-- 12月订单明细		
select 
	a.order_code,a.order_date,a.delivery_date,a.delivery_type_name,a.business_type_name,a.customer_code,a.customer_name,a.sub_customer_code,a.sub_customer_name,a.goods_code,a.goods_name,
	sum(a.sale_qty) as sale_qty,a.unit_name,a.sale_price,sum(a.sale_amt) as sale_amt
from 
	csx_analyse_tmp.csx_analyse_tmp_sale_detail_supplier a 
where
	a.supplier_code='20044273'
	and delivery_date between '20221201' and '20221221'
group by 
	a.order_code,a.order_date,a.delivery_date,a.delivery_type_name,a.business_type_name,a.customer_code,a.customer_name,a.sub_customer_code,a.sub_customer_name,a.goods_code,a.goods_name,a.unit_name,a.sale_price
;

-- 单量
select
	a.customer_code,a.customer_name,a.sub_customer_code,a.sub_customer_name,
	substr(delivery_date,1,6) as smonth,
	count(distinct a.order_code) as order_cnt,
	sum(a.sale_amt) as sale_amt,
	count(a.goods_code) as goods_cnt
from 
	csx_analyse_tmp.csx_analyse_tmp_sale_detail_supplier a 
where
	a.supplier_code='20044273'
	and delivery_date between '20220101' and '20221221'
group by 
	a.customer_code,a.customer_name,a.sub_customer_code,a.sub_customer_name,
	substr(delivery_date,1,6)
