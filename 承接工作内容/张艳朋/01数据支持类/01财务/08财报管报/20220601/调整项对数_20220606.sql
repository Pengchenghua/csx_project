--==============================================================================================================================================================================

select
	--对抵负库存的成本调整
	sum(case when adjustment_remark='in_remark' then adjustment_value_no_tax end) adj_ddfkc_no,
	--采购退货金额差异的成本调整
	sum(case when adjustment_remark='out_remark' then adjustment_value_no_tax end) adj_cgth_no,
	--工厂月末分摊-调整销售订单
	sum(case when (adjustment_remark in('fac_remark_sale','fac_remark_span') 
				and adjustment_type='sale'
				and wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82') )
			then adjustment_value_no_tax end) adj_gc_xs_no,		
	--工厂月末分摊-调整跨公司调拨订单
	sum(case when (adjustment_remark in('fac_remark_sale','fac_remark_span') 
				and adjustment_type='sale'
				and wms_biz_type in('06','07','08','09','12','15','17') )
			then adjustment_value_no_tax end) adj_gc_db_no,		
	--工厂月末分摊-调整其他
	sum(case when adjustment_remark in('fac_remark_sale','fac_remark_span')		
			and adjustment_type='sale'
			and wms_biz_type not in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82','06','07','08','09','12','15','17')
			then adjustment_value_no_tax end) adj_gc_qt_no,	
	--手工调整销售成本
	sum(case when adjustment_remark='manual_remark' then if(adjustment_type='stock',-1*adjustment_value_no_tax,adjustment_value_no_tax) end) adj_sg_no,
	--采购入库价格补救-调整销售
	sum(case when adjustment_remark = 'pur_remark_remedy' 
			and adjustment_type='sale'
			and wms_biz_type in ('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82')
			then adjustment_value_no_tax end) adj_bj_xs_no,				
	--采购入库价格补救-调整跨公司调拨	
	sum(case when adjustment_remark = 'pur_remark_remedy'
			and adjustment_type='sale'
			and wms_biz_type in ('06','07','08','09','12','15','17')
			then adjustment_value_no_tax end) adj_bj_db_no,				
	--采购入库价格补救-调整其他
	sum(case when adjustment_remark = 'pur_remark_remedy' 
			and adjustment_type='sale'
			and wms_biz_type not in ('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82','06','07','08','09','12','15','17')
			then adjustment_value_no_tax end) adj_bj_qt_no
from 
	csx_dw.dws_cas_r_d_account_adjustment_detail
where 
	sdt>='20220501' and sdt<='20220531'
;


select 
	sum(amount)amount -- 价量差
from 
	csx_ods.source_mms_r_a_factory_report_no_share_product
where 
	sdt='20220606'
	and period in('2022-05')  --'2020-05'	
;

select 
	sum(d_cost_subtotal) d_cost_subtotal -- 工厂分摊后成本小于0
from 
	csx_ods.source_mms_r_a_factory_report_diff_apportion_header
where 
	sdt='20220606'
	and period in('2022-05')  --'2020-05'	
	and notice_status = '3'	
;

	
select
	sum(amt_no_tax) -- 报损
from
	(
	select
		location_code,location_name,company_code,company_name,goods_code product_code,goods_name product_name,unit,price_no_tax,
		credential_no,posting_time,purchase_group_code,purchase_group_name,move_type,reservoir_area_code,wms_biz_type_code,
		wms_order_no,wms_biz_type_code wms_biz_type,wms_biz_type_name,cost_center_code,cost_center_name,
		if(move_type in ('117B','118B'),-1*qty,qty) qty,
		if(move_type in ('117B','118B'),-1*amt_no_tax,amt_no_tax) amt_no_tax,
		if(move_type in ('117B','118B'),-1*amt,amt) amt
	from 
		csx_dw.dws_cas_r_d_account_credential_detail
	where 
		sdt>='20220501' and sdt<='20220531'
		and wms_biz_type_code in (35, 36, 37, 38, 39, 40, 41, 64, 66, 76, 77, 78)
	)a
	left join
		(
		select 
			* 
		from 
			csx_ods.source_wms_w_a_wms_reservoir_area
		) b on a.location_code=b.warehouse_code and a.reservoir_area_code=b.reservoir_area_code
where 
	(reservoir_area_attribute='C' or reservoir_area_attribute='Y')
	and (( a.wms_biz_type_code <>'64' and b.reservoir_area_attribute = 'C' and ( a.purchase_group_code like 'H%' or a.purchase_group_code like 'U%' ) ) 
	or a.wms_biz_type_code = '64' )
;

select 
	sum(net_value) -- 后台收入
from 
	csx_dw.dwd_gss_r_d_settle_bill 
where
	sdt>='20220501' and sdt<='20220531'
;

-- 旧表
--select 	
--	sum(sales_value/(1+tax_rate/100)) amt_no_tax -- 销售后台支出-调价+返利
--from 
--	csx_dw.dwd_csms_r_d_rebate_order
--where 
--	order_type_code in ('0','1')
--	and commit_time>='2022-04-01'
--	and commit_time<='2022-04-30 23:59:59'
--	and order_status='1'
--;

select 
	sum(sales_value/(1+tax_rate/100)) amt_no_tax
from
	(
	select
		'Z68' as adjust_reason,rebate_no as no_type,dc_code,dc_name,customer_no,customer_name,goods_code,goods_name,sales_value,tax_rate
	from
		csx_dw.dwd_sss_r_d_customer_rebate_detail --客户返利单明细表
	where
		sdt>='20220317' and sdt<=regexp_replace(date_sub(current_date, 1), '-', '') 
	union all
	select
		'Z69' as adjust_reason,adjust_price_no as no_type,dc_code,dc_name,customer_no,customer_name,goods_code,goods_name,sales_value,tax_rate
	from
		csx_dw.dwd_sss_r_d_customer_adjust_price_detail --客户调价单明细表
	where
		sdt>='20220317' and sdt<=regexp_replace(date_sub(current_date, 1), '-', '') 
	union all
	select
		adjust_reason_code as adjust_reason,rebate_no as no_type,dc_code,dc_name,customer_no,customer_name,goods_code,goods_name,sales_value,tax_rate
	from
		csx_dw.dwd_csms_r_d_rebate_order_detail	
	where
		sdt>='20220101' and sdt<'20220317'					
	) a 
	join(select distinct order_no from csx_dw.dws_sale_r_d_detail where sdt>='20220501' and sdt<='20220531') b on b.order_no=a.no_type
;