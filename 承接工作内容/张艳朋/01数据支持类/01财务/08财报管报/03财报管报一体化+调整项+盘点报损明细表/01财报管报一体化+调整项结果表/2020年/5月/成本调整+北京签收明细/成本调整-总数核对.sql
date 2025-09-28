

--1 2 3 4 5 7
select 
--对抵负库存的成本调整
case when adjustment_reason='in_remark' then adjustment_amt_no_tax end adj_cost_fkc,
--采购退货金额差异的成本调整
case when adjustment_reason='out_remark' then adjustment_amt_no_tax end adj_cost_cgth,
--工厂月末分摊-调整销售订单
case when (adjustment_reason in('fac_remark_sale','fac_remark') 
			and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73',
									'A18','A20','A21','A22','A23','A24','A25','A55') )
		then adjustment_amt_no_tax end adj_cost_gc_xs,
--工厂月末分摊-调整跨公司调拨订单
case when ((adjustment_reason in('fac_remark_sale','fac_remark') and item_wms_biz_type in('06','07','08','09','15','17','A06','A07','A08','A09','A15') )
				or (adjustment_reason = 'fac_remark_sale' AND item_wms_biz_type='12'))
		then adjustment_amt_no_tax end adj_cost_gc_db,
--工厂月末分摊-调整其他
case when adjustment_reason='fac_remark_sale' 
		and item_wms_biz_type not in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73',
									'06','07','08','09','15','17','A18','A20','A21','A22','A23','A24','A25','A55','12')
		then adjustment_amt_no_tax end adj_cost_gc_qt,
--手工调整销售成本
case when adjustment_reason='manual_remark' then if(adjustment_type='stock',-1*adjustment_amt_no_tax,adjustment_amt_no_tax) end adj_cost_sg
from csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment
where sdt = '19990101'
and posting_time >= '2020-05-01 00:00:00' 
and  posting_time < '2020-06-01 00:00:00';



select 
--对抵负库存的成本调整
sum(case when adjustment_reason='in_remark' then adjustment_amt_no_tax end) adj_cost_fkc,
--采购退货金额差异的成本调整
sum(case when adjustment_reason='out_remark' then adjustment_amt_no_tax end) adj_cost_cgth,
--工厂月末分摊-调整销售订单
sum(case when (adjustment_reason in('fac_remark_sale','fac_remark') 
			and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73',
									'A18','A20','A21','A22','A23','A24','A25','A55') )
		then adjustment_amt_no_tax end) adj_cost_gc_xs,
--工厂月末分摊-调整跨公司调拨订单
sum(case when ((adjustment_reason in('fac_remark_sale','fac_remark') and item_wms_biz_type in('06','07','08','09','15','17','A06','A07','A08','A09','A15') )
				or (adjustment_reason = 'fac_remark_sale' AND item_wms_biz_type='12'))
		then adjustment_amt_no_tax end) adj_cost_gc_db,
--工厂月末分摊-调整其他
sum(case when adjustment_reason='fac_remark_sale' 
			and item_wms_biz_type not in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','A18','A20','A21','A22','A23','A24','A25','A55',
										'06','07','08','09','15','17','A06','A07','A08','A09','A15','12')
		then adjustment_amt_no_tax end) adj_cost_gc_qt,
--手工调整销售成本
sum(case when adjustment_reason='manual_remark' then if(adjustment_type='stock',-1*adjustment_amt_no_tax,adjustment_amt_no_tax) end) adj_cost_sg
from csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment
where sdt = '19990101'
and posting_time >= '2020-05-01 00:00:00' 
and  posting_time < '2020-06-01 00:00:00';



--6 价量差工厂未使用的商品 
select sum(amount) 
from csx_ods.source_mms_r_a_factory_report_no_share_product 
where sdt='20200606' 
and period='2020-05';

----------------------------------------------------------------------------------------------------------------------------------------------------------
csx_b2b_accounting.accounting_credential_item---------------------csx_dw.dwd_cas_r_d_accounting_credential_detail
csx_b2b_wms.wms_reservoir_area---------------------csx_ods.source_wms_w_a_wms_reservoir_area  

csx_b2b_accounting.accounting_credential_item---------------------csx_ods.source_cas_r_d_accounting_credential_item  csx_dw.dwd_cas_r_d_accounting_credential_detail (xiaomin)
csx_b2b_accounting.accounting_credential_header---------------------csx_ods.source_cas_r_d_accounting_credential_header  csx_dw.dwd_cas_r_d_accounting_credential_detail (xiaomin)
csx_b2b_settle.settle_settle_bill---------------------csx_ods.settle_settle_bill_ods   (xiaomin)

csx_b2b_factory.factory_report_no_share_product---------------------csx_ods.source_mms_r_a_factory_report_no_share_product  (xiaolong)
csx_b2b_factory.factory_report_diff_apportion_header---------------------csx_ods.source_mms_r_a_factory_report_diff_apportion_header  (xiaolong)

data_relation_cas_sale_adjustment---------------------csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment  (wangwei)
data_sync_broken_item---------------------csx_ods.source_sync_r_d_data_sync_broken_item  报损  (wangwei)
data_sync_inventory_item---------------------csx_ods.source_sync_r_d_data_sync_inventory_item  盘点  (wangwei)
data_sync.data_relation_cas_sale_receiving_credential---------------------csx_ods.source_sync_r_d_data_relation_cas_sale_receiving_credential(wangwei) M端签收


--8 报损	
select
sum(a.amt_no_tax) amt_no_tax,
sum(amt) amt
from
csx_ods.source_sync_r_d_data_sync_broken_item a
where a.sdt = '19990101'
and (( a.wms_biz_type <>'64' and a.reservoir_area_prop = 'C' and ( a.purchase_group_code like 'H%' or a.purchase_group_code like 'U%' ) ) 
or a.wms_biz_type = '64' )
and a.posting_time >= '2020-05-01 00:00:00' 
and a.posting_time < '2020-06-01 00:00:00';


--9 10 盘盈 盘亏 
select
sum(a.amt_no_tax) amt_no_tax,
sum(amt) amt
from
csx_ods.source_sync_r_d_data_sync_inventory_item a
where a.sdt = '19990101'
and a.reservoir_area_code = 'PD01' 
and ( a.purchase_group_code like 'H%' or a.purchase_group_code like 'U%' ) 
and a.posting_time >= '2020-05-01 00:00:00' 
and a.posting_time < '2020-06-01 00:00:00';


--11  采购后台收入	net_value，value_tax_total
select cost_name,sum( net_value ) 
from csx_ods.settle_settle_bill_ods 
where sdt='19990101'
and attribution_date >= '2020-05-01' 
and attribution_date < '2020-06-01'
group by cost_name;


--工厂分摊后成本小于0，未分摊金额
select
sum( d_cost_subtotal )
from csx_ods.source_mms_r_a_factory_report_diff_apportion_header
where sdt='20200606'
and period = '2020-05' 
and notice_status = '3';

--销售后台支出-调价、返利  Z68是返利Z69是调价 --hive中返利表
select adjust_reason,
sum(total_price/(1+tax_rate/100)) 
from csx_dw.dwd_csms_r_d_yszx_customer_rebate_detail_new 
where type in ('0','1')
and commit_time>='2020-05-01 00:00:00'
and commit_time<'2020-06-01 00:00:00'
group by adjust_reason;





