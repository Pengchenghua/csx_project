-- 成本端发货维度数据关系调整单 - 按原单凭证号和商品汇总
drop table b2b_tmp.tmp_data_relation_cas_sale_adjustment_summary;
create temporary table b2b_tmp.tmp_data_relation_cas_sale_adjustment_summary 
as 
select 
  concat_ws(',',collect_set(cast(id as string))) as id,
  concat_ws(',',collect_set(adjustment_no)) as adjustment_no,
  adjustment_reason,
  adjustment_type,
  item_credential_no,
  item_source_order_no,
  product_code,
  product_name,
  company_code,
  sum(qty) as qty,
  sum(adjustment_amt) as adjustment_amt,
  sum(adjustment_amt_no_tax) as adjustment_amt_no_tax,
  item_wms_biz_type,
  reservoir_area_code
from csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment
where sdt = '19990101'
group by adjustment_reason, adjustment_type, item_credential_no, item_source_order_no, product_code, product_name, company_code,
  item_wms_biz_type, reservoir_area_code;


-- 凭证表 唯一
select count(concat(credential_no,product_code)),count(distinct concat(credential_no,product_code))
from csx_dw.dwd_sync_r_d_data_relation_cas_sale_detail
where create_time>='2020-02-01'
and create_time<'2020-03-01';

--成本端发货维度数据关系调整单 - 按原单凭证号和商品汇总
select count(concat(credential_no,product_code)),count(distinct concat(credential_no,product_code))
from csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment
where posting_time >= '2020-02-01 00:00:00' 
and  posting_time < '2020-03-01 00:00:00';

--成本核算 凭证商品可能有多次调整  sdt=create_time
select count(concat(credential_no,product_code)),count(distinct concat(credential_no,product_code))
from csx_dw.dwd_cas_r_d_accounting_credential_detail
where sdt>='20200201'
and sdt<'20200301';

------------------------------------------------------------------------------------------



--1 2 3 4 5 7
select 
--对抵负库存的成本调整
case when adjustment_reason='in_remark' then adjustment_amt_no_tax end adj_cost_fkc,
--采购退货金额差异的成本调整
case when adjustment_reason='out_remark' then adjustment_amt_no_tax end adj_cost_cgth,
--工厂月末分摊-调整销售订单
case when (adjustment_reason='fac_remark_sale' and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73') )
		or (adjustment_reason='fac_remark' and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73') )
		then adjustment_amt_no_tax end adj_cost_gc_xs,
--工厂月末分摊-调整跨公司调拨订单
case when (adjustment_reason='fac_remark_sale' and item_wms_biz_type in('06','07','08','09','15','17') )
		or (adjustment_reason='fac_remark' and item_wms_biz_type in('06','07','08','09','15','17') )
		then adjustment_amt_no_tax end adj_cost_gc_db,
--工厂月末分摊-调整其他单据或无原单
case when adjustment_reason='fac_remark_sale' and item_wms_biz_type not in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','06','07','08','09','15','17')
		then adjustment_amt_no_tax end adj_cost_gc_qt,
--手工调整销售成本
case when adjustment_reason='manual_remark' then adjustment_amt_no_tax end adj_cost_sg
from csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment
where posting_time >= '2020-02-01 00:00:00' 
and  posting_time < '2020-03-01 00:00:00';



select 
--对抵负库存的成本调整
sum(case when adjustment_reason='in_remark' then adjustment_amt_no_tax end) adj_cost_fkc,
--采购退货金额差异的成本调整
sum(case when adjustment_reason='out_remark' then adjustment_amt_no_tax end) adj_cost_cgth,
--工厂月末分摊-调整销售订单
sum(case when (adjustment_reason='fac_remark_sale' and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73') )
		or (adjustment_reason='fac_remark' and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73') )
		then adjustment_amt_no_tax end) adj_cost_gc_xs,
--工厂月末分摊-调整跨公司调拨订单
sum(case when (adjustment_reason='fac_remark_sale' and item_wms_biz_type in('06','07','08','09','15','17') )
		or (adjustment_reason='fac_remark' and item_wms_biz_type in('06','07','08','09','15','17') )
		then adjustment_amt_no_tax end) adj_cost_gc_db,
--工厂月末分摊-调整其他
sum(case when adjustment_reason='fac_remark_sale' and item_wms_biz_type not in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','06','07','08','09','15','17')
		then adjustment_amt_no_tax end) adj_cost_gc_qt,
--手工调整销售成本
sum(case when adjustment_reason='manual_remark' then adjustment_amt_no_tax end) adj_cost_sg
from csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment
where posting_time >= '2020-02-01 00:00:00' 
and  posting_time < '2020-03-01 00:00:00';



--6 价量差工厂未使用的商品 
select sum(amount) from csx_ods.source_mms_r_a_factory_report_no_share_product where period='2020-02';


csx_b2b_factory.factory_report_no_share_product---------------------csx_ods.source_mms_r_a_factory_report_no_share_product
csx_b2b_accounting.accounting_credential_item---------------------csx_ods.source_cas_r_d_accounting_credential_item
csx_b2b_accounting.accounting_credential_header---------------------csx_ods.source_cas_r_d_accounting_credential_header
csx_b2b_wms.wms_reservoir_area---------------------csx_ods.wms_reservoir_area_ods
settle_settle_bill---------------------csx_ods.settle_settle_bill_ods
accounting_credential_item---------------------csx_dw.dwd_cas_r_d_accounting_credential_detail


-- 8 报损	
select
sum(item.amt_no_tax)
from csx_dw.dwd_cas_r_d_accounting_credential_detail item
left join csx_ods.wms_reservoir_area_ods area on area.warehouse_code = item.location_code 
and area.reservoir_area_code = item.reservoir_area_code 
where item.sdt >= '20200201'
and item.sdt < '20200301'
and item.move_type in( '117A','117B')
and concat(item.wms_biz_type,area.reservoir_area_attribute) in ('35C','36C','37C','38C','38Y','39C','40C','41C','64C','64Y','66C','66Y','76C','76Y','77C','78C');

-- 9 10 盘盈 盘亏 
select
sum(case when move_type in ( '115A', '116B' ) then -amt_no_tax end ) inventory_p, --盘盈  取移动类型为115A或116B，且库区是PD01的金额
sum(case when move_type in ( '115B', '116A' ) then amt_no_tax end ) inventory_l --盘亏
from csx_dw.dwd_cas_r_d_accounting_credential_detail
where sdt >= '20200201'
and sdt < '20200301'
and reservoir_area_code = 'PD01';


select
location_code,wms_biz_type,reservoir_area_code,move_type,
sum(amt_no_tax)amt_no_tax,sum(amt)amt
from csx_dw.dwd_cas_r_d_accounting_credential_detail
where sdt >= '20200201'
and sdt < '20200301'
group by location_code,wms_biz_type,reservoir_area_code,move_type;




-- 11  采购后台收入	net_value，value_tax_total
select cost_name,sum( value_tax_total ) 
from csx_ods.settle_settle_bill_ods 
where attribution_date >= '2020-02-01' 
and attribution_date < '2020-03-01'
and sdt='19990101'
group by cost_name;


-- 8 报损	底表XXX
--select
--sum(item.amt_no_tax)
--from csx_ods.source_cas_r_d_accounting_credential_item item
--left join csx_ods.source_cas_r_d_accounting_credential_header header on item.credential_no = header.credential_no
--left join csx_ods.wms_reservoir_area_ods area on area.warehouse_code = item.location_code 
--and area.reservoir_area_code = item.reservoir_area_code 
--where item.posting_time >= '2020-02-01 00:00:00' 
--and item.posting_time < '2020-03-01 00:00:00'
--and item.move_type in( '117A','117B')
--and sdt='19990101'
--and concat(header.wms_biz_type,area.reservoir_area_attribute) in ('35C','36C','37C','38C','38Y','39C','40C','41C','64C','64Y','66C','66Y','76C','76Y','77C','78C');

select sum(amt_no_tax) 
from data_sync_broken_item 
where posting_time >= '2020-02-01 00:00:00' and posting_time < '2020-03-01 00:00:00' and ( wms_biz_type, reservoir_area_prop ) IN (
 ( '35', 'C' ),
 ( '36', 'C' ),
 ( '37', 'C' ),
 ( '38', 'C' ),
 ( '38', 'Y' ),
 ( '39', 'C' ),
 ( '40', 'C' ),
 ( '41', 'C' ),
 ( '64', 'C' ),
 ( '64', 'Y' ),
 ( '66', 'C' ),
 ( '66', 'Y' ),
 ( '76', 'C' ),
 ( '76', 'Y' ),
 ( '77', 'C' ),
 ( '78', 'C' ) 
 ) 
 
-- 9 10 盘盈 盘亏 底表XXX
--select
--sum(case when reservoir_area_code = 'PD01' and move_type in ( '115A', '116B' ) then if(direction = '+', -1 * amt_no_tax, amt_no_tax) end ) inventory_p, --盘盈  取移动类型为115A或116B，且库区是PD01的金额
--sum(case when reservoir_area_code = 'PD01' and move_type in ( '115B', '116A' ) then if(direction = '+', -1 * amt_no_tax, amt_no_tax) end ) inventory_l --盘亏
--from csx_ods.source_cas_r_d_accounting_credential_item
--where posting_time >= '2020-02-01 00:00:00' 
--and posting_time < '2020-03-01 00:00:00';	




