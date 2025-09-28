-- 销售收入		518994147.94
select 
	sum(sale_amt_no_tax)
from 
	data_relation_cas_sale_receiving_credential 
where 
	posting_time >= '2021-08-01 00:00:00' 
	and posting_time < '2021-09-01 00:00:00';

-- 销售后台支出-调价+返利 	-337239.481222
select 
	sum(case when a.type='1' then -1*b.total_price/(1+c.tax_rate/100) else b.total_price/(1+c.tax_rate/100) end) 
from 
	b2b_mall_prod.yszx_customer_rebate a 
	left join b2b_mall_prod.yszx_customer_rebate_item b on a.rebate_no=b.rebate_no
	left join csx_basic_data.md_product_info c on b.product_code=c.product_code
	left join csx_basic_data.md_shop_info d on a.inventory_dc_code=d.location_code
where 
	a.type in ('0','1')
	and a.commit_time>='2021-08-01 00:00:00'
	and a.commit_time<'2021-09-01 00:00:00'
	and a.`status`='1';

-- 定价成本	491028537.10
select 
	sum( cost_amt_no_tax )
from 
	data_relation_cas_sale_receiving_credential 
where 
	posting_time >= '2021-08-01 00:00:00' 
	and posting_time < '2021-09-01 00:00:00'; 

--  对抵负库存的成本调整	1061.83
select
	sum(adjustment_amt_no_tax) 
from
	data_sync.data_relation_cas_sale_adjustment 
where
	adjustment_reason = 'in_remark' 
	and posting_time >= '2021-08-01 00:00:00' 
	and posting_time < '2021-09-01 00:00:00';

--  采购退货金额差异的成本调整	39658.69
select
	sum(adjustment_amt_no_tax) 
from
	data_sync.data_relation_cas_sale_adjustment 
where
	adjustment_reason = 'out_remark' 
	and posting_time >= '2021-08-01 00:00:00' 
	and posting_time < '2021-09-01 00:00:00';

-- 采购入库价格补救-调整销售	24043.80
select
	sum(adjustment_amt_no_tax) 
from
	data_sync.data_relation_cas_sale_adjustment 
where
	adjustment_reason = 'pur_remark_remedy' 
	and adjustment_type='sale'
	and item_wms_biz_type in ('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82')
	and posting_time >= '2021-08-01 00:00:00' 
	and posting_time < '2021-09-01 00:00:00';

-- 采购入库价格补救-调整跨公司调拨	-88.50
select
	sum( adjustment_amt_no_tax ) 
from
	data_sync.data_relation_cas_sale_adjustment 
where
	adjustment_reason = 'pur_remark_remedy' 
	and adjustment_type='sale'
	and item_wms_biz_type in ('06','07','08','09','12','15','17') 
	and posting_time >= '2021-08-01 00:00:00' 
	and posting_time < '2021-09-01 00:00:00';

-- 采购入库价格补救-调整其他	0
select
	sum( adjustment_amt_no_tax ) 
from
	data_sync.data_relation_cas_sale_adjustment 
where
	adjustment_reason = 'pur_remark_remedy' 
	and adjustment_type='sale'
	and item_wms_biz_type not in ('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82','06','07','08','09','12','15','17')
	and posting_time >= '2021-08-01 00:00:00' 
	and posting_time < '2021-09-01 00:00:00';

--  工厂月末分摊-调整销售订单	3434192.19
SELECT
sum(adjustment_amt_no_tax)
FROM
	 data_sync.data_relation_cas_sale_adjustment
WHERE
	(adjustment_reason = 'fac_remark_sale' or adjustment_reason = 'fac_remark_span')
AND adjustment_type='sale'
	AND item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82')
AND posting_time >= '2021-08-01 00:00:00' 
AND posting_time < '2021-09-01 00:00:00';

--  工厂月末分摊-调整跨公司调拨订单	85569.71
SELECT
	sum( adjustment_amt_no_tax ) 
FROM
	data_sync.data_relation_cas_sale_adjustment 
WHERE
	(adjustment_reason = 'fac_remark_sale' or adjustment_reason = 'fac_remark_span')
	AND adjustment_type='sale'
	AND item_wms_biz_type in('06','07','08','09','12','15','17')
	AND posting_time >= '2021-08-01 00:00:00' 
	AND posting_time < '2021-09-01 00:00:00';

--  工厂月末分摊-调整其他单据或无原单	-235089.96
SELECT
	sum(adjustment_amt_no_tax)
FROM
	data_sync.data_relation_cas_sale_adjustment
WHERE
	(adjustment_reason = 'fac_remark_sale' or adjustment_reason = 'fac_remark_span')
	AND adjustment_type='sale'
	AND item_wms_biz_type not in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82','06','07','08','09','12','15','17')
	AND posting_time >= '2021-08-01 00:00:00' 
	AND posting_time < '2021-09-01 00:00:00';

--  价量差工厂未使用的商品	351524.24000
SELECT 
	sum(amount) 
FROM 
	csx_b2b_factory.factory_report_no_share_product 
WHERE period='2021-08';
 
-- 工厂分摊后成本小于0，未分摊金额  -87186.91000
SELECT
	sum(d_cost_subtotal)
FROM
	csx_b2b_factory.factory_report_diff_apportion_header 
WHERE
	period = '2021-08' 
	AND notice_status = '3'; 

-- 工厂无法分摊-手工调整金额
-- 见excel

--  手工调整销售成本	0
SELECT
	sum(case when adjustment_type='stock' then -1*adjustment_amt_no_tax else adjustment_amt_no_tax end) 
FROM
	data_sync.data_relation_cas_sale_adjustment 
WHERE
	adjustment_reason = 'manual_remark' 
	AND posting_time >= '2021-08-01 00:00:00' 
	AND posting_time < '2021-09-01 00:00:00';

--  报损	212857.49
SELECT
	sum(amt_no_tax) AS '不含税金额'
FROM
	data_sync.data_sync_broken_item 
WHERE
	(( wms_biz_type <>'64' AND reservoir_area_prop = 'C' AND ( purchase_group_code LIKE 'H%' OR purchase_group_code LIKE 'U%' ) ) 
	OR wms_biz_type = '64' )
	AND posting_time >= '2021-08-01 00:00:00' 
	AND posting_time < '2021-09-01 00:00:00';

--  盘点（生鲜）（负数代表盘盈，销售成本减少）
SELECT
	sum(amt_no_tax) AS '不含税金额',
	sum(amt) AS '含税金额' 
FROM
	data_sync.data_sync_inventory_item
WHERE
	reservoir_area_code = 'PD01' 
	AND ( purchase_group_code LIKE 'H%' OR purchase_group_code LIKE 'U%' ) 
	AND posting_time >= '2021-08-01 00:00:00' 
	AND posting_time < '2021-09-01 00:00:00';

-- 盘点（食百）
--  采购后台收入	3579681.05
SELECT
	sum(net_value) 
FROM
	csx_b2b_settle.settle_settle_bill 
WHERE
	attribution_date >= '2021-08-01' 
	AND attribution_date < '2021-09-01';
