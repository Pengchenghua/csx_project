-- 销售收入		419612542.37
SELECT 
sum( sale_amt_no_tax )
FROM data_relation_cas_sale_receiving_credential 
WHERE posting_time >= '2020-11-01 00:00:00' 
AND posting_time < '2020-12-01 00:00:00';

-- 销售后台支出-调价+返利 	-1412207.049503
SELECT sum(CASE WHEN a.type='1' then -1*b.total_price/(1+c.tax_rate/100) else b.total_price/(1+c.tax_rate/100) end) FROM b2b_mall_prod.yszx_customer_rebate a 
LEFT JOIN b2b_mall_prod.yszx_customer_rebate_item b on a.rebate_no=b.rebate_no
LEFT JOIN  csx_basic_data.md_product_info c on b.product_code=c.product_code
LEFT JOIN csx_basic_data.md_shop_info d on a.inventory_dc_code=d.location_code
WHERE a.type in ('0','1')
and a.commit_time>='2020-11-01 00:00:00'
and a.commit_time<'2020-12-01 00:00:00'
and a.`status`='1';

-- 定价成本	397340498.10
SELECT 
sum( cost_amt_no_tax )
FROM data_relation_cas_sale_receiving_credential 
WHERE posting_time >= '2020-11-01 00:00:00' 
AND posting_time < '2020-12-01 00:00:00'; 

--  对抵负库存的成本调整	-72789.91
 SELECT
sum(adjustment_amt_no_tax) 
FROM
data_relation_cas_sale_adjustment 
WHERE
adjustment_reason = 'in_remark' 
AND posting_time >= '2020-11-01 00:00:00' 
AND posting_time < '2020-12-01 00:00:00';

--  采购退货金额差异的成本调整	88734.10
SELECT
sum(adjustment_amt_no_tax) 
FROM
data_relation_cas_sale_adjustment 
WHERE
adjustment_reason = 'out_remark' 
AND posting_time >= '2020-11-01 00:00:00' 
AND posting_time < '2020-12-01 00:00:00';

-- 采购入库价格补救-调整销售	-7059.07
SELECT
	sum( adjustment_amt_no_tax ) 
FROM
	data_relation_cas_sale_adjustment 
WHERE
	adjustment_reason = 'pur_remark_remedy' 
	AND adjustment_type='sale'
	AND item_wms_biz_type IN ('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82')
AND posting_time >= '2020-11-01 00:00:00' 
AND posting_time < '2020-12-01 00:00:00';

-- 采购入库价格补救-调整跨公司调拨	0
SELECT
	sum( adjustment_amt_no_tax ) 
FROM
	data_relation_cas_sale_adjustment 
WHERE
	adjustment_reason = 'pur_remark_remedy' 
	AND adjustment_type='sale'
	AND item_wms_biz_type IN ('06','07','08','09','12','15','17') 
AND posting_time >= '2020-11-01 00:00:00' 
AND posting_time < '2020-12-01 00:00:00';

-- 采购入库价格补救-调整其他	-28.31
SELECT
	sum( adjustment_amt_no_tax ) 
FROM
	data_relation_cas_sale_adjustment 
WHERE
	adjustment_reason = 'pur_remark_remedy' 
	AND adjustment_type='sale'
	AND item_wms_biz_type NOT IN ('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82','06','07','08','09','12','15','17')
AND posting_time >= '2020-11-01 00:00:00' 
AND posting_time < '2020-12-01 00:00:00';

--  工厂月末分摊-调整销售订单	3082954.88 3180853.21
SELECT
sum(adjustment_amt_no_tax)
FROM
	 data_relation_cas_sale_adjustment
WHERE
	(adjustment_reason = 'fac_remark_sale' or adjustment_reason = 'fac_remark_span')
AND adjustment_type='sale'
	AND item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82')
AND posting_time >= '2020-11-01 00:00:00' 
AND posting_time < '2020-12-01 00:00:00';

--  工厂月末分摊-调整跨公司调拨订单	519.55
SELECT
sum( adjustment_amt_no_tax ) 
FROM
data_relation_cas_sale_adjustment 
WHERE
(adjustment_reason = 'fac_remark_sale' or adjustment_reason = 'fac_remark_span')
AND adjustment_type='sale'
AND item_wms_biz_type in('06','07','08','09','12','15','17')
AND posting_time >= '2020-11-01 00:00:00' 
AND posting_time < '2020-12-01 00:00:00';

--  工厂月末分摊-调整其他单据或无原单	170652.13  170881.49
SELECT
sum(adjustment_amt_no_tax)
FROM
data_sync.data_relation_cas_sale_adjustment
WHERE
(adjustment_reason = 'fac_remark_sale' or adjustment_reason = 'fac_remark_span')
AND adjustment_type='sale'
AND item_wms_biz_type not in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82','06','07','08','09','12','15','17')
AND posting_time >= '2020-11-01 00:00:00' 
AND posting_time < '2020-12-01 00:00:00';

--  价量差工厂未使用的商品	203360.50000
SELECT sum(amount) FROM csx_b2b_factory.factory_report_no_share_product WHERE period='2020-11';
 
-- 工厂分摊后成本小于0，未分摊金额  -129923.56000
SELECT
sum( d_cost_subtotal )
FROM
csx_b2b_factory.factory_report_diff_apportion_header 
WHERE
period = '2020-11' 
AND notice_status = '3'; 

-- 工厂无法分摊-手工调整金额
-- 见excel

--  手工调整销售成本	0
SELECT
sum(case when adjustment_type='stock' then -1*adjustment_amt_no_tax else adjustment_amt_no_tax end) 
FROM
data_relation_cas_sale_adjustment 
WHERE
adjustment_reason = 'manual_remark' 
AND posting_time >= '2020-11-01 00:00:00' 
AND posting_time < '2020-12-01 00:00:00';

--  报损	938775.95
SELECT
sum(amt_no_tax) AS '不含税金额'
FROM
data_sync_broken_item 
WHERE
(( wms_biz_type <>'64' AND reservoir_area_prop = 'C' AND ( purchase_group_code LIKE 'H%' OR purchase_group_code LIKE 'U%' ) ) 
OR wms_biz_type = '64' )
AND posting_time >= '2020-11-01 00:00:00' 
AND posting_time < '2020-12-01 00:00:00';

--  盘点（生鲜）（负数代表盘盈，销售成本减少）-82703.96	-69631.61
SELECT
sum(amt_no_tax) AS '不含税金额',
sum(amt) AS '含税金额' 
FROM
data_sync_inventory_item
WHERE
reservoir_area_code = 'PD01' 
AND ( purchase_group_code LIKE 'H%' OR purchase_group_code LIKE 'U%' ) 
AND posting_time >= '2020-11-01 00:00:00' 
AND posting_time < '2020-12-01 00:00:00';

-- 盘点（食百）
--  采购后台收入	2767073.22
SELECT
sum( net_value ) 
FROM
csx_b2b_settle.settle_settle_bill 
WHERE
attribution_date >= '2020-11-01' 
AND attribution_date < '2020-12-01';
