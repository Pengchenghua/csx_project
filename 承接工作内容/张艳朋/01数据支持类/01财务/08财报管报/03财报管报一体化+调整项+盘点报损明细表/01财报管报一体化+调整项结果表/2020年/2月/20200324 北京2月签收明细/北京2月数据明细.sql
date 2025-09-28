
--开发提供
SELECT
 item.*,orders.*,if(move_type in ('107A','108B'),item.qty,-item.qty) ,if(move_type in ('107A','108B'),item.amt,-item.amt) 
FROM
 csx_b2b_accounting.accounting_credential_item item  
 left join data_sync_sale_order_item orders on  orders.order_no = item.source_order_no and item.product_code = orders.product_code
WHERE
 item.move_type in( '107A','108A','107B','108B') 
 AND item.posting_time >= '2020-03-22 00:00:00' 
 AND item.posting_time < '2020-03-23 00:00:00' limit 1000;
 
 

--csx_ods.source_cas_r_d_accounting_credential_item   --csx_dw.dwd_cas_r_d_accounting_credential_detail
--csx_ods.source_sync_r_d_data_sync_sale_order_item  --data_sync_sale_order_item

-- 北京2月明细
insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t' 
select
 b.province_name,
 if(move_type in ('107A','108B'),item.qty,-item.qty) qty,if(move_type in ('107A','108B'),item.amt,-item.amt) amt,
 item.credential_no,item.move_type,item.direction,item.receive_type,item.bar_code,item.product_code,item.product_name,
 item.purchase_group_code,item.purchase_group_name,item.unit,item.qty,item.price,item.location_code,item.shipper_code,
 item.shipper_name,item.customer_code,item.category_code,item.tax_code,item.tax_rate,item.amt,item.amt_no_tax,item.tax_amt,
 item.adjust_amt,item.next_adjust_amt,item.batch_no,item.reservoir_area_code,item.reservoir_area_name,item.link_wms_order_no,
 item.wms_order_no,item.company_code,item.company_name,item.wms_batch_no,item.move_name,item.source_order_no,item.link_wms_batch_no,
 item.purchase_org_name,item.purchase_org_code,item.wms_order_time,item.source_order_type,item.in_out_type,item.customer_name,
 item.category_name,item.small_category_code,item.small_category_name,item.wms_order_type,item.valuation_category_name,
 item.posting_time,item.price_no_tax,
 orders.order_no,orders.product_code,orders.product_name,orders.bar_code,orders.spec,orders.pieces_num,orders.unit,
 orders.approve_qty,orders.root_category_code,orders.small_category_code,orders.purchase_group_code,orders.purchase_group_name,
 orders.price,orders.total_price,orders.total_price_tax,orders.tax_code,orders.tax,orders.purchase_price,orders.csx_price,
 orders.cost,orders.status,orders.send_qty,orders.sign_qty,orders.refund_reason
from
 (select * from csx_ods.source_cas_r_d_accounting_credential_item
 where sdt='19990101'
 and move_type in( '107A','108A','107B','108B') 
 and posting_time >= '2020-02-01 00:00:00' 
 and posting_time < '2020-03-01 00:00:00' 
) item  
left join csx_ods.source_sync_r_d_data_sync_sale_order_item orders 
 on  orders.order_no = item.source_order_no and item.product_code = orders.product_code
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=item.location_code
where province_name='北京市'
and is_delete<>'1'
;

--北京2月总金额
select sum(qty) qty,sum(amt) amt,
--if(move_type in ('107A','108B'),qty,-qty) qty1,if(move_type in ('107A','108B'),amt,-amt) amt1
from csx_ods.source_cas_r_d_accounting_credential_item
 where sdt='19990101'
 and move_type in( '107A','108A','107B','108B') 
 and posting_time >= '2020-02-01 00:00:00' 
 and posting_time < '2020-03-01 00:00:00'
and location_code in('W0A3','W0G7','W0H2');

--清洗表订单号与商品不能确认唯一数据
--OM200222000778_285
select order_no,product_code,* 
from csx_ods.source_sync_r_d_data_sync_sale_order_item
where sdt>='20200201'
and order_no='OM200222000778'
and product_code='285';