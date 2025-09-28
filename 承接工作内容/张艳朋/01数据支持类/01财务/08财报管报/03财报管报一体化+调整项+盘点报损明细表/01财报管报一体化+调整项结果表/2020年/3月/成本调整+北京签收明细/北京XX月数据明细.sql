
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
 
--data_sync_sale_order
--csx_ods.source_sync_r_d_data_sync_sale_order  wangwei
--19990101 分区 

--csx_ods.source_cas_r_d_accounting_credential_item   --csx_dw.dwd_cas_r_d_accounting_credential_detail
--csx_ods.source_sync_r_d_data_sync_sale_order_item  --data_sync_sale_order_item kaimin

-- 北京2月明细
drop table b2b_tmp.cust_sign_detail_1;
create temporary table b2b_tmp.cust_sign_detail_1
as
select
 b.province_code,b.province_name,d.channel,e.sales_belong_flag,c.sale_cust_code,c.sale_cust_name,
 if(move_type in ('107A','108B'),item.qty,-item.qty) qty,
 if(move_type in ( '107A', '108B' ), item.qty*orders.price,-item.qty*orders.price)amt,
 round(if(move_type in ( '107A', '108B' ), item.qty*orders.price,-item.qty*orders.price)/(1+orders.tax/100),2)amt_no_tax,
 --if(move_type in ( '107A', '108B' ), cast(item.qty as decimal(10,2))*cast(orders.price as decimal(10,2)),-cast(item.qty as decimal(10,2))*cast(orders.price as decimal(10,2)))amt,
 --if(move_type in ('107A','108B'),item.qty,-item.qty) qty,if(move_type in ('107A','108B'),item.amt,-item.amt) amt,
 item.credential_no,item.move_type,item.direction,item.bar_code bar_code1,item.product_code product_code1,item.product_name product_name1,
 item.purchase_group_code purchase_group_code1,item.purchase_group_name purchase_group_name1,item.unit unit1,item.qty qty1,item.price price1,item.location_code,item.shipper_code,
 item.shipper_name,--item.customer_code,
 item.category_code,item.tax_code tax_code1,item.tax_rate,item.amt amt1,item.amt_no_tax amt_no_tax1,item.tax_amt,
 item.adjust_amt,item.next_adjust_amt,item.batch_no,item.reservoir_area_code,item.reservoir_area_name,item.link_wms_order_no,
 item.wms_order_no,item.company_code,item.company_name,item.wms_batch_no,item.move_name,item.source_order_no,item.link_wms_batch_no,
 item.purchase_org_name,item.purchase_org_code,item.wms_order_time,item.source_order_type,item.in_out_type,--item.customer_name,
 item.category_name,item.small_category_code small_category_code1,item.small_category_name,item.wms_order_type,item.valuation_category_name,
 item.posting_time,item.price_no_tax,
 orders.order_no,orders.product_code,orders.product_name,orders.bar_code,orders.spec,orders.pieces_num,orders.unit,
 orders.approve_qty,orders.root_category_code,orders.small_category_code,orders.purchase_group_code,orders.purchase_group_name,
 orders.price,orders.total_price,orders.total_price_tax,orders.tax_code,orders.tax,orders.purchase_price,orders.csx_price,
 orders.cost,orders.status,orders.send_qty,orders.sign_qty,orders.refund_reason
from
 (select * from csx_ods.source_cas_r_d_accounting_credential_item
 where sdt='19990101'
 and move_type in( '107A','108A','107B','108B') 
 and posting_time >= '2020-03-01 00:00:00' 
 and posting_time < '2020-04-01 00:00:00' 
) item  
left join (select * from csx_ods.source_sync_r_d_data_sync_sale_order_item 
where sdt='20200408') orders 
 on  orders.order_no = item.source_order_no and item.product_code = orders.product_code
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_code,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=item.location_code
left join 
(select if(sale_cust_code like '9%' or sale_cust_code like 'W%',concat('S',substr(sale_cust_code,1,4)),sale_cust_code)sale_cust_code,
sale_cust_name,order_no
from csx_ods.source_sync_r_d_data_sync_sale_order
where sdt='19990101' and is_delete<>'1') c on c.order_no=item.source_order_no
left join 
(
  select
    customer_no, customer_name, attribute, channel, sales_id, sales_name, work_no, first_supervisor_name,
    second_supervisor_name, third_supervisor_name, fourth_supervisor_name, sales_province, sales_city,
    first_category, second_category, third_category, supervisor_id
  from
  (
    select
      customer_no, customer_name, attribute, channel, sales_id, sales_name, work_no, first_supervisor_name,
      second_supervisor_name, third_supervisor_name, fourth_supervisor_name, sales_province, sales_city,
      first_category, second_category, third_category, first_supervisor_code as supervisor_id,
      row_number()over(partition by customer_no order by sales_province desc) ranks
    from csx_dw.dws_crm_w_a_customer_m
    where sdt >= '20200101' and customer_no <> '' and customer_no is not null
  )a where ranks = 1 
)d on c.sale_cust_code=d.customer_no
left join 
(select if(shop_id like 'E%',concat('9',substr(shop_id,2,3)),shop_id)shop_id,shop_name,sales_belong_flag
from csx_dw.shop_m where sdt = 'current') e on concat('S',e.shop_id)=c.sale_cust_code
where  b.province_name='北京市'
and orders.is_delete<>'1';



insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t' 
select a.*,if(c.workshop_code is null,'不是工厂商品','是工厂商品') as is_factory_goods_name,fh_sdt
from b2b_tmp.cust_sign_detail_1 a
left join
(select 
sdt as fh_sdt,
order_no,goods_code
from csx_dw.dws_sale_r_d_customer_sale
where sdt>='20191101'
)b on b.order_no=a.source_order_no and b.goods_code=a.product_code1
left join
  (select
      workshop_code, province_code, goods_code
    from csx_dw.dws_mms_w_a_factory_setting_craft_once_all
    where sdt='current' and new_or_old=1
  )c on a.province_code=c.province_code and a.product_code1=c.goods_code;



--北京2月总金额
select sum(qty) qty,sum(amt) amt,
--if(move_type in ('107A','108B'),qty,-qty) qty1,if(move_type in ('107A','108B'),amt,-amt) amt1
from csx_ods.source_cas_r_d_accounting_credential_item
 where sdt='19990101'
 and move_type in( '107A','108A','107B','108B') 
 and posting_time >= '2020-03-01 00:00:00' 
 and posting_time < '2020-04-01 00:00:00'
and location_code in('W0A3','W0G7','W0H2');

--清洗表订单号与商品不能确认唯一数据
--OM200222000778_285
select order_no,product_code,* 
from csx_ods.source_sync_r_d_data_sync_sale_order_item
where sdt>='20200201'
and order_no='OM200222000778'
and product_code='285';

