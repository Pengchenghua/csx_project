-- 东北订单明细周期进价测算毛利率

with 
tmp_order_detail as 
(select order_code,
case when order_status= 'CREATED' then '待支付'
    when order_status='PAID' then '待确认'
    when order_status='CONFIRMED' then '待接单'
    when order_status='CUTTED' then '待发货'
    when order_status='STOCKOUT' then '配送中'
    end order_status,       -- CREATED:待支付 PAID:待确认 CONFIRMED:待截单 CUTTED:待出库 STOCKOUT:配送中 SITE:服务站签收 FETCHED:已自提 HOME:买家已签收 R_APPLY:退货申请 R_PERMIT:退货中 R_BACK:退货回库
order_date,order_time,
inventory_dc_code,
inventory_dc_name,
customer_code,
customer_name,
sub_customer_code,
sub_customer_name,
recep_order_time,
recep_order_by,
require_delivery_date,
require_delivery_time,
require_delivery_time_end,
order_remarks,
buyer_remarks,
is_cycle_order,
additional_order_flag,
price_confirm_flag,
requirement_order_flag,
goods_code,
goods_name,
purchase_unit_name,
unit_name,
supplier_code,
supplier_name,
purchase_qty,
send_qty,
origin_price,
sale_price,
sale_price_explain,
discount_rate,
goods_remarks,
promotion_cost_price 

from
  csx_dwd.csx_dwd_csms_yszx_order_detail_di 
where sdt>='20241201'
-- and order_status ='CUTTED'
and inventory_dc_code   in ('WD31','WC09','WB86')
and  order_status in ('CUTTED','PAID') 
) ,
tmp_scm_purchase_cost as 
(select
  b.calday,
  a.*
from
  (
    select
      product_code,
      supplier_code,
      location_code,
      purchase_price,
      cast(cycle_end_time as string ) cycle_end_time,
      cast(cycle_start_time as string ) cycle_start_time,
      cycle_price_status
    from
        csx_ods.csx_ods_csx_b2b_scm_scm_product_purchase_cycle_price_df
    where
      sdt = '20241205'
      and location_code in ('WD31', 'WC09', 'WB86')
      and cycle_price_status=0
  ) a
  inner join (
    select
      calday
    from
      csx_dim.csx_dim_basic_date
    where
      calday >= '20241101'
      and calday <= '20250101'
  ) b on a.cycle_end_time  >= b.calday
  and a.cycle_start_time <= b.calday 
  ),
tmp_product_plan_supplier as
(select
  location_code,
  product_code,
  supplier_code,
  supplier_name,
  plan_purchase_master_flag
 from  csx_dwd.csx_dwd_scm_product_source_supplier_di  
where sdt>='20220101' 
    -- and  location_code='WC09' 
    and status=1 
    and plan_purchase_master_flag=1 
),
tmp_product_daily_supplier as
(select
  location_code,
  product_code,
  supplier_code,
  supplier_name,
  daily_purchase_master_flag
from  csx_dwd.csx_dwd_scm_product_source_supplier_di  
where sdt>='20220101' 
    -- and  location_code='WC09' 
    and status=1 
    and daily_purchase_master_flag=1 
)
-- 先关联计划主供应商若无再关联日采供应商
select  order_code,
order_status,
order_date,order_time,
inventory_dc_code,
inventory_dc_name,
customer_code,
customer_name,
sub_customer_code,
sub_customer_name,
recep_order_time,
recep_order_by,
require_delivery_date,
require_delivery_time,
require_delivery_time_end,
order_remarks,
buyer_remarks,
is_cycle_order,
additional_order_flag,
price_confirm_flag,
requirement_order_flag,
a.goods_code,
c.goods_name,
c.classify_large_name,
c.classify_middle_name,
c.classify_small_name	,
purchase_unit_name,
unit_name,
a.supplier_code,
a.supplier_name,
purchase_qty,
send_qty,
-- origin_price,
sale_price,
sale_price_explain,
-- discount_rate,
goods_remarks,
-- promotion_cost_price,
purchase_price,
purchase_qty*coalesce(purchase_price,0) as purchase_cost,
sale_price*coalesce(purchase_qty,0) as sale_amount,
(sale_price*purchase_qty - purchase_qty*coalesce(purchase_price,0) )as profit_amount,
(sale_price*purchase_qty - purchase_qty*coalesce(purchase_price,0) )/(sale_price*coalesce(purchase_qty,0) ) as profit_rate,
cycle_start_time,
cycle_end_time
-- cycle_price_status
from 
(select  order_code,
order_status,
order_date,order_time,
inventory_dc_code,
inventory_dc_name,
customer_code,
customer_name,
sub_customer_code,
sub_customer_name,
recep_order_time,
recep_order_by,
require_delivery_date,
require_delivery_time,
require_delivery_time_end,
order_remarks,
buyer_remarks,
is_cycle_order,
additional_order_flag,
price_confirm_flag,
requirement_order_flag,
a.goods_code,
purchase_unit_name,
unit_name,
coalesce(b.supplier_code,c.supplier_code) supplier_code,
coalesce(b.supplier_name,c.supplier_name) supplier_name,
purchase_qty,
send_qty,
origin_price,
sale_price,
sale_price_explain,
discount_rate,
goods_remarks,
promotion_cost_price 
from tmp_order_detail a 
left join tmp_product_plan_supplier b on a.goods_code=b.product_code and a.inventory_dc_code = b.location_code
left join tmp_product_daily_supplier c on a.goods_code=c.product_code and a.inventory_dc_code = c.location_code
)a 
left join tmp_scm_purchase_cost d on a.inventory_dc_code=d.location_code and a.goods_code=d.product_code and a.supplier_code=d.supplier_code and a.require_delivery_date=d.calday
left join 
(select goods_code,
    goods_name,
    classify_large_name,	
    classify_middle_name,
    classify_small_name	 
from csx_dim.csx_dim_basic_goods 
where sdt='current') c on a.goods_code=c.goods_code

;


-- CREATED:待支付 PAID:待确认 CONFIRMED:待截单 CUTTED:待出库 STOCKOUT:配送中 SITE:服务站签收 FETCHED:已自提 HOME:买家已签收 R_APPLY:退货申请 R_PERMIT:退货中 R_BACK:退货回库
-- R_PAY:退款中 R_SUCCESS:退货成功 R_REJECT:退货关闭（拒绝退货） SUCCESS:已完成 CANCELLED:已取消', 