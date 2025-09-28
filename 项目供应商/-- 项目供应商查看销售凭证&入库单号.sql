-- 项目供应商查看销售凭证&入库单号
select a.credential_no,
  wms_order_no,
  link_wms_move_type_code,
  order_code,
  a.performance_region_code,
  a.performance_region_name,
  a.performance_province_code,
  a.performance_province_name,
  a.performance_city_code,
  a.performance_city_name,
  a.business_type_name,
  a.inventory_dc_code,
  a.inventory_dc_name,
  a.sign_company_code,
  a.sign_company_name,
  a.customer_code,
  a.customer_name,
  c.supplier_code,
  c.supplier_name,
  coalesce(b.link_wms_move_type_code, '') as link_wms_move_type_code,
  coalesce(b.link_wms_move_type_name, '') as link_wms_move_type_name,
  round((b.sale_qty),2) as sale_qty,
  round((b.sale_qty * a.sale_price),2) as sale_amt,
  round((b.sale_cost),2) as sale_cost,
  round((b.sale_qty * a.sale_price),2) - round( (b.sale_cost),2) as profit,
  a.sdt
from
(
  select
    sdt,
    split(id, '&')[0] as credential_no,
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    business_type_name,
    inventory_dc_code,
    inventory_dc_name,
    sign_company_code,
    sign_company_name,
    customer_code,
    customer_name,
    goods_code,
    sale_qty,
    sale_cost,
    sale_price
  from csx_dws.csx_dws_sale_detail_di 
  where sdt >= regexp_replace(add_months(trunc(date_sub('${current_date}', 1), 'MM'), -1), '-', '') 
	  and channel_code in ('1', '7', '9')
    and business_type_code in( '4') 
    and order_channel_detail_code<>'25'
)a
--批次操作明细表
left join
(
  select
    credential_no,
    wms_order_no, -- wms入库订单号
    goods_code,
    sum(if(in_or_out = 0, -1 * qty, qty)) as sale_qty,
    sum(if(in_or_out = 0, -1 * amt, amt)) as sale_cost,
    link_wms_move_type_code,
    link_wms_move_type_name
  from csx_dws.csx_dws_wms_batch_detail_di
  where sdt >= regexp_replace(add_months(trunc(date_sub('${current_date}', 1), 'MM'), -3), '-', '')
  group by credential_no, wms_order_no, goods_code, link_wms_move_type_code, link_wms_move_type_name
)b on b.credential_no = a.credential_no and b.goods_code = a.goods_code
--入库明细
left join
(
  select distinct
    supplier_code,
    supplier_name,
    order_code,
    goods_code
  from csx_dws.csx_dws_wms_entry_detail_di
  where sdt >= regexp_replace(add_months('${current_date}', -9), '-', '')
)c on c.order_code = b.wms_order_no and b.goods_code = c.goods_code
where a.customer_code='229493' and supplier_code in ('20057116','20061829')
-- group by a.performance_region_code, a.performance_region_name, a.performance_province_code, a.performance_province_name, a.performance_city_code, a.performance_city_name,
--   a.business_type_name, a.inventory_dc_code, a.inventory_dc_name, a.sign_company_code, a.sign_company_name, a.customer_code, a.customer_name, c.supplier_code,
--   c.supplier_name, coalesce(b.link_wms_move_type_code, ''), coalesce(b.link_wms_move_type_name, ''), a.sdt;