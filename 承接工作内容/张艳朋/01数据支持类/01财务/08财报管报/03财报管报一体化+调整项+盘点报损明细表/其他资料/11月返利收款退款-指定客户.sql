select
  a.rebate_no,
  e.company_code,
  d.company_code,
  a.original_order_no,
  a.rebate_no,
  a.sdt,
  a.sap_cus_code, 
  a.sap_cus_name, 
  b.channel,
  b.sales_province_code,
  b.sales_province,
  b.sales_city,
  a.agreement_dc_code,
  a.inventory_dc_code,
  b.attribute,
  b.first_category,
  b.second_category,
  b.third_category,
  b.sales_name,
  b.work_no,
  a.product_code,
  c.goods_name,
  c.division_code,
  c.division_name,
  c.department_id,
  c.department_name,
  a.total_price as sales_value,
  a.total_price_no_tax as excluding_tax_sales,
  0 as sales_cost,
  0 as excluding_tax_cost,
  a.total_price as profit,
  a.total_price_no_tax as excluding_tax_profit
from
(
  select * from csx_dw.dwd_csms_r_d_yszx_customer_rebate_detail_new
  where sdt >= '20201101' and sdt < '20201201' and type in (0, 1) and status = 1
) a left join
(
  select * from csx_dw.dws_crm_w_a_customer_m_v1
  where sdt = 'current' and source = 'crm'
) b on a.sap_cus_code = b.customer_no
left join
(
  select *
  from csx_dw.dws_basic_w_a_csx_product_m
  where sdt = 'current'
) c on a.product_code = c.goods_id
left join
(
  select
    shop_id, company_code
  from csx_dw.dws_basic_w_a_csx_shop_m
  where sdt = 'current'
) d on a.agreement_dc_code = d.shop_id
left join
(
  select
    shop_id, company_code, province_code
  from csx_dw.dws_basic_w_a_csx_shop_m
  where sdt = 'current'
) e on a.inventory_dc_code = e.shop_id
left outer join
(
  select *
  from csx_dw.dws_mms_w_a_factory_setting_craft_once_all
  where sdt = 'current' and new_or_old = 1
)f on e.province_code = f.province_code and a.product_code = f.goods_code;
