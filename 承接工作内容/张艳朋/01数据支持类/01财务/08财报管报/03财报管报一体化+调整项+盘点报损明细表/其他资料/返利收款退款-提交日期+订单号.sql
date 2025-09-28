-- 返利 收款和退款
insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t'
select
  --a.status,
  a.rebate_no,  --返利单号
  e.company_code,
  d.company_code,
  if(g.code is null, round(0.01*sum(a.total_price), 2), '') as channel_value,
  if(g.code is null, round(0.99*sum(a.total_price), 2), '') as caiwu_sales_value,
  if(g.code is null, round(0.01*sum(a.total_price_no_tax), 2), '') as excluding_tax_channel_value,
  if(g.code is null, round(0.99*sum(a.total_price_no_tax), 2), '') as excluding_tax_caiwu_sales_value,
  regexp_replace(substr(a.commit_time,1,10),'-','') sdt,
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
  c.division_code,
  c.division_name,
  c.department_id,
  c.department_name,
  sum(a.total_price) as sales_value,
  sum(a.total_price_no_tax) as excluding_tax_sales,
  0.0 as sales_cost,
  0.0 as excluding_tax_cost,
  0.0 as profit,
  0.0 as excluding_tax_profit,
  if(f.workshop_code is null, '不是工厂商品', '是工厂商品') as is_factory_goods_name
from
(
  select *
  from csx_dw.dwd_csms_r_d_yszx_customer_rebate_detail_new
  where commit_time >= '2020-12-01' and commit_time < '2021-01-01' and type in (0, 1) and status='1'
) a left join
(
  select * from csx_dw.dws_crm_w_a_customer_m_v1
  where sdt = regexp_replace(date_sub(current_date, 1), '-', '') and source = 'crm'
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
)f on e.province_code = f.province_code and a.product_code = f.goods_code
left join
(
  SELECT code FROM csx_ods.source_basic_w_a_md_company_code
  WHERE sdt = regexp_replace(date_sub(current_date, 1), '-', '')
) g on d.company_code = g.code
group by a.rebate_no,e.company_code, d.company_code,regexp_replace(substr(a.commit_time,1,10),'-',''), a.sap_cus_code, a.sap_cus_name, b.channel, b.sales_province_code,
  b.sales_province, b.sales_city, a.agreement_dc_code, a.inventory_dc_code, b.attribute, b.first_category,
  b.second_category, b.third_category, b.sales_name, b.work_no, c.division_code, c.division_name, c.department_id,
  c.department_name, if(f.workshop_code is null, '不是工厂商品', '是工厂商品'), g.code;
