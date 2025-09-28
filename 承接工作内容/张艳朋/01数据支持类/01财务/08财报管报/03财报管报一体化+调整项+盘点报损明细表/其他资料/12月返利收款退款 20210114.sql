-- 返利 收款和退款  --Z68返利;Z69调价
INSERT OVERWRITE DIRECTORY '/tmp/raoyanhua/fanli' row FORMAT DELIMITED fields TERMINATED BY '\t'
select
  a.adjust_reason,
  a.rebate_no,
  a.company_code,
  a.sign_company_code,
  coalesce(round(0.01 * sum( if(a.is_borrow_aptitudes = 0, null, a.sales_value) ), 2), '') as channel_value,
  coalesce(round(0.99 * sum( if(a.is_borrow_aptitudes = 0, null, a.sales_value) ), 2), '') as caiwu_sales_value,
  coalesce(round(0.01 * sum( if(a.is_borrow_aptitudes = 0, null, a.excluding_tax_sales) ), 2), '') as excluding_tax_channel_value,
  coalesce(round(0.99 * sum( if(a.is_borrow_aptitudes = 0, null, a.excluding_tax_sales) ), 2), '') as excluding_tax_caiwu_sales_value,
  a.sdt,
  a.customer_no, 
  a.customer_name,
  a.channel_name,
  a.province_code,
  a.province_name,
  a.city_name,
  a.prefer_dc_code,
  a.dc_code,
  a.attribute_name,
  a.first_category_name,
  a.second_category_name,
  a.third_category_name,
  b.sales_name,
  b.work_no,
  a.division_code,
  a.division_name,
  a.department_code,
  a.department_name,
  sum(a.sales_value) as sales_value,
  sum(a.excluding_tax_sales) as excluding_tax_sales,
  0 as sales_cost,
  0 as excluding_tax_cost,
  sum(a.sales_value) as profit,
  sum(a.excluding_tax_sales) as excluding_tax_profit,
  if(f.workshop_code is null, '不是工厂商品', '是工厂商品') as is_factory_goods_desc
from
(
  select * from csx_dw.dwd_csms_r_d_rebate_order
  where sdt >= '20201201' and sdt < '20210101' and order_type_code in (0, 1) and order_status = 1
) a left join
(
  select customer_no, sales_name, work_no
  from csx_dw.dws_crm_w_a_customer
  where sdt = 'current'
) b on a.customer_no = b.customer_no
left join
(
  select *
  from csx_dw.dws_mms_w_a_factory_setting_craft_once_all
  where sdt = 'current' and new_or_old = 1
)f on a.dc_province_code = f.province_code and a.goods_code = f.goods_code
group by a.adjust_reason,a.rebate_no, a.company_code, a.sign_company_code, a.sdt, a.customer_no, a.customer_name, a.channel_name,
  a.province_code, a.province_name, a.city_name, a.prefer_dc_code, a.dc_code, a.attribute_name, a.first_category_name,
  a.second_category_name, a.third_category_name, b.sales_name, b.work_no, a.division_code, a.division_name,
  a.department_code, a.department_name, if(f.workshop_code is null, '不是工厂商品', '是工厂商品');


