-- 附件3 B端
INSERT OVERWRITE DIRECTORY '/tmp/wangwei/aaaaa' row FORMAT DELIMITED fields TERMINATED BY '\t'
select
  a.dc_company_code,
  a.sign_company_code,
  if(b.code is null, round(0.01*sum(sales_value), 2), '') as channel_value,
  if(b.code is null, round(0.99*sum(sales_value), 2), '') as caiwu_sales_value,
  if(b.code is null, round(0.01*sum(excluding_tax_sales), 2), '') as excluding_tax_channel_value,
  if(b.code is null, round(0.99*sum(excluding_tax_sales), 2), '') as excluding_tax_caiwu_sales_value,
  a.sales_date,
  a.customer_no,
  a.customer_name,
  case when a.channel not in ('1','7','9') then ''
    when a.channel = '7' then 'BBC' 
    when a.channel = '1' and a.attribute = '合伙人客户' then '城市服务商' 
    when a.channel = '1' and (a.customer_name like '%内%购%' or a.customer_name like '%临保%') then '批发内购'  
    when a.channel = '1' and a.attribute = '贸易客户' and d.order_profit_rate <= 0.015 then '批发内购' 
    when a.channel = '1' and a.attribute = '贸易客户' and d.order_profit_rate > 0.015 then '省区大宗'
    when a.channel = '1' and a.order_kind = 'WELFARE' then '福利单' 
    else '日配单' end sale_group,
  a.channel_name,
  a.province_code,
  a.province_name,
  a.city_real,
  a.perform_dc_code,
  a.dc_code,
  a.attribute,
  a.first_category,
  a.second_category,
  a.third_category,
  a.sales_name,
  a.work_no,
  a.division_code,
  a.division_name,
  a.department_code,
  a.department_name,
  c.classify_middle_code,
  c.classify_middle_name,
  sum(sales_value) as sales_value,
  sum(excluding_tax_sales) as excluding_tax_sales,
  sum(sales_cost) as sales_cost,
  sum(excluding_tax_cost) as excluding_tax_cost,
  sum(profit) as profit,
  sum(excluding_tax_profit) as excluding_tax_profit,
  a.is_factory_goods_name
from 
(
  select * from csx_dw.dws_sale_r_d_customer_sale
  where sdt >= '20201101' and sdt < '20201201' and channel <> '2'
    and order_no not in ('OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025')
) a left join
(
  SELECT code FROM csx_ods.source_basic_w_a_md_company_code
  WHERE sdt = regexp_replace(date_sub(current_date, 1), '-', '')
) b on a.sign_company_code = b.code
left join
(
  select goods_id, classify_middle_code, classify_middle_name
  from csx_dw.dws_basic_w_a_csx_product_m
  where sdt = 'current'
) c on a.goods_code = c.goods_id
left join
(
  select order_no, sum(profit)/sum(sales_value) order_profit_rate
  from csx_dw.dws_sale_r_d_customer_sale
  where sdt >= '20201101' and sdt < '20201201' and channel in ('1','7','9')
  group by order_no
) d on a.order_no = d.order_no
group by a.dc_company_code, a.sign_company_code, a.sales_date, a.customer_no, a.customer_name,
  case when a.channel not in ('1','7','9') then '' when a.channel = '7' then 'BBC' 
    when a.channel = '1' and a.attribute = '合伙人客户' then '城市服务商' 
    when a.channel = '1' and (a.customer_name like '%内%购%' or a.customer_name like '%临保%') then '批发内购'  
    when a.channel = '1' and a.attribute = '贸易客户' and d.order_profit_rate <= 0.015 then '批发内购' 
    when a.channel = '1' and a.attribute = '贸易客户' and d.order_profit_rate > 0.015 then '省区大宗'
    when a.channel = '1' and a.order_kind = 'WELFARE' then '福利单' else '日配单' end,
  a.channel_name, a.province_code, a.province_name, a.city_real, a.perform_dc_code, a.dc_code, a.attribute,
  a.first_category, a.second_category, a.third_category, a.sales_name, a.work_no, a.division_code, a.division_name,
  a.department_code, a.department_name, c.classify_middle_code, c.classify_middle_name, a.is_factory_goods_name, b.code;

-- 附件3 M端
INSERT OVERWRITE DIRECTORY '/tmp/wangwei/ssssss' row FORMAT DELIMITED fields TERMINATED BY '\t'
select
  a.dc_company_code,
  a.sign_company_code,
  a.sales_date,
  c.sales_belong_flag,
  a.customer_no,
  a.customer_name,
  a.channel_name,
  a.province_code,
  a.province_name,
  a.city_real,
  a.perform_dc_code,
  a.dc_code,
  a.attribute,
  a.first_category,
  a.second_category,
  a.third_category,
  a.sales_name,
  a.work_no,
  a.division_code,
  a.division_name,
  a.department_code,
  a.department_name,
  sum(sales_value) as sales_value,
  sum(excluding_tax_sales) as excluding_tax_sales,
  sum(sales_cost) as sales_cost,
  sum(excluding_tax_cost) as excluding_tax_cost,
  sum(profit) as profit,
  sum(excluding_tax_profit) as excluding_tax_profit,
  a.is_factory_goods_name
from 
(
  select * from csx_dw.dws_sale_r_d_customer_sale
  where sdt >= '20201101' and sdt < '20201201' and channel = '2'
) a left join
(
  select * from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current'
) c on a.customer_no = concat('S', c.shop_id)
group by a.dc_company_code, a.sign_company_code, a.sales_date, c.sales_belong_flag, a.customer_no, a.customer_name,
  a.channel_name, a.province_code, a.province_name, a.city_real, a.perform_dc_code, a.dc_code, a.attribute,
  a.first_category, a.second_category, a.third_category, a.sales_name, a.work_no, a.division_code, a.division_name,
  a.department_code, a.department_name,a.is_factory_goods_name;



-- 附件4 B端
INSERT OVERWRITE DIRECTORY '/tmp/wangwei/dddddd' row FORMAT DELIMITED fields TERMINATED BY '\t'
select
  dc_code,
  dc_name,
  dc_company_code,
  dc_province_name,
  channel_name,
  customer_no,
  customer_name,
  city_real,
  sales_belong_flag,
  department_code,
  department_name,
  goods_code,
  goods_name,
  is_factory_goods_name,
  case when substr(order_no, 1, 2) = 'OC' then '返利'
    when return_flag = 'X' then '退货'
    when return_flag = '' then  '销售' end as order_type,
  sum(sales_qty) as sales_qty,
  promotion_price,
  tax_rate,
  case when order_mode = 0 then '配送'
    when order_mode = 1 then '直送'
    when order_mode = 2 then '自提'
    when order_mode = 3 then '直通'
    else '' end AS order_mode,
  sum(sales_value) as sales_value,
  sum(excluding_tax_sales) as excluding_tax_sales,
  sum(sales_cost) as sales_cost,
  sum(excluding_tax_cost) as excluding_tax_cost,
  sum(profit) as profit,
  sum(excluding_tax_profit) as excluding_tax_profit
from 
(
  select * from csx_dw.dws_sale_r_d_customer_sale
  where sdt >= '20201101' and sdt < '20201201' and channel <> '2'
    and order_no not in ('OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025')
) a left join
(
  select shop_id, sales_belong_flag 
  from csx_dw.dws_basic_w_a_csx_shop_m 
  where sdt = 'current'
) b on a.customer_no = concat('S', b.shop_id)
group by dc_code, dc_name, dc_company_code, dc_province_name, channel_name, customer_no, 
  customer_name, city_real, sales_belong_flag,department_code, department_name, goods_code, 
  goods_name, is_factory_goods_name, case when substr(order_no, 1, 2) = 'OC' then '返利' 
  when return_flag = 'X' then '退货' when return_flag = '' then  '销售' end, promotion_price, 
  tax_rate, case when order_mode = 0 then '配送' when order_mode = 1 then '直送'
  when order_mode = 2 then '自提' when order_mode = 3 then '直通' else '' end;

-- 附件4 M端
INSERT OVERWRITE DIRECTORY '/tmp/wangwei/ffffff' row FORMAT DELIMITED fields TERMINATED BY '\t'
select
  dc_code,
  dc_name,
  dc_company_code,
  dc_province_name,
  channel_name,
  customer_no,
  customer_name,
  province_name,
  case when province_name = '福建省' then '-'
    when province_name = '江苏省' then if(dc_code = 'WOK5' or dc_company_code = '2131', '南京市', '昆山市')
    else city_name end as city_name,
  sales_belong_flag,
  department_code,
  department_name,
  goods_code,
  goods_name,
  is_factory_goods_name,
  if(dc_code like 'E%', '是', '否') as operation_mode,
  case when substr(order_no, 1, 2) = 'OC' then '返利'
    when return_flag = 'X' then '退货'
    when return_flag = '' then  '销售' end as order_type,
  sum(sales_qty) as sales_qty,
  promotion_price,
  tax_rate,
  case when order_mode = 0 then '配送'
    when order_mode = 1 then '直送'
    when order_mode = 2 then '自提'
    when order_mode = 3 then '直通'
    else '' end AS order_mode,
  sum(sales_value) as sales_value,
  sum(excluding_tax_sales) as excluding_tax_sales,
  sum(sales_cost) as sales_cost,
  sum(excluding_tax_cost) as excluding_tax_cost,
  sum(profit) as profit,
  sum(excluding_tax_profit) as excluding_tax_profit
from 
(
  select * from csx_dw.dws_sale_r_d_customer_sale
  where sdt >= '20201101' and sdt < '20201201' and channel = '2'
) a left join
(
  select shop_id, sales_belong_flag 
  from csx_dw.dws_basic_w_a_csx_shop_m 
  where sdt = 'current'
) b on a.customer_no = concat('S', b.shop_id)
group by dc_code, dc_name, dc_company_code, dc_province_name, channel_name, customer_no,
  customer_name, province_name, case when province_name = '福建省' then '-'
  when province_name = '江苏省' then if(dc_code = 'WOK5' or dc_company_code = '2131', '南京市', '昆山市')
  else city_name end, city_real, sales_belong_flag,department_code, department_name, goods_code,
  goods_name, is_factory_goods_name, if(dc_code like 'E%', '是', '否'),
  case when substr(order_no, 1, 2) = 'OC' then '返利' when return_flag = 'X' then '退货'
  when return_flag = '' then  '销售' end, promotion_price, tax_rate, case when order_mode = 0 then '配送'
  when order_mode = 1 then '直送' when order_mode = 2 then '自提' when order_mode = 3 then '直通' else '' end;



-- 返利 收款和退款
INSERT OVERWRITE DIRECTORY '/tmp/wangwei/qqqqqqq' row FORMAT DELIMITED fields TERMINATED BY '\t'
select
  a.rebate_no,
  e.company_code,
  d.company_code,
  if(g.code is null, round(0.01*sum(a.total_price), 2), '') as channel_value,
  if(g.code is null, round(0.99*sum(a.total_price), 2), '') as caiwu_sales_value,
  if(g.code is null, round(0.01*sum(a.total_price_no_tax), 2), '') as excluding_tax_channel_value,
  if(g.code is null, round(0.99*sum(a.total_price_no_tax), 2), '') as excluding_tax_caiwu_sales_value,
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
  c.division_code,
  c.division_name,
  c.department_id,
  c.department_name,
  sum(a.total_price) as sales_value,
  sum(a.total_price_no_tax) as excluding_tax_sales,
  '' as sales_cost,
  '' as excluding_tax_cost,
  '' as profit,
  '' as excluding_tax_profit,
  if(f.workshop_code is null, '不是工厂商品', '是工厂商品') as is_factory_goods_name
from
(
  select *
  from csx_dw.dwd_csms_r_d_yszx_customer_rebate_detail_new
  where sdt >= '20201101' and sdt < '20201201' and type in (0, 1) and status = 1
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
group by a.rebate_no, e.company_code, d.company_code, a.sdt, a.sap_cus_code, a.sap_cus_name, b.channel, b.sales_province_code,
  b.sales_province, b.sales_city, a.agreement_dc_code, a.inventory_dc_code, b.attribute, b.first_category, b.second_category,
  b.third_category, b.sales_name, b.work_no, c.division_code, c.division_name, c.department_id, c.department_name,
  if(f.workshop_code is null, '不是工厂商品', '是工厂商品'), g.code;
