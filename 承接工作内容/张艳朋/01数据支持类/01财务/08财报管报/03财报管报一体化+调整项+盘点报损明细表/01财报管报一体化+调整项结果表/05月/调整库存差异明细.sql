
--202105 调整库存差异明细
insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t'
select
  a.channel_name,a.sdt,a.credential_no,a.order_no,a.business_type_name,
  a.region_name,a.province_name,a.city_group_name,a.dc_code,a.perform_dc_code,
  a.customer_no,c.customer_name,c.first_category_name,c.second_category_name,c.third_category_name,
  a.sign_company_code,a.sign_company_name,
  a.goods_code,d.goods_name,d.division_code,d.division_name,d.department_id,d.department_name,
  d.classify_middle_code,d.classify_middle_name,
  b.sales_qty,b.cost_price,b.sales_value,b.sales_cost,b.excluding_tax_sales,b.excluding_tax_cost,
  b.cost_price_after,b.cost_amt_after,b.cost_amt_no_tax_after,
  b.cost_price_no_tax_after,b.cost_amt_diff,b.cost_amt_no_tax_diff,a.is_factory_goods_desc
from
(
  select 
  split(id, '&')[0] as credential_no,*
  from csx_dw.dws_sale_r_d_detail
  where sdt >= '20210501' and sdt<'20210601'
  --and sales_type <> 'fanli'
)a 
left join
(
  select * from csx_dw.dws_sale_r_d_simple
  where sdt >= '20210501'
) b on a.credential_no = b.credential_no and a.goods_code = b.goods_code
left join
(
  select * from csx_dw.dws_crm_w_a_customer
  where sdt = 'current'
) c on a.customer_no = c.customer_no
left join
 (select * from csx_dw.dws_basic_w_a_csx_product_m where sdt='current' 
)d on d.goods_id=a.goods_code
where b.cost_amt_diff is not null;



--调整库存差异明细 汇总数据
set hive.execution.engine=mr;
insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t'
select
  a.channel_name,a.sdt,a.business_type_name,
  a.region_name,a.province_name,a.city_group_name,a.dc_code,a.perform_dc_code,
  a.customer_no,c.customer_name,c.first_category_name,c.second_category_name,c.third_category_name,
  a.sign_company_code,a.sign_company_name,
  d.division_code,d.division_name,d.department_id,d.department_name,
  d.classify_middle_code,d.classify_middle_name,a.is_factory_goods_desc,
  sum(b.sales_value) sales_value,
  sum(b.sales_cost) sales_cost,
  sum(b.excluding_tax_sales) excluding_tax_sales,
  sum(b.excluding_tax_cost) excluding_tax_cost,
  sum(b.cost_amt_after) cost_amt_after,
  sum(b.cost_amt_no_tax_after) cost_amt_no_tax_after,
  sum(b.cost_amt_diff) cost_amt_diff,
  sum(b.cost_amt_no_tax_diff) cost_amt_no_tax_diff
from
(
  select 
  split(id, '&')[0] as credential_no,*
  from csx_dw.dws_sale_r_d_detail
  where sdt >= '20210501' and sdt<'20210601'
  --and sales_type <> 'fanli'
)a 
join
(
  select * from csx_dw.dws_sale_r_d_simple
  where sdt >= '20210501'
) b on a.credential_no = b.credential_no and a.goods_code = b.goods_code
left join
(
  select * from csx_dw.dws_crm_w_a_customer
  where sdt = 'current'
) c on a.customer_no = c.customer_no
left join
 (select * from csx_dw.dws_basic_w_a_csx_product_m where sdt='current' 
)d on d.goods_id=a.goods_code
group by 
  a.channel_name,a.sdt,a.business_type_name,
  a.region_name,a.province_name,a.city_group_name,a.dc_code,a.perform_dc_code,
  a.customer_no,c.customer_name,c.first_category_name,c.second_category_name,c.third_category_name,
  a.sign_company_code,a.sign_company_name,
  d.division_code,d.division_name,d.department_id,d.department_name,
  d.classify_middle_code,d.classify_middle_name,a.is_factory_goods_desc
having cost_amt_diff<>0 and cost_amt_no_tax_diff<>0
;





