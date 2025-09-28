-- 成本端签收维度数据关系凭证

drop table csx_tmp.cust_sign_detail_1;
create table csx_tmp.cust_sign_detail_1
as
SELECT 
  b.province_code,
  b.province_name,
  d.channel, 
  b.sales_belong_flag,
  a.source_sys,
  a.credential_no,
  a.product_code ,
  a.product_name,
  a.posting_time,
  a.create_by,
  a.source_order_no,
  a.orig_source_order_no,
  a.out_order_no,
  a.source_order_type,
  b.company_code as dc_company_code,
  c.name,
  a.customer_no,
  d.customer_name,
  a.sub_customer_no,
  a.sub_customer_name,
  a.create_time,
  a.sign_company_code,
  a.sign_company_name,
  a.contract_location_code,
  a.contract_location_name,
  a.company_code,
  a.company_name,
  a.location_code,
  a.location_name,
  a.remark,
  a.return_reason,
  a.logistics_mode,
  a.replenish_type,
  a.bar_code,
  a.spec,
  a.number_of_packages,
  a.unit,
  a.qty,
  a.root_category,
  a.purchase_group_code,
  a.purchase_group_name,
  a.small_category_code,
  a.small_category_name,
  a.sale_order_price,
  a.tax_code,
  a.tax_rate,
  a.sale_amt_no_tax,
  a.sale_amt,
  a.cost_amt_no_tax,
  a.cost_amt,
  a.sale_amt_no_tax-a.cost_amt_no_tax as profit_no_tax
FROM 
(
  select * from csx_ods.source_sync_r_d_data_relation_cas_sale_receiving_credential
  where sdt = '19990101' and posting_time >= '2020-05-01 00:00:00' AND posting_time < '2020-05-31 59:59:59'
  and (source_order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046') or order_no is null)
    --and source_sys = '3'
) a
LEFT JOIN 
    (select shop_id,province_code,province_name,company_code,sales_belong_flag
    from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current') b ON a.customer_no = b.shop_id
LEFT JOIN (select name,code from csx_dw.dws_basic_w_a_company_code where sdt='current') c ON b.company_code = c.code
LEFT JOIN
(select customer_no, customer_name, channel
  from csx_dw.dws_crm_w_a_customer_m
  where sdt = '20200531' and customer_no <> ''
)d on concat('S',a.customer_no)=d.customer_no
where  b.province_name='北京市'
;



--insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t' 
drop table csx_tmp.cust_sign_detail_2;
create table csx_tmp.cust_sign_detail_2
as
select a.*,if(c.workshop_code is null,'不是工厂商品','是工厂商品') as is_factory_goods_name,fh_sdt
from csx_tmp.cust_sign_detail_1 a
left join
(select 
sdt as fh_sdt,
order_no,goods_code
from csx_dw.dws_sale_r_d_customer_sale
where sdt>='20191201'
)b on b.order_no=a.source_order_no and b.goods_code=a.product_code
left join
  (select
      workshop_code, province_code, goods_code
    from csx_dw.dws_mms_w_a_factory_setting_craft_once_all
    where sdt='current' and new_or_old=1
  )c on a.province_code=c.province_code and a.product_code=c.goods_code;
  
  
  
insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by ',' 
select * from csx_tmp.cust_sign_detail_2;















