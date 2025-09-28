-- 项目合伙人下单情况 -蒋艳
with o as (select
  order_code,
  customer_code,
  customer_name,
  create_by,
  is_town_order,
  count(goods_code) as count_sku,
  sum(purchase_qty*sale_price) as purchase_amt
  from 
  csx_dwd.csx_dwd_oms_sale_order_detail_di
where
  partner_type_code != 0
  and  sdt>='20231201'
    and sdt<='20231231'
    group by   order_code,
  customer_code,
  customer_name,
  create_by,
  is_town_order
  ) ,
  sale as (
  
  select order_code ,business_type_code		 
    from csx_analyse.csx_analyse_bi_sale_detail_di
    where sdt>='20231201'
        and sdt<='20231231'
        and business_type_code=4
        group by order_code ,business_type_code
    )
    select  o.order_code,
  customer_code,
  customer_name,
  create_by,
  is_town_order,
    count_sku,
  purchase_amt,
  business_type_code
  from o 
  left join sale on o.order_code=sale.order_code
  where business_type_code is not null 