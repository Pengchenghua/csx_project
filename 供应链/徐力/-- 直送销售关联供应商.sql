-- 直送销售关联供应商
/*
** 注意：取直送关联供应商只有是正向单，逆向单、调价、价格补救单无法关联，
*/
with sale as (select
  a.sdt,
  performance_region_name,
  performance_province_code        ,
  performance_province_name        ,
  performance_city_code        ,
  performance_city_name,
  inventory_dc_code,
  inventory_dc_name,
  operation_mode_name,
  original_order_code,
  order_code,
  business_type_name,
  delivery_type_code,
  delivery_type_name,
  customer_code,
  customer_name,
  sub_customer_code,
  sub_customer_name        ,
  channel_name,        
  a.goods_code,
  a.goods_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name        ,
  sale_qty,
  sale_amt,
  sale_cost,
  profit,
  refund_order_flag,
  order_channel_code,
  order_channel_detail_name,
  direct_delivery_type,
--   b.supplier_code,
--   b.supplier_name,
--   b.purchase_price,
--   sale_unit_send_qty,
  b.purchase_order_code,
  b.delivery_type as argreement_delivery_type,
  float_ratio,      -- 为固定扣点,不等于0即为固定扣点
  price_type,       -- 价格类型 1-周期进 2-售价下浮 3-不指定
  assign_type,      --  1、客户指定，联营直送、2 供应链指定
  assign_supplier,      --指定供应商 0-否 1-是
  auto_audit_flag ,
  if(float_ratio<>0,1,0) float_flag ,  -- 判断固定扣点
  shop_low_profit_flag,
  /*
  1、assign_type 1、客户指定，联营直送、2 供应链指定 为供应链配置信息
  2、按照优先级定义，如果直送1 \2  有包含 assign_type 类型，先取直送1\2
  3、供应链端'客户指定供应商'与销售端直送1'客户指定供应商' 有区别，需要注意两者的区别
  */
  case when delivery_type_name<>'直送' then '配送'
  --  when direct_delivery_type=0 and assign_type=1 then '客户指定供应商'
  --  when direct_delivery_type=0   then '供应链指定'
    when direct_delivery_type=1   then '直送1'   -- 客户指定包含直送1+采购客户指定，细分优先直送1
    when direct_delivery_type=2 then '直送2'    -- 直送2
    when direct_delivery_type=11 then '临时加单'
    when direct_delivery_type=12 then '紧急补货'
    else '普通' 
    end direct_delivery_type_name,
  case when delivery_type_name<>'直送' then '配送'
  --  when direct_delivery_type=0 and assign_type=1 then '客户指定供应商'
    when direct_delivery_type=0 and assign_type=2 then '供应链指定'
    when (direct_delivery_type=1  or assign_type=1) then '客户指定'   -- 客户指定包含直送1+采购客户指定，细分优先直送1
    when direct_delivery_type=2 then '客户自购'    -- 直送2
    when direct_delivery_type=11 then '临时加单'
    when direct_delivery_type=12 then '紧急补货'
    else 'T+0调度直送' 
    end new_direct_delivery_type,
 case when delivery_type_name<>'直送' then '配送'
    when (direct_delivery_type=1  or assign_type IN (1,2)) then '计划直送'   -- 客户指定包含直送1+采购客户指定，细分优先直送1
    when direct_delivery_type in (11,12,2) then '紧急直送'
    else 'T+0调度直送' 
    end direct_delivery_large_type,
    agreement_order_code
from
    csx_dws.csx_dws_sale_detail_di a
 left join 
  --关联履约单明细，客户指定或者供应链指定取assign_type，固定扣点 float_ratio，履约单最早为23年7月
  -- CO为直送履约
  (
    select
      agreement_order_code,
      sale_order_code,
      delivery_type,
      goods_code,
      float_ratio,      -- 为固定扣点,不等于0即为固定扣点
      price_type,       -- 价格类型 1-周期进 2-售价下浮 3-不指定
      assign_type,      --  1、客户指定，联营直送、2 供应链指定
      assign_supplier,      --指定供应商 0-否 1-是
      auto_audit_flag,       -- 自动审核标识 0-否 1-是
    --   supplier_code,
    --   supplier_name,
    --   purchase_price,
      sale_unit_send_qty,
      purchase_order_code
    from
         csx_dwd.csx_dwd_oms_agreement_order_detail_di
       where sdt>='20230101'
  ) b 
  -- 根据wms_order_code 会出现一个履约单号 后面分组 -1、-2
  on  regexp_replace(wms_order_code,'-.*','') = agreement_order_code 
  -- 按照源销售单关联履约销售单
  -- and original_order_code=b.sale_order_code
  and a.goods_code=b.goods_code
  left join 
  (select shop_code,shop_low_profit_flag from   csx_dim.csx_dim_shop where sdt='current') c on a.inventory_dc_code=c.shop_code
where
  sdt >= '20240101'
  and sdt<'20240523'
  and business_type_code='1'
  and delivery_type_name='直送'
  ),
  purch as 
  (select order_code, 
    supplier_code,
    supplier_name,
    goods_code,
    order_qty,
    price_include_tax,
    amount_include_tax
  from
    csx_dws.csx_dws_scm_order_detail_di 
  where sdt>='20230101'
  )
  select substr(sdt,1,6) month,
  order_channel_detail_name,
 -- refund_order_flag,
    performance_province_name,
    performance_city_name,
    inventory_dc_code,
    inventory_dc_name,
    operation_mode_name,
    -- purchase_order_code,
    -- agreement_order_code,
    -- original_order_code,
    -- order_code,
    shop_low_profit_flag,
    direct_delivery_type_name,
    rp_service_user_name_new,
    customer_code,
    customer_name,
    sub_customer_code,
    sub_customer_name,
    classify_large_name,
    classify_middle_name,
    a.goods_code,
    goods_name,
    sum(sale_qty) sale_qty,
    sum(sale_amt) sale_amt,
    sum(profit)profit ,
    sum(profit)/sum(sale_amt) prfit_rate,
    float_flag,
    supplier_code,
    supplier_name,
    sum(amount_include_tax) purchase_amt
    from sale a
    left join 
    purch as b on a.purchase_order_code=b.order_code and a.goods_code=b.goods_code
    left join
    (select  customer_no,rp_service_user_name_new
    from  csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
    where  sdt='20240523') c on a.customer_code=c.customer_no
    where 1=1 
    -- performance_city_name='福州市'
    --  sdt>='20240101' and sdt<='20240131'
    and refund_order_flag <>1 
    
     group by substr(sdt,1,6),
     performance_province_name,
    performance_city_name,
    inventory_dc_code,
    inventory_dc_name,
    shop_low_profit_flag,
    direct_delivery_type_name,
    customer_code,
    customer_name,
    sub_customer_code,
    sub_customer_name,
    classify_large_name,
    classify_middle_name,
    a.goods_code,
    goods_name,
    float_flag,
    supplier_code,
    supplier_name,
    order_channel_detail_name,
   -- refund_order_flag,
    operation_mode_name,
    rp_service_user_name_new
--     original_order_code,
--   order_code,
--   agreement_order_code,
--   purchase_order_code
  