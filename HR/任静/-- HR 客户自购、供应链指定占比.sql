-- HR 客户自购、供应链指定占比

/*
大客户5月、6月 客户指定供应商和客户自购这两个类型的销售额和占比，
另外就是看看这些单子前置和后置的数据和占比，
下单日期晚于送货日期是后置，
下单日期早于送货日期是前置，帮忙看看
*/
with sale as (
  select
    a.sdt,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    inventory_dc_code,
    inventory_dc_name,
    original_order_code,
    order_code,
    order_time,
    delivery_time,
    if(delivery_time<order_time, 1, 0) as delivery_time_flag,
    regexp_replace(wms_order_code, '-.*', '')wms_order_code,
    business_type_name,
    delivery_type_code,
    delivery_type_name,
    customer_code,
    customer_name,
    sub_customer_code,
    sub_customer_name,
    channel_name,
    a.goods_code,
    a.goods_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    sale_qty,
    sale_amt,
    sale_cost,
    profit,
    refund_order_flag,
    order_channel_code,
    order_channel_detail_name,
    direct_delivery_type,
    b.delivery_type as argreement_delivery_type,
    float_ratio,
    -- 为固定扣点,不等于0即为固定扣点
    price_type,
    -- 价格类型 1-周期进 2-售价下浮 3-不指定
    assign_type,
    --  1、客户指定，联营直送、2 供应链指定
    assign_supplier,
    --指定供应商 0-否 1-是
    auto_audit_flag,
    if(float_ratio <> 0, 1, 0) float_flag, -- 判断固定扣点
    shop_low_profit_flag,     -- 是否直送仓
    delivery_to_direct_flag,   -- 配转直 
    /*
     1、assign_type 1、客户指定，联营直送、2 供应链指定 为供应链配置信息
     2、按照优先级定义，如果直送1 \2  有包含 assign_type 类型，先取直送1\2
     3、供应链端'客户指定供应商'与销售端直送1'客户指定供应商' 有区别，需要注意两者的区别
     */
    case
      when delivery_type_name <> '直送' then '配送' --  when direct_delivery_type=0 and assign_type=1 then '客户指定供应商'
      when direct_delivery_type = 0
      and assign_type = 2 then '供应链指定'
      when (direct_delivery_type = 1 or assign_type = 1 ) then '客户指定' -- 客户指定包含直送1+采购客户指定，细分优先直送1
      when direct_delivery_type = 2 then '客户自购' -- 直送2
      when direct_delivery_type = 11 then '临时加单'
      when direct_delivery_type = 12 then '紧急补货'
      else 'T+0调度直送'
    end new_direct_delivery_type,
    case
      when delivery_type_name <> '直送' then '配送'
      when (
        direct_delivery_type = 1
        or assign_type IN (1, 2)
      ) then '计划直送' -- 客户指定包含直送1+采购客户指定，细分优先直送1
      when direct_delivery_type in (11, 12, 2) then '紧急直送'
      else 'T+0调度直送'
    end direct_delivery_large_type,
    b.scm_order_code as scm_order_code
  from
      csx_dws.csx_dws_sale_detail_di a
    left join 
    --关联履约单明细，客户指定或者供应链指定取assign_type，固定扣点 float_ratio，履约单最早为23年7月
    -- CO为直送履约
    (
      select
        a.agreement_order_code,
        scm_order_code,
        a.sale_order_code,
        a.delivery_type,
        a.goods_code,
        a.float_ratio,
        -- 为固定扣点,不等于0即为固定扣点
        a.price_type,
        -- 价格类型 1-周期进 2-售价下浮 3-不指定
        a.assign_type,
        --  1、客户指定，联营直送、2 供应链指定
        a.assign_supplier,
        --指定供应商 0-否 1-是
        a.auto_audit_flag,
        -- 自动审核标识 0-否 1-是
        delivery_to_direct_flag -- 是否配转直
      from
        csx_dwd.csx_dwd_oms_agreement_order_detail_di a
        left join (
          select
            order_code as scm_order_code,
            source_order_code,
            agreement_order_no,
            delivery_to_direct_flag,
            goods_code
          from
            csx_dws.csx_dws_scm_order_detail_di
          where
            sdt >= '20240401' -- and delivery_to_direct_flag = 1
            and price_remedy_flag !=1 --  剔除补救
          group by
            source_order_code,
            agreement_order_no,
            delivery_to_direct_flag,
            goods_code,
            order_code
        ) b on a.agreement_order_code = b.agreement_order_no
        and a.goods_code = b.goods_code
      where
        sdt >= '20240401'
    ) b -- 根据wms_order_code 会出现一个履约单号 后面分组 -1、-2
    on regexp_replace(wms_order_code, '-.*', '') = agreement_order_code -- 按照源销售单关联履约销售单
    -- and original_order_code=b.sale_order_code
    and a.goods_code = b.goods_code
    left join (
      select
        shop_code,
        shop_low_profit_flag
      from
        csx_dim.csx_dim_shop
      where
        sdt = 'current'
    ) c on a.inventory_dc_code = c.shop_code
  where
    sdt >= '20240501'
    and sdt < '20240701'
    and business_type_code in( '1')
)
select
  substr(sdt,1,6) sale_month,
  performance_region_name,
  performance_province_name,
  performance_city_name,
  inventory_dc_code,
  inventory_dc_name,
  customer_code,
  customer_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  new_direct_delivery_type,
  sum( sale_qty ) as sale_qty,
  sum( sale_amt ) as sale_amt,
  sum( sale_cost ) as sale_cost,
  sum( profit ) as profit,
  sum(if(new_direct_delivery_type='客户自购',sale_amt,0)) as khzg_sale_amt,
  sum(if(new_direct_delivery_type='客户自购',profit,0)) as khzg_profit,
  sum(if(new_direct_delivery_type='供应链指定',sale_amt,0)) as gxzd_sale_amt,
  sum(if(new_direct_delivery_type='供应链指定',profit,0)) as gxzd_profit

from
  sale
where 1=1
group by substr(sdt,1,6),
  performance_region_name,
  performance_province_name,
  performance_city_name,
  inventory_dc_code,
  inventory_dc_name,
  customer_code,
  customer_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
new_direct_delivery_type
  -- new_direct_delivery_type != '配送'
 -- and wms_order_code='CO24060900008314'