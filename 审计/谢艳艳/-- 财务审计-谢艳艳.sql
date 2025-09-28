-- 财务审计-谢艳艳
-- 销售客户&供应商是稽核 品类一致的商品明细
  with aa as (
  select
    basic_performance_province_name,
    basic_performance_city_name,
    case
      when division_code in ('10', '11') then '11'
      else '12'
    end division_code,
    case
      when division_code in ('10', '11') then '生鲜'
      else '食百'
    end division_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    a.supplier_code,
    supplier_name,
    c.supplier_tax_code,
    goods_code,
    goods_name,
    sum(receive_qty-shipped_qty) as net_qty,
    sum(no_tax_receive_amt - no_tax_shipped_amt) net_amt
  from
    csx_analyse.csx_analyse_scm_purchase_order_flow_di a
    join (
      select
        shop_code,
        company_code,
        basic_performance_province_name,
        basic_performance_city_name
      from
        csx_dim.csx_dim_shop
      where
        sdt = 'current'
       -- and company_code = '2304'
        and purpose not in('09', '04', '06')
        and basic_performance_region_name = '华西大区'
    ) b on a.dc_code = b.shop_code
    join (
      select
        supplier_code,
        supplier_tax_code
      from
        csx_dim.csx_dim_basic_supplier
      where
        sdt = 'current'
    ) c on a.supplier_code = c.supplier_code
  where
    sdt >= '20221001'
    and sdt<='20231031'
  group by
    basic_performance_province_name,
    case
      when division_code in ('10', '11') then '11'
      else '12'
    end,
    case
      when division_code in ('10', '11') then '生鲜'
      else '食百'
    end,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    a.supplier_code,
    supplier_name,
    c.supplier_tax_code,
    goods_code,
    goods_name,
    basic_performance_city_name
),
bb as (
  select
    performance_province_name,
    performance_city_name,
    social_credit_code,
    a.customer_code,
    customer_name,
    classify_middle_code,
    classify_middle_name,
    goods_code,
    goods_name,
    sum(sale_qty) sale_qty,
    sum(sale_amt_no_tax) sale_amt
  from
    csx_dws.csx_dws_sale_detail_di a
    join (
      select
        customer_code,
        social_credit_code
      from
        csx_dim.csx_dim_crm_customer_info
      where
        sdt = 'current'
    ) b on a.customer_code = b.customer_code
  where
    sdt >= '20221001'
    and sdt<='20231031'
   -- and performance_province_name='北京市'
 --   and performance_province_name='湖北省'
  group by
    goods_code,
    goods_name,
    social_credit_code,
    a.customer_code,
    customer_name,
    classify_middle_code,
    classify_middle_name,
    performance_province_name,
    performance_city_name
)
select
  performance_province_name,
  performance_city_name,
  social_credit_code,
  customer_code,
  customer_name,
  bb.classify_middle_code,
  bb.classify_middle_name,
  bb.goods_code,
  bb.goods_name,
  sale_amt,
  sale_qty,
  basic_performance_province_name,
  basic_performance_city_name,
  aa.classify_middle_code a_classify_middle_code,
  aa.classify_middle_name a_classify_middle_name,
  aa.goods_code aa_goods_code,
  aa.goods_name aa_goods_name,
  aa.supplier_code,
  supplier_name,
  supplier_tax_code,
  net_amt,
  net_qty
from
  bb
  join aa on bb.social_credit_code = aa.supplier_tax_code 
    and aa.classify_middle_code=bb.classify_middle_code
  where bb.social_credit_code !=''
;




-- 是客户&供应商是稽核品类稽核
  with aa as (
  select
    basic_performance_province_name,
    basic_performance_city_name,
    case
      when division_code in ('10', '11') then '11'
      else '12'
    end division_code,
    case
      when division_code in ('10', '11') then '生鲜'
      else '食百'
    end division_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    a.supplier_code,
    supplier_name,
    c.supplier_tax_code,
    sum(no_tax_receive_amt - no_tax_shipped_amt) net_amt
  from
    csx_analyse.csx_analyse_scm_purchase_order_flow_di a
    join (
      select
        shop_code,
        company_code,
        basic_performance_province_name,
        basic_performance_city_name
      from
        csx_dim.csx_dim_shop
      where
        sdt = 'current'
       -- and company_code = '2304'
        and purpose not in('09', '04', '06')
        and basic_performance_region_name = '华西大区'
    ) b on a.dc_code = b.shop_code
    join (
      select
        supplier_code,
        supplier_tax_code
      from
        csx_dim.csx_dim_basic_supplier
      where
        sdt = 'current'
    ) c on a.supplier_code = c.supplier_code
  where
    sdt >= '20221001'
    and sdt<='20231031'
  group by
    basic_performance_province_name,
    case
      when division_code in ('10', '11') then '11'
      else '12'
    end,
    case
      when division_code in ('10', '11') then '生鲜'
      else '食百'
    end,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    a.supplier_code,
    supplier_name,
    c.supplier_tax_code,
    basic_performance_city_name
),
bb as (
  select
    performance_province_name,
    performance_city_name,
    social_credit_code,
    a.customer_code,
    customer_name,
    classify_middle_code,
    classify_middle_name,
    sum(sale_amt_no_tax) sale_amt
  from
    csx_dws.csx_dws_sale_detail_di a
    join (
      select
        customer_code,
        social_credit_code
      from
        csx_dim.csx_dim_crm_customer_info
      where
        sdt = 'current'
    ) b on a.customer_code = b.customer_code
  where
    sdt >= '20221001' --   and performance_province_name='北京市'
  --  and performance_region_name='华西大区'
  group by
    social_credit_code,
    a.customer_code,
    customer_name,
    classify_middle_code,
    classify_middle_name,
    performance_province_name,
    performance_city_name
)
select
  performance_province_name,
  performance_city_name,
  social_credit_code,
  customer_code,
  customer_name,
  bb.classify_middle_code,
  bb.classify_middle_name,
  sale_amt,
  basic_performance_province_name,
  basic_performance_city_name,
  aa.classify_middle_code a_classify_middle_code,
  aa.classify_middle_name a_classify_middle_name,
  aa.supplier_code,
  supplier_name,
  supplier_tax_code,
  net_amt
from
  bb
  join aa on bb.social_credit_code = aa.supplier_tax_code 
  -- and aa.classify_middle_code=bb.classify_middle_code
  where bb.social_credit_code !=''
;

