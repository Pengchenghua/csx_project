-- 省区、DC，商品编码/条码，商品名称，区域化名称，客户化名称，管理一/二/三级，品牌，单位，商品状态，11月入库，12月入库，1月入库，
select
  substr(sdt, 1, 6) smt,
  performance_region_name,
  performance_province_name,
  performance_city_name,
  a.dc_code,
  a.dc_name,
  a.goods_code,
  b.goods_bar_code,
  goods_name,
  b.goods_short_name,
  b.regionalized_goods_name,
  b.classify_large_name,
  b.classify_middle_name,
  b.classify_small_name,
  sum(receive_amt) receive_amt,
  sum(receive_qty) receive_qty
from
  csx_analyse.csx_analyse_scm_purchase_order_flow_di a
  left join (
    select
      dc_code,
      a.goods_code,
      b.goods_bar_code,
      goods_short_name,
      regionalized_goods_name,
      brand_name,
      classify_large_name,
      classify_middle_code,
      classify_middle_name,
      classify_small_code,
      classify_small_name,
      goods_status_name
    from
      csx_dim.csx_dim_basic_dc_goods a
      left join (
        select
          goods_code,
          goods_bar_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name,
          classify_small_code,
          classify_small_name
        from
          csx_dim.csx_dim_basic_goods
        where
          sdt = 'current'
      ) b on a.goods_code = b.goods_code
    where
      sdt = 'current'
  ) b on a.goods_code = b.goods_code
  and a.dc_code = b.dc_code
  join (
    select
      dc_code,
      regexp_replace(to_date(enable_time), '-', '') enable_date,
      '1' is_dc_tag
    from
      csx_dim.csx_dim_csx_data_market_conf_supplychain_location
    where
      sdt = 'current'
  ) c on a.dc_code = c.dc_code
where
  sdt >= '20241101'
  and sdt <= '20250131'
group by
  performance_region_name,
  performance_province_name,
  performance_city_name,
  a.dc_code,
  a.dc_name,
  a.goods_code,
  b.goods_bar_code,
  goods_name,
  b.goods_short_name,
  b.regionalized_goods_name,
  b.classify_large_name,
  b.classify_middle_name,
  b.classify_small_name,
  substr(sdt, 1, 6)