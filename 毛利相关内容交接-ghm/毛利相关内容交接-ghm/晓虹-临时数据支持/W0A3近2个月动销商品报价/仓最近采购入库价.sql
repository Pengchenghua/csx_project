-- 商品动销及报价情况\--商品动销及报价情况.sql
desc csx_dwd.csx_dwd_price_effective_purchase_prices_di;


-- 销售金额、销售毛利率、商品状态、本期采购报价
with tmp_sale_detail as (
  select
    inventory_dc_code,
    goods_code,
    goods_name,
    sum(sale_amt) sale_amt,
    sum(profit) profit,
    sum(profit) / sum(sale_amt) profit_rate
  from
    csx_dws.csx_dws_sale_detail_di
  where
    sdt >= '20250401'
    and sdt <= '20250615'
    and inventory_dc_code = 'W0A3'
  group by
    inventory_dc_code,
    goods_code,
    goods_name
),
tmp_csx_dwd_price_effective_purchase_prices_di as (
  select
    *
  from
    (
      select
        warehouse_code,
        product_code,
        purchase_price,
        row_number() over(
          partition by warehouse_code,
          product_code
          order by
            price_end_time desc
        ) as rn
      from
        csx_dwd.csx_dwd_price_effective_purchase_prices_di
      where
        warehouse_code = 'W0A3'
        and effective = 1
        and base_product_status = 0
        and sdt >= '20250101'
        and substr(price_begin_time,1,10) <= current_date
        and substr(price_end_time,1,10) >= current_date
    ) a
  where
    rn = 1
)
select
  a.inventory_dc_code,
  a.goods_code,
  c.goods_name,
  classify_large_name,
  classify_middle_name,
  classify_small_name,
  sale_amt,
  profit,
  profit_rate,
  goods_status_name,
  purchase_price
from
  tmp_sale_detail a
  left join (
    select
      goods_code,
      goods_name,
      goods_status_name
    from
      csx_dim.csx_dim_basic_dc_goods
    where
      sdt = 'current'
      and dc_code = 'W0A3'
  ) b on a.goods_code = b.goods_code
  left join (
    select
      goods_code,
      goods_name,
      classify_large_name,
      classify_middle_name,
      classify_small_name
    from
      csx_dim.csx_dim_basic_goods
    where
      sdt = 'current'
  ) c on a.goods_code = c.goods_code
  left join tmp_csx_dwd_price_effective_purchase_prices_di d on a.goods_code = d.product_code