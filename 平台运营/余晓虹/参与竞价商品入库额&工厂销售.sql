-- 参与竞价商品入库额
create table csx_analyse_tmp.csx_analyse_tmp_canyujingj as 
    select
      basic_performance_city_code,
      basic_performance_city_name,
      h.target_location_code,
      purpose,
      purpose_name,
      mp.classify_large_code,
      mp.classify_large_name,
      mp.classify_middle_code,
      mp.classify_middle_name,
      classify_small_code,
      classify_small_name,
      r.purchase_order_code,
      r.goods_code,
      substr(h.create_time, 1, 7) as create_time,
      SUM(r.received_qty * p.price1_include_tax) as amount
    FROM
      csx_dwd.csx_dwd_scm_order_header_di h
      LEFT JOIN csx_dwd.csx_dwd_scm_order_product_price_di p ON h.order_code = p.order_code
      LEFT JOIN (
        select
          *
        from
          csx_dwd.csx_dwd_scm_order_items_mi
        where
          assign_type != 1
          and status in (3, 4)
          and smt > '2025'
      ) i ON p.order_code = i.order_code
      AND p.goods_code = i.goods_code
      LEFT JOIN csx_dwd.csx_dwd_scm_product_received_di r ON i.order_code = r.purchase_order_code
      AND i.goods_code = r.goods_code
      LEFT JOIN csx_dwd.csx_dwd_scm_purchase_request_detail_di pi ON pi.purchase_order_code = i.order_code
      and pi.goods_code = i.goods_code
      LEFT JOIN csx_dwd.csx_dwd_scm_local_purchase_request_di l ON l.local_purchase_order_code = pi.replenishment_order_code
      and l.local_purchase_plan_type = 2
      LEFT JOIN (
        select
          *
        from
          csx_dim.csx_dim_basic_goods
        where
          sdt = 'current'
      ) mp ON i.goods_code = mp.goods_code
      left join (
        select
          *
        from
          csx_dim.csx_dim_shop
        where
          sdt = 'current'
      ) dc on h.target_location_code = dc.shop_code
    WHERE
      h.source_type not in (20, 21)
      and h.super_class = 1
      and i.assign_type != 1
      and i.status in (3, 4)
      and mp.classify_large_code in ('B01', 'B02', 'B03')
      and p.cycle_price_source = 2
      and l.id is null
      and mp.classify_middle_code not in ('B0902', 'B1001')
      and r.received_qty is not null
      and h.create_time >= '2025-06-01 00:00:00'
      and h.create_time <= '2025-10-09 23:59:59'
    group by
      basic_performance_city_code,
      basic_performance_city_name,
      h.target_location_code,
      purpose,
      purpose_name,
      mp.classify_large_code,
      mp.classify_large_name,
      mp.classify_middle_code,
      mp.classify_middle_name,
      classify_small_code,
      classify_small_name,
      r.purchase_order_code,
      r.goods_code,
      substr(h.create_time, 1, 7)
      
  

-- 入库类型占比，竞价、售价下浮
with scm_order_detail as (
select
  a.*,
  if(p.goods_code is not null ,1,0) jingjia_type
from
  csx_analyse.csx_analyse_scm_purchase_order_flow_di a
  LEFT JOIN csx_analyse_tmp.csx_analyse_tmp_canyujingj p ON a.purchase_order_code = p.purchase_order_code and a.goods_code=p.goods_code
where
  sdt >= '20250901'
  and sdt <= '20250930'
--   and price_type != 2
  and source_type_code in (1, 8, 9, 10, 23, 19)
--   and assign_type != 1
  and a.purpose_name in ('大客户物流', '工厂')
)
select performance_region_name,performance_province_name,
    performance_city_name,
    purpose_name,
    dc_code,
    dc_name,
    goods_code	,
    goods_name,
    classify_large_name,
    classify_middle_name,
    classify_small_name,
    sum(receive_qty) receive_qty,
    sum(receive_amt) receive_amt,
    sum(if(price_type=2,receive_qty,0)) as price_2_qty,
    sum(if(price_type=2,receive_amt,0)) as price_2_amt,
    sum(if(jingjia_type=1,receive_qty,0)) as jingjia_qty,
    sum(if(jingjia_type=1,receive_amt,0)) as jingjia_amt
from scm_order_detail
group by  performance_region_name,performance_province_name,
    performance_city_name,
    purpose_name,
    dc_code,
    dc_name,
    goods_code	,
    goods_name,
    classify_large_name,
    classify_middle_name,
    classify_small_name
  
;


 -- 工厂日配销售额
select substr(sdt,1,6) as sales_months,
    performance_region_name,
    performance_province_name,
    inventory_dc_city_name,
    inventory_dc_code,
    inventory_dc_name,
    a.goods_code,
    b.goods_name,
    b.unit_name,
    case when b.classify_large_code in ('B04','B05','B06','B07','B08','B09') then '食百'else b.classify_large_name end classify_large_name,
    b.classify_middle_code,
    b.classify_middle_name,
    b.classify_small_name,
    sum(sale_qty) sale_qty,
    sum(sale_cost) sale_cost,
    sum(sale_amt)sale_amt,
    sum(profit) profit,
    is_factory_goods_flag
from      csx_dws.csx_dws_sale_detail_di a
LEFT JOIN (
        SELECT code, name, extra 
        FROM csx_dim.csx_dim_basic_topic_dict_df 
        WHERE parent_code = 'direct_delivery_type'
    ) p ON CAST(a.direct_delivery_type AS STRING) = code
left join 
(select goods_code,
    goods_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_name,
    unit_name
    from  csx_dim.csx_dim_basic_goods 
    where sdt='current'
   
) b on a.goods_code=b.goods_code
  where sdt >=  '20250901' and sdt<='20250930'
    and business_type_code=1
    and extra='采购参与'
    -- and  direct_delivery_type in ('0','11','12','16','17')     -- 日配-采购管理  --剔除 18-委外（供应链指定）
group by  case when b.classify_large_code in ('B04','B05','B06','B07','B08','B09') then '食百'else b.classify_large_name end ,
    b.classify_middle_code,
    b.classify_middle_name,
    business_type_code,
    business_type_name,
    substr(sdt,1,6),
    a.goods_code,
    b.goods_name,
    performance_province_name,
    performance_region_name,
    inventory_dc_city_name,
    b.classify_small_name,
    b.unit_name,
    inventory_dc_name,
    inventory_dc_code,
     is_factory_goods_flag
  ;