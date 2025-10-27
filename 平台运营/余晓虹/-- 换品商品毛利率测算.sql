-- A 商品销售数量、销售金额，销售成本、毛利额、毛利率
WITH tmp_A_sale_goods AS (
  SELECT 
    a.customer_code,
    a.goods_code,
    a.performance_city_name,
    a.inventory_dc_code,
    a.goods_name,
    a.unit_name,
    a.classify_middle_name,
    SUM(a.sale_qty) AS sale_qty,
    SUM(a.sale_amt) AS sale_amt,
    sum(sale_cost) sale_cost,
    SUM(a.profit) AS profit
  FROM csx_dws.csx_dws_sale_detail_di a
  LEFT JOIN (
    SELECT
      code,
      name,
      extra
    FROM csx_dim.csx_dim_basic_topic_dict_df
    WHERE parent_code = 'direct_delivery_type'
  ) p ON CAST(a.direct_delivery_type AS STRING) = p.code
  WHERE a.sdt >= '20250926'
    AND a.performance_province_name = '广东深圳'
    AND a.business_type_code = 1
    AND p.extra = '采购参与'
  GROUP BY 
     a.goods_code,
    a.performance_city_name,
    a.inventory_dc_code,
    a.goods_name,
    a.unit_name,
    a.classify_middle_name,
    customer_code
),

-- B 商品入库成本
tmp_B_entry_goods AS (
  SELECT 
    a.goods_code,
    a.goods_name,
    a.unit_name,
    a.classify_middle_name,
    SUM(a.receive_qty) AS receive_qty,
    SUM(a.receive_amt) AS receive_amt
  FROM csx_analyse.csx_analyse_scm_purchase_order_flow_di a
  JOIN csx_analyse_tmp.csx_analyse_tmp_goods_link b 
    ON a.goods_code = b.goods_code_b
  WHERE a.sdt >= regexp_replace(date_add('2025-10-26', -30), '-', '')
    AND a.sdt <= regexp_replace('2025-10-26', '-', '')
    AND a.assign_type <> '1' -- 剔除客户指定数据
    AND a.dc_code IN ('W0BK', 'W0BJ')
    AND a.price_type <> '2'
    AND a.source_type_code IN ('1', '10', '19', '23', '9')
    AND a.remedy_flag <> '1'
  GROUP BY 
    a.goods_code,
    a.goods_name,
    a.unit_name,
    a.classify_middle_name
),

-- 主查询
final_result AS (
  SELECT 
    m.performance_region_name,
    m.performance_province_name,
    m.performance_city_name,
    a.customer_code,
    m.customer_name,
    m.link_customer_code,
    m.link_customer_name,
    a.goods_code,
    a.goods_name,
    a.unit_name,
    a.classify_middle_name,
    a.sale_qty,
    sale_cost,
    a.sale_amt,
    profit,
    profit/sale_amt profit_rate,
    m.goods_code_b,
    m.goods_name_b,
    e.receive_amt / e.receive_qty AS cost, -- B 商品成本
    t4.customer_price
  FROM tmp_A_sale_goods a
  JOIN csx_analyse_tmp.tmp_customer_config m 
    ON a.customer_code = m.link_customer_code 
    AND a.goods_code = m.goods_code
  LEFT JOIN (
    -- 主客户商品 B 报价
    SELECT 
      warehouse_code,
      customer_code,
      product_code,
      MAX(customer_price) AS customer_price
    FROM csx_dwd.csx_dwd_price_customer_price_guide_di
    WHERE price_begin_time <= CURRENT_DATE
      AND price_end_time >= CURRENT_DATE
      AND (sub_customer_code IS NULL OR sub_customer_code = '')
      AND warehouse_code = 'W0BK'
    GROUP BY 
      warehouse_code,
      customer_code,
      product_code
  ) t4 ON m.customer_no = t4.customer_code 
       AND m.goods_code_b = t4.product_code
  LEFT JOIN tmp_B_entry_goods e 
    ON m.goods_code_b = e.goods_code
)

-- 输出最终结果
SELECT *
FROM final_result;


 -- 关联子客户商品b报价
    LEFT JOIN (
        SELECT 
            warehouse_code,
            customer_code,
            sub_customer_code,
            product_code,
            MAX(customer_price) AS customer_price  
        FROM csx_dwd.csx_dwd_price_customer_price_guide_di
        WHERE price_begin_time <= CURRENT_DATE 
            AND price_end_time >= CURRENT_DATE 
            AND sub_customer_code IS NOT NULL 
            AND sub_customer_code != ''
        GROUP BY 
            warehouse_code,
            customer_code,
            sub_customer_code,
            product_code
    ) t5 
        ON a.inventory_dc_code = t5.warehouse_code 
        AND a.customer_code = t5.customer_code 
        AND a.sub_customer_code = t5.sub_customer_code 
        AND a.goods_code_b = t5.product_code
