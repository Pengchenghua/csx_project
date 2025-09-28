SELECT
  province_name, city_name, warehouse_code, division_code,division_name,
  department_id,department_name,
   product_code, product_name
FROM
(
  SELECT DISTINCT warehouse_code, product_code, product_name
  FROM csx_ods.source_price_r_d_effective_purchase_prices
  WHERE sdt >= '20200411' AND sdt <= '20200417' AND effective = 1 
    AND ( (price_begin_time >= '2020-04-11' AND price_begin_time < '2020-04-18')
      OR (price_end_time >= '2020-04-11' AND price_end_time < '2020-04-18')
      OR(price_begin_time <= '2020-04-11' AND price_end_time > '2020-04-18') )
) a LEFT JOIN
(
  SELECT shop_id, province_name, city_name
  FROM csx_dw.shop_m
  WHERE sdt = 'current'
) b ON a.warehouse_code = b.shop_id
LEFT JOIN
(
  SELECT goods_id, division_code,division_name,department_id,department_name
  FROM csx_dw.goods_m
  WHERE sdt = 'current'
) c ON a.product_code = c.goods_id;



-- 9999
SELECT
  province_name, city_name, warehouse_code, division_code, 
  division_name, product_code, product_name
FROM
(
  SELECT DISTINCT warehouse_code, product_code, product_name, price_begin_time, price_end_time
  FROM csx_ods.source_price_r_d_effective_purchase_prices
  WHERE sdt = '20200421'  AND effective = 1 AND
    price_begin_time >= '2020-04-18' AND price_end_time like '9%'
) a LEFT JOIN
(
  SELECT shop_id, province_name, city_name
  FROM csx_dw.shop_m
  WHERE sdt = 'current'
) b ON a.warehouse_code = b.shop_id
LEFT JOIN
(
  SELECT goods_id, division_code, division_name
  FROM csx_dw.goods_m
  WHERE sdt = 'current'
) c ON a.product_code = c.goods_id;