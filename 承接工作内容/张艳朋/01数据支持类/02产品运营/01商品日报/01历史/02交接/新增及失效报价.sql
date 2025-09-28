-- 0314-0320首次报价商品（生效）
SELECT
  province_name, 
  city_name,
  warehouse_code,
  division_code,
  division_name,
  department_id,
  department_name,
  price_begin_time,
  product_code,
  product_name
FROM
(
  SELECT
    warehouse_code,
    min(price_begin_time) as price_begin_time,
    product_code,
    product_name
  FROM csx_ods.source_price_r_d_effective_purchase_prices
  WHERE sdt = regexp_replace(date_sub(current_date, 1), '-', '') AND effective = 1
  GROUP BY warehouse_code, product_code, product_name
) a LEFT JOIN
(
  SELECT shop_id,province_name,city_name FROM csx_dw.shop_m
  WHERE sdt = 'current'
) b ON a.warehouse_code = b.shop_id	
LEFT JOIN
(
	SELECT goods_id,division_code,division_name,department_id,department_name FROM csx_dw.goods_m
	WHERE sdt = 'current'
) c on a.product_code = c.goods_id
WHERE price_begin_time >= '2020-07-18' AND price_begin_time < ='2020-07-24'


-- 0314-0320失效报价
SELECT
  province_name,
  city_name,  
  division_code,
  division_name,
  department_id,
  department_name,
  warehouse_code,
  price_begin_time,
  product_code,
  product_name
FROM
(
  SELECT
    warehouse_code,
    price_begin_time,
    product_code,
    product_name
  FROM csx_ods.source_price_r_d_effective_purchase_prices
  WHERE sdt = regexp_replace(date_sub(current_date, 1), '-', '') AND effective = 0
    AND price_begin_time >= '2020-07-18' AND price_begin_time < ='2020-07-24'
) a LEFT JOIN
(
  SELECT shop_id, province_name,city_name FROM csx_dw.shop_m
  WHERE sdt = 'current'
) b ON a.warehouse_code = b.shop_id
LEFT JOIN
(
	SELECT goods_id,division_code,division_name,department_id,department_name FROM csx_dw.goods_m
	WHERE sdt = 'current'
) c on a.product_code = c.goods_id;

