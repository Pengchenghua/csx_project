
--供应链教育客户每月商品销售情况
select * from  csx_analyse_tmp.csx_analyse_tmp_sale_detial
where month_of_year>'202506'


create table csx_analyse_tmp.csx_analyse_tmp_sale_detial as 
WITH date_info AS (
    SELECT 
        calday, 
        month_of_year, 
        csx_week_begin, 
        csx_week_end,
        concat_ws('-', csx_week_begin, csx_week_end) AS csx_week_begin_end
    FROM csx_dim.csx_dim_basic_date AS d 
    WHERE d.calday >= '20250401' AND d.calday <= '20250729'
),
customer_info AS (
    SELECT 
        customer_code,
        customer_name,
        second_category_name ,
        third_category_name,
    FROM csx_dim.csx_dim_crm_customer_info 
    WHERE sdt = 'current'
        AND second_category_name = '教育'
),
goods_info AS (
    SELECT 
        goods_code,
        goods_name,
        goods_bar_code,
        brand_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name
    FROM csx_dim.csx_dim_basic_goods
    WHERE sdt = 'current'
),
dict_info AS (
    SELECT 
        code, 
        name, 
        extra 
    FROM csx_dim.csx_dim_basic_topic_dict_df 
    WHERE parent_code = 'direct_delivery_type'
)
SELECT 
    dd.month_of_year,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name,
    a.customer_code,
    f.customer_name,
    f.second_category_name,
    a.goods_code,
    m.goods_name,
    m.goods_bar_code,
    m.brand_name,
    a.unit_name,
    m.classify_large_code,
    m.classify_large_name,
    m.classify_middle_code,
    m.classify_middle_name,
    m.classify_small_code,
    m.classify_small_name,
    SUM(a.sale_amt) AS sale_amt,
    SUM(a.profit) AS profit,
    CASE 
        WHEN SUM(a.sale_amt) = 0 THEN 0 
        ELSE SUM(a.profit) / SUM(a.sale_amt) 
    END AS gross_margin
FROM csx_dws.csx_dws_sale_detail_di a
INNER JOIN customer_info f 
    ON a.customer_code = f.customer_code
LEFT JOIN dict_info p 
    ON CAST(a.direct_delivery_type AS STRING) = p.code
LEFT JOIN date_info dd 
    ON a.sdt = dd.calday
INNER JOIN goods_info m 
    ON a.goods_code = m.goods_code
WHERE a.sdt >= '20250101' 
    AND a.sdt <= '20251209'
    AND a.business_type_code = '1' 
    AND p.extra = '采购参与'
GROUP BY 
    dd.month_of_year,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name,
    a.customer_code,
    f.customer_name,
    f.second_category_name,
    a.goods_code,
    m.goods_name,
    m.goods_bar_code,
    m.brand_name,
    a.unit_name,
    m.classify_large_code,
    m.classify_large_name,
    m.classify_middle_code,
    m.classify_middle_name,
    m.classify_small_code,
    m.classify_small_name
    ;


    --供应链教育客户每月商品销售情况
select
--   month_of_year,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  a.customer_code,
  a.customer_name,
  a.second_category_name,
  b.third_category_name,
  goods_code,
  goods_name,
  goods_bar_code,
  brand_name,
  unit_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  classify_small_code,
  classify_small_name,
  sum(sale_qty ) sale_qty,
  sum(sale_amt) sale_amt,
  sum(profit) profit,
  sum(profit)/ sum(sale_amt)  gross_margin
from
  csx_analyse_tmp.csx_analyse_tmp_sale_detial a 
  left join 
  (
    SELECT 
        customer_code,
        customer_name,
        second_category_name ,
        third_category_name
    FROM csx_dim.csx_dim_crm_customer_info 
    WHERE sdt = 'current'
        -- AND second_category_name = '教育'
) b on a.customer_code=b.customer_code
group by   performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  a.customer_code,
  a.customer_name,
  a.second_category_name,
  b.third_category_name,
  goods_code,
  goods_name,
  goods_bar_code,
  brand_name,
  unit_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  classify_small_code,
  classify_small_name