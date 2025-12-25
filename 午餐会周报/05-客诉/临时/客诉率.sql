WITH sale_count AS (
    -- 签收
    SELECT
        performance_region_name,
        performance_province_name,
        performance_city_name,
        customer_code,
        COUNT(goods_code) AS sale_cnt_bq  -- 签收SKU
    FROM csx_dws.csx_dws_sale_detail_di
    WHERE sdt >= '20251020' 
        AND sdt <= '20251026' 
        AND channel_code IN ('1','7','9') 
        AND business_type_code NOT IN (4,6) -- 业务类型编码
        AND order_channel_code = 1 -- 1-b端
        AND refund_order_flag = 0  -- 剔除退货
        AND shipper_code = 'YHCSX'
    GROUP BY 
        performance_region_name,
        performance_province_name,
        performance_city_name,
        customer_code
),

complaint_data AS (
    SELECT 
        week_of_year,
        week_range,
        performance_region_name,
        performance_province_name,
        performance_city_name,
        customer_code,
        customer_name,
        COUNT(DISTINCT complaint_code) AS goods_num  -- 客诉条数
    FROM csx_analyse.csx_analyse_fr_complaint_month_report_mf 
    WHERE month = '202510' 
        AND week_of_year = '202543'
    GROUP BY 
        week_of_year,
        week_range,
        performance_region_name,
        performance_province_name,
        performance_city_name,
        customer_code,
        customer_name
)

SELECT 
    a.*,
    b.sale_cnt_bq,
    c.customer_large_level
FROM complaint_data a
LEFT JOIN sale_count b 
    ON a.customer_code = b.customer_code
    AND a.performance_region_name = b.performance_region_name
    AND a.performance_province_name = b.performance_province_name
    AND a.performance_city_name = b.performance_city_name
LEFT JOIN (
    -- 客户等级
    SELECT 
        customer_no,
        customer_large_level
    FROM csx_analyse.csx_analyse_report_sale_customer_level_mf 
    WHERE month = '202510'
        AND tag = 1 -- 数据标识：1：全量数据；2：剔除不统计业绩仓数据
) c ON c.customer_no = a.customer_code;