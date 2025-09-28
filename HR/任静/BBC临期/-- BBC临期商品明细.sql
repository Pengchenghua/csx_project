-- BBC临期商品明细
select
 performance_region_code
,performance_region_name
,performance_province_code
,performance_province_name
,performance_city_code
,performance_city_name
,company_code
,company_name
,dc_code
,dc_name
,business_division_name
,detail_business_division_code
,detail_business_division_name
,purchase_group_code
,purchase_group_name
,classify_large_code
,classify_large_name
,classify_middle_code
,classify_middle_name
,classify_small_code
,classify_small_name
,goods_code
,goods_name
,stock_batch_id
,product_source
,store_location_code
,production_date
,receive_date
,expiry_date
,shelf_life
,product_receiving
,product_storage
,product_deliver
,is_expiry
,is_receiving
,is_storage
,is_deliver
,validity_type
,expiry_diff
,price
,price_no_tax
,period_stock_qty
,period_stock_amt
,period_stock_amt_no_tax
,expiry_stock_qty
,expiry_stock_amt
,expiry_stock_amt_no_tax
,imperfect_stock_qty
,imperfect_stock_amt
,imperfect_stock_amt_no_tax
,stock_qty
,stock_amt
,stock_amt_no_tax
,is_period_expiry_flag
,is_imperfect_flag
,sdt

from
  csx_report.csx_report_wms_period_imperfect_goods_stock_df  
where
  sdt = '20250228'
  and dc_code in (
    'W0H2',
    'WB62',
    'W0S6',
    'W0Q6',
    'W0BA',
    'WC58',
    'W0N9',
    'WB96',
    'W0S7',
    'W0P9',
    'W0Z5',
    'W0B6',
    'WA92',
    'WB46',
    'WB33',
    'WC79',
    'W0R2',
    'WD20',
    'W0M9',
    'W0X3',
    'W0AB',
    'W0K8',
    'W0S4',
    'W0G8',
    'W0BE',
    'WB58',
    'W0BV'
  )





-- BBC临期商品明细
select
--  performance_region_code
,performance_region_name
-- ,performance_province_code
,performance_province_name
-- ,performance_city_code
,performance_city_name
-- ,company_code
-- ,company_name
,dc_code
,dc_name
,business_division_name
-- ,detail_business_division_code
-- ,detail_business_division_name
-- ,purchase_group_code
-- ,purchase_group_name
-- ,classify_large_code
,classify_large_name
-- ,classify_middle_code
,classify_middle_name
-- ,classify_small_code
-- ,classify_small_name
,goods_code
,goods_name
,stock_batch_id
-- ,product_source
-- ,store_location_code
,production_date
,receive_date
,expiry_date
,shelf_life
,product_receiving
,product_storage
,product_deliver
,is_expiry
,is_receiving
,is_storage
,is_deliver
,validity_type
,expiry_diff
,price
,price_no_tax
,period_stock_qty
,period_stock_amt
,period_stock_amt_no_tax
,expiry_stock_qty
,expiry_stock_amt
,expiry_stock_amt_no_tax
,imperfect_stock_qty
,imperfect_stock_amt
,imperfect_stock_amt_no_tax
,stock_qty
,stock_amt
,stock_amt_no_tax
,if(is_period_expiry_flag=1,"是","否") is_period_expiry_flag
,if(is_imperfect_flag =1,"是","否") is_imperfect_flag
,sdt

from
  csx_report.csx_report_wms_period_imperfect_goods_stock_df  
where
  sdt = '20250228'
  and dc_code in (
    'W0H2',
    'WB62',
    'W0S6',
    'W0Q6',
    'W0BA',
    'WC58',
    'W0N9',
    'WB96',
    'W0S7',
    'W0P9',
    'W0Z5',
    'W0B6',
    'WA92',
    'WB46',
    'WB33',
    'WC79',
    'W0R2',
    'WD20',
    'W0M9',
    'W0X3',
    'W0AB',
    'W0K8',
    'W0S4',
    'W0G8',
    'W0BE',
    'WB58',
    'W0BV'
  )

-- 福利销售及费用绩效明细
  http://fr.csxdata.cn/webroot/decision/view/report?viewlet=HR%252F%25E7%25BB%25A9%25E6%2595%2588%25E4%25B8%2593%25E9%25A2%2598%252F%25E7%25A6%258F%25E5%2588%25A9%25E9%2594%2580%25E5%2594%25AE_%25E5%2590%258E%25E5%258F%25B0%25E8%25B4%25B9%25E7%2594%25A8.cpt&ref_t=design&op=write&__cutpage__=v&ref_c=df21e670-fddb-4333-8f96-5a3c11bc30ab

-- 供应链销售及后台费用绩效明细
http://fr.csxdata.cn/webroot/decision/view/report?viewlet=HR%252F%25E7%25BB%25A9%25E6%2595%2588%25E4%25B8%2593%25E9%25A2%2598%252F%25E4%25BE%259B%25E5%25BA%2594%25E9%2593%25BE%25E9%2594%2580%25E5%2594%25AE%25E5%2590%258E%25E5%258F%25B0%25E8%25B4%25B9%25E7%2594%25A8%25E7%25BB%25A9%25E6%2595%2588%25E6%2598%258E%25E7%25BB%2586.cpt&ref_t=design&op=write&__cutpage__=v&ref_c=df21e670-fddb-4333-8f96-5a3c11bc30ab