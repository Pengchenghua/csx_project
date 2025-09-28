 -- 非直送仓毛利率供应商情况
 -- drop table csx_analyse_tmp.csx_analyse_tmp_zs_sale;
create table csx_analyse_tmp.csx_analyse_tmp_zs_sale as 
with aa as ( select
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
   sale_time,
   a.original_order_code,
   a.order_code,
   delivery_type_name,
   customer_code,
   customer_name,
   second_category_name,
   sales_user_name,
   inventory_dc_code,
   inventory_dc_name,
   a.goods_code,
   goods_name,
   unit_name,
   classify_large_code,
   classify_large_name,
   classify_middle_code,
   classify_middle_name,
   classify_small_code,
   classify_small_name,
   supplier_code,
   supplier_name,
   cost_price,
   sale_price,
   purchase_qty,
   sale_qty,
   sale_amt,
   sale_cost,
   profit,
   wms_biz_type_name,
   csx_week_begin,
   csx_week_end,
   csx_week	,
   month_of_year,
   shop_low_profit_flag,
   direct_delivery_type_name,
   coalesce(a.purchase_order_code,b.purchase_order_code) purchase_order_code,
   coalesce(a.source_type_name,b.source_type_name) source_type_name,
   wms_order_code
from 
  (select
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
   sale_time,
   a.original_order_code,
   order_code,
   delivery_type_name,
   customer_code,
   customer_name,
   second_category_name,
   sales_user_name,
   inventory_dc_code,
   inventory_dc_name,
   goods_code,
   goods_name,
   unit_name,
   classify_large_code,
   classify_large_name,
   classify_middle_code,
   classify_middle_name,
   classify_small_code,
   classify_small_name,
   supplier_code,
   supplier_name,
   cost_price,
   sale_price,
   purchase_qty,
   sale_qty,
   sale_amt,
   sale_cost,
   profit,
   wms_biz_type_name,
   csx_week_begin,
   csx_week_end,
   csx_week	,
   month_of_year,
   b.shop_low_profit_flag,
   direct_delivery_type_name,
   purchase_order_code ,
   source_type_code	,
   source_type_name,
   wms_order_code
from
      csx_analyse.csx_analyse_bi_sale_detail_di a 
  join (select shop_code,shop_low_profit_flag from csx_dim.csx_dim_shop where sdt='current' and shop_low_profit_flag=1) b on a.inventory_dc_code=b.shop_code
where
  sdt >= '20230801'
  and sdt <= '20231024'
  and channel_code in('1', '7', '9','11','12')
  and business_type_code in (1)
  and order_channel_detail_code not in (27,26,25)
  and inventory_dc_code not in ('W0BQ','W0AX','W0BD','W0T0','W0AJ','W0G6','WB71','W0J2')
 -- and coalesce(purchase_order_code,'') !=''
  )a 
  left join 
  (select
  original_order_code,
  order_code,
  goods_code,
  purchase_order_code,
  source_type_name
from
    csx_analyse.csx_analyse_bi_sale_detail_di a 
where
  sdt >= '20210101'
  and sdt <= '20231024'
  and channel_code in('1', '7', '9','11','12')
  and business_type_code in (1)
  and inventory_dc_code not in ('W0BQ','W0AX','W0BD','W0T0','W0AJ','W0G6','WB71','W0J2')
  group by original_order_code,
  order_code,
  goods_code,
  purchase_order_code,
  source_type_name
  ) b on a.original_order_code=b.order_code and a.goods_code=b.goods_code
  
  where 1=1 
  ) ,
  bb as (select supplier_code,supplier_name,order_code,goods_code from csx_dws.csx_dws_scm_order_detail_di 
  where sdt>='20220101' group by  supplier_code,supplier_name,order_code,goods_code )  
   select
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
   sale_time,
   original_order_code,
   aa.order_code,
   delivery_type_name,
   customer_code,
   customer_name,
   second_category_name,
   sales_user_name,
   inventory_dc_code,
   inventory_dc_name,
   aa.goods_code,
   goods_name,
   unit_name,
   classify_large_code,
   classify_large_name,
   classify_middle_code,
   classify_middle_name,
   classify_small_code,
   classify_small_name,
   bb.supplier_code,
   bb.supplier_name,
   cost_price,
   sale_price,
   purchase_qty,
   sale_qty,
   sale_amt,
   sale_cost,
   profit,
   wms_biz_type_name,
   csx_week_begin,
   csx_week_end,
   csx_week	,
   month_of_year,
   shop_low_profit_flag,
   direct_delivery_type_name,
   purchase_order_code,
   source_type_name,
   wms_order_code
from  aa 
    left join bb on aa.purchase_order_code=bb.order_code and aa.goods_code=bb.goods_code 



    -- select * from  csx_analyse.csx_analyse_bi_sale_detail_di  where original_order_code='OM22120900003089';
select 
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
   delivery_type_name,
   customer_code,
   customer_name,
   second_category_name,
  -- sales_user_name,
   supplier_code,
   supplier_name,
   direct_delivery_type_name,
  sum(coalesce(case when month_of_year='202308' then sale_amt end ,0)) sale_amt_08,
  sum(coalesce(case when month_of_year='202309' then sale_amt end ,0)) sale_amt_09,
  sum(coalesce(case when month_of_year='202310' then sale_amt end ,0)) sale_amt_10,
  sum(coalesce(case when month_of_year='202308' then profit end ,0)) profit_08, 
  sum(coalesce(case when month_of_year='202309' then profit end ,0)) profit_09,
  sum(coalesce(case when month_of_year='202310' then profit end ,0)) profit_10,
   sum(sale_qty )sale_qty ,
   sum(sale_amt )sale_amt ,
   sum(sale_cost)sale_cost,
   sum(profit) profit
from     csx_analyse_tmp.csx_analyse_tmp_zs_sale aa
group by 
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
   delivery_type_name,
   customer_code,
   customer_name,
   second_category_name,
 --  sales_user_name,
   supplier_code,
   supplier_name,
   direct_delivery_type_name
;



-- 物流直送销售分析，非直送仓
  drop table csx_analyse_tmp.csx_analyse_tmp_nzs_sale;
create table csx_analyse_tmp.csx_analyse_tmp_nzs_sale as 
with aa as ( select
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  delivery_type_name,
   sale_time,
   a.original_order_code,
   a.order_code,
   delivery_type_name,
   customer_code,
   customer_name,
   second_category_name,
   sales_user_name,
   inventory_dc_code,
   inventory_dc_name,
   a.goods_code,
   goods_name,
   unit_name,
   classify_large_code,
   classify_large_name,
   classify_middle_code,
   classify_middle_name,
   classify_small_code,
   classify_small_name,
   supplier_code,
   supplier_name,
   cost_price,
   sale_price,
   purchase_qty,
   sale_qty,
   sale_amt,
   sale_cost,
   profit,
   wms_biz_type_name,
   csx_week_begin,
   csx_week_end,
   csx_week	,
   month_of_year,
   shop_low_profit_flag,
   direct_delivery_type_name,
   coalesce(a.purchase_order_code,b.purchase_order_code) purchase_order_code,
   coalesce(a.source_type_name,b.source_type_name) source_type_name,
   wms_order_code
from 
  (select
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  delivery_type_name,
   sale_time,
   a.original_order_code,
   order_code,
   delivery_type_name,
   customer_code,
   customer_name,
   second_category_name,
   sales_user_name,
   inventory_dc_code,
   inventory_dc_name,
   goods_code,
   goods_name,
   unit_name,
   classify_large_code,
   classify_large_name,
   classify_middle_code,
   classify_middle_name,
   classify_small_code,
   classify_small_name,
   supplier_code,
   supplier_name,
   cost_price,
   sale_price,
   purchase_qty,
   sale_qty,
   sale_amt,
   sale_cost,
   profit,
   wms_biz_type_name,
   csx_week_begin,
   csx_week_end,
   csx_week	,
   month_of_year,
   b.shop_low_profit_flag,
   direct_delivery_type_name,
   purchase_order_code ,
   source_type_code	,
   source_type_name,
   wms_order_code
from
       csx_analyse.csx_analyse_bi_sale_detail_di a 
  join (select shop_code,shop_low_profit_flag from csx_dim.csx_dim_shop where sdt='current' and shop_low_profit_flag=0) b on a.inventory_dc_code=b.shop_code
where
  sdt >= '20230901'
  and sdt <= '20230930'
  and channel_code in('1', '7', '9','11','12')
  and business_type_code in (1)
  and  delivery_type_code = '2'
 -- and order_channel_detail_code not in (27,26,25)
  and inventory_dc_code not in ('W0BQ','W0AX','W0BD','W0T0','W0AJ','W0G6','WB71','W0J2')
 -- and coalesce(purchase_order_code,'') !=''
  )a 
  left join 
  (select
  original_order_code,
  order_code,
  goods_code,
  purchase_order_code,
  source_type_name
from
    csx_analyse.csx_analyse_bi_sale_detail_di a 
where
  sdt >= '20210101'
  and sdt <= '20231024'
  and channel_code in('1', '7', '9','11','12')
  and business_type_code in (1)
  and inventory_dc_code not in ('W0BQ','W0AX','W0BD','W0T0','W0AJ','W0G6','WB71','W0J2')
  group by original_order_code,
  order_code,
  goods_code,
  purchase_order_code,
  source_type_name
  ) b on a.original_order_code=b.order_code and a.goods_code=b.goods_code
  
  where 1=1 
  ) ,
  bb as (select supplier_code,supplier_name,order_code,goods_code from csx_dws.csx_dws_scm_order_detail_di 
  where sdt>='20220101' group by  supplier_code,supplier_name,order_code,goods_code )  
   select
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
   sale_time,
   original_order_code,
   aa.order_code,
   delivery_type_name,
   customer_code,
   customer_name,
   second_category_name,
   sales_user_name,
   inventory_dc_code,
   inventory_dc_name,
   aa.goods_code,
   goods_name,
   unit_name,
   classify_large_code,
   classify_large_name,
   classify_middle_code,
   classify_middle_name,
   classify_small_code,
   classify_small_name,
   bb.supplier_code,
   bb.supplier_name,
   cost_price,
   sale_price,
   purchase_qty,
   sale_qty,
   sale_amt,
   sale_cost,
   profit,
   wms_biz_type_name,
   csx_week_begin,
   csx_week_end,
   csx_week	,
   month_of_year,
   shop_low_profit_flag,
   direct_delivery_type_name,
   purchase_order_code,
   source_type_name,
   wms_order_code
from  aa 
    left join bb on aa.purchase_order_code=bb.order_code and aa.goods_code=bb.goods_code 
;


    -- select * from  csx_analyse.csx_analyse_bi_sale_detail_di  where original_order_code='OM22120900003089';
select 
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
   delivery_type_name,
   inventory_dc_code,
   customer_code,
   customer_name,
   inventory_dc_code,
   second_category_name,
  -- sales_user_name,
   supplier_code,
   supplier_name,
   direct_delivery_type_name,
   a.goods_code,
   goods_name,
   unit_name,
   classify_large_code,
   classify_large_name,
   classify_middle_code,
   classify_middle_name,
   classify_small_code,
   classify_small_name,
   source_type_name,
--   sum(coalesce(case when month_of_year='202308' then sale_amt end ,0)) sale_amt_08,
--   sum(coalesce(case when month_of_year='202309' then sale_amt end ,0)) sale_amt_09,
--   sum(coalesce(case when month_of_year='202310' then sale_amt end ,0)) sale_amt_10,
--   sum(coalesce(case when month_of_year='202308' then profit end ,0)) profit_08, 
--   sum(coalesce(case when month_of_year='202309' then profit end ,0)) profit_09,
--   sum(coalesce(case when month_of_year='202310' then profit end ,0)) profit_10,
   sum(sale_qty )sale_qty ,
   sum(sale_amt )sale_amt ,
   sum(sale_cost)sale_cost,
   sum(profit) profit
from     csx_analyse_tmp.csx_analyse_tmp_nzs_sale a
group by 
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
   delivery_type_name,
   inventory_dc_code,
   customer_code,
   customer_name,
   inventory_dc_code,
   second_category_name,
  -- sales_user_name,
   supplier_code,
   supplier_name,
   direct_delivery_type_name,
   a.goods_code,
   goods_name,
   unit_name,
   classify_large_code,
   classify_large_name,
   classify_middle_code,
   classify_middle_name,
   classify_small_code,
   source_type_name,
   classify_small_name