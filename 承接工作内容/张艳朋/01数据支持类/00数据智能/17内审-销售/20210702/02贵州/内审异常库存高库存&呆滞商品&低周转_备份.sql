set sdt='20210630';     -- 库存日期
 set dc=('W0A6');        -- 需要稽核DC

-- 1.0 未销售商品明细
drop table if exists csx_tmp.temp_no_sale_goods;
create table csx_tmp.temp_no_sale_goods as 
SELECT a.dist_code,
       a.dist_name,
       a.dc_code,
       a.dc_name,
    --   a.division_code,
    --   a.division_name,
       a.goods_id,
       b.bar_code,
       b.goods_name,
       b.unit_name,
       b.standard,
       classify_middle_code,
       classify_middle_name,
       a.dept_id,
       a.dept_name,
       a.final_qty,
       a.final_amt,
       a.no_sale_days,
       a.max_sale_sdt,
       a.entry_days,
       a.entry_qty,
       a.entry_sdt
FROM csx_tmp.ads_wms_r_d_goods_turnover a
JOIN
  (SELECT goods_id,
          goods_name,
          classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name,
          unit_name,
          standard,
          bar_code
   FROM csx_dw.dws_basic_w_a_csx_product_m
   WHERE sdt='current') b ON a.goods_id=b.goods_id
JOIN
  (SELECT sales_province_code,
          sales_province_name,
          shop_id,
          shop_name
   FROM csx_dw.dws_basic_w_a_csx_shop_m
   WHERE sdt='current'
     AND table_type=1
     AND purpose IN ('01')
     -- AND shop_id in ${hiveconf:dc}
     ) c ON a.dc_code=c.shop_id
WHERE sdt=${hiveconf:sdt}
  AND a.final_qty>a.entry_qty
  AND (
        (category_large_code='1101'
        AND (a.no_sale_days>30 or a.max_sale_sdt='' )
        )
       OR (dept_id IN ('H02',
                       'H03')
           AND (a.no_sale_days>5 or a.max_sale_sdt='' )
           )
       OR (dept_id IN ('H04',
                       'H05',
                       'H06',
                       'H07',
                       'H08',
                       'H09',
                       'H10',
                       'H11')
            AND (a.no_sale_days>30 or a.max_sale_sdt=''  )
         )
       OR (division_code ='12'
            AND ( a.no_sale_days>30 or a.max_sale_sdt='' )
         )
       OR (division_code IN ('13',
                             '14',
                             '15')
           AND (a.no_sale_days>60 or a.max_sale_sdt='' )
         )
      )
  AND final_qty>0 
  and a.final_amt>2000
  AND a.entry_days>30;
  

  
-- 低周转天数商品
drop table if exists csx_tmp.tmp_hight_turn_goods ;
create temporary table  csx_tmp.tmp_hight_turn_goods as 
SELECT a.dist_code,
       a.dist_name,
       a.dc_code,
       a.dc_name,
    --   a.division_code,
    --   a.division_name,
       a.goods_id,
       b.bar_code,
       b.goods_name,
       b.unit_name,
       b.standard,
       classify_middle_code,
       classify_middle_name,
       a.dept_id,
       a.dept_name,
       coalesce(final_amt/final_qty) as cost,
       a.final_qty,
       a.final_amt,
       a.days_turnover_30,
       a.no_sale_days,
       a.max_sale_sdt,
       a.entry_days,
       a.entry_qty,
       a.entry_sdt
FROM csx_tmp.ads_wms_r_d_goods_turnover a
JOIN
  (SELECT goods_id,
          goods_name,
          classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name,
          unit_name,
          standard,
          bar_code
   FROM csx_dw.dws_basic_w_a_csx_product_m
   WHERE sdt='current') b ON a.goods_id=b.goods_id
JOIN
  (SELECT sales_province_code,
          sales_province_name,
          shop_id,
          shop_name
   FROM csx_dw.dws_basic_w_a_csx_shop_m
   WHERE sdt='current'
     AND table_type=1
    AND purpose IN ('01')
    -- and shop_id in ${hiveconf:dc}
    -- AND sales_region_code='3'
    --and sales_province_code='24'   --稽核省区编码
    ) c ON a.dc_code=c.shop_id
WHERE    
    sdt=${hiveconf:sdt}             --更改查询日期
  AND a.final_qty>a.entry_qty
  AND ( (category_large_code='1101' and days_turnover_30>45 AND final_amt>3000)
    or (dept_id in ('H02','H03') and days_turnover_30>5 and a.final_amt>500 )
    OR (dept_id IN ('H04','H05','H06','H07','H08','H09','H10','H11') AND days_turnover_30>15 and a.final_amt>2000) 
    or (division_code ='12' and days_turnover_30>45 and final_amt>2000 )
    or (division_code in ('13','14')  and days_turnover_30>60 and final_amt>3000))
    and final_qty>0
    and a.entry_days>3
    and (a.no_sale_days>7 or no_sale_days='')
  ;
  
-- 关联未销售商品明细，剔除未销售商品
drop table if exists  csx_tmp.tmp_hight_turn_goods_01;

create temporary table csx_tmp.tmp_hight_turn_goods_01 as 
select a.* from csx_tmp.tmp_hight_turn_goods a 
 left join
csx_tmp.temp_no_sale_goos b  on a.dc_code=b.dc_code and a.goods_id=b.goods_id
where b.goods_id is null 
  ;
 
-- 高库存商品
-- 关联未销售商品明细，剔除未销售商品&低周转商品

drop table if exists csx_tmp.tmp_hight_stock ;
create temporary table  csx_tmp.tmp_hight_stock as 
select a.* from 
(SELECT a.dist_code,
       a.dist_name,
       a.dc_code,
       a.dc_name,
    --   a.division_code,
    --   a.division_name,
       a.goods_id,
       b.bar_code,
       b.goods_name,
       b.unit_name,
       b.standard,
       classify_middle_code,
       classify_middle_name,
       a.dept_id,
       a.dept_name,
       coalesce(final_amt/final_qty) as cost,
       a.final_qty,
       a.final_amt,
       a.days_turnover_30,
       a.no_sale_days,
       a.max_sale_sdt,
       a.entry_days,
       a.entry_qty,
       a.entry_sdt
FROM csx_tmp.ads_wms_r_d_goods_turnover a
JOIN
  (SELECT goods_id,
          goods_name,
          classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name,
          unit_name,
          standard,
          bar_code
   FROM csx_dw.dws_basic_w_a_csx_product_m
   WHERE sdt='current') b ON a.goods_id=b.goods_id
JOIN
  (SELECT sales_province_code,
          sales_province_name,
          shop_id,
          shop_name
   FROM csx_dw.dws_basic_w_a_csx_shop_m
   WHERE sdt='current'
     AND table_type=1
    AND purpose IN ('01')
    -- and shop_id in ${hiveconf:dc}
    -- AND sales_region_code='3'
    --and sales_province_code='24'   --稽核省区编码
    ) c ON a.dc_code=c.shop_id
WHERE    
    sdt=${hiveconf:sdt}             --更改查询日期
    )a 
    left join 
    csx_tmp.temp_no_sale_goos b  on a.dc_code=b.dc_code and a.goods_id=b.goods_id
    left join 
    csx_tmp.tmp_hight_turn_goods c  on a.dc_code=c.dc_code and a.goods_id=c.goods_id
    where b.goods_id is null and c.goods_id is null 
        and a.final_amt>5000
  order by a.final_amt desc 
 
  ;
  
-- 呆滞商品明细
  select * from  csx_tmp.temp_no_sale_goods where 1=1 and dc_code in #{hiveconf:dc};
-- 低周转商品明细
  select * from  csx_tmp.tmp_hight_turn_goods_01 where 1=1 and dc_code in #{hiveconf:dc};

-- 高库存商品明细
  select * from  csx_tmp.tmp_hight_stock where 1=1 and dc_code in #{hiveconf:dc};
    
     
  

  
       
  
  --统计数据导出
  select sales_province_code,
          sales_province_name,
          city_group_code,
          city_group_name,
          dc_code,
         dc_name,
        business_division_code,
        business_division_name, 
        sum(t1.final_amt)/10000 final_amt ,
        sum(period_inv_amt_30day)/10000 period_inv_amt_30day,
        sum(cost_30day)/10000 as cost_30day,
        sum(period_inv_amt_30day)/sum(cost_30day) as turn_days,
        count(distinct t1.goods_id) as stock_sku,
        count(distinct t2.goods_id) as no_sale_sku,
        sum(t2.final_amt)/10000 no_sale_final_amt ,
        count(distinct t3.goods_id) as turn_sku,
        sum(t3.final_amt)/10000 turn_final_amt ,
        count(distinct t4.goods_id) as hight_sku,
        sum(t4.final_amt)/10000 hight_final_amt ,
        grouping__id
  from 
  (select sales_province_code,
          sales_province_name,
          city_group_code,
          city_group_name,
         dc_code,
         dc_name,
         business_division_code,
         business_division_name, 
         final_amt ,
         cost_30day,
         period_inv_amt_30day,
         goods_id
    from csx_tmp.ads_wms_r_d_goods_turnover  a
    
    join 
     (SELECT sales_province_code,
          sales_province_name,
          city_group_code,
          city_group_name,
          shop_id,
          shop_name
   FROM csx_dw.dws_basic_w_a_csx_shop_m
   WHERE sdt='current'
     AND table_type=1
    AND purpose IN ('01')
    -- and shop_id in ${hiveconf:dc}
    -- AND sales_region_code='3'
    --and sales_province_code='24'   --稽核省区编码
    ) c ON a.dc_code=c.shop_id
  where sdt=${hiveconf:sdt}
   and a.final_qty>0
  ) t1
  left join 
  -- 未销售查询
   csx_tmp.temp_no_sale_goods as t2 on t1.dc_code=t2.dc_code and t1.goods_id=t2.goods_id 
  left join
  --低周转库存
   csx_tmp.tmp_hight_turn_goods_01 t3 on t1.dc_code=t3.dc_code and t1.goods_id=t3.goods_id 
  left join
  --高库存
   csx_tmp.tmp_hight_stock t4 on t1.dc_code=t4.dc_code and t1.goods_id=t4.goods_id 
   group by t1.dc_code,
            t1.dc_name,
        sales_province_code,
          sales_province_name,
          city_group_code,
          city_group_name,
          business_division_code,
         business_division_name
    grouping sets ((sales_province_code,
          sales_province_name,
          city_group_code,
          city_group_name,
          t1.dc_code,
         t1.dc_name,
          business_division_code,
         business_division_name),
         (sales_province_code,
          sales_province_name,
          city_group_code,
          city_group_name,
          business_division_code,
         business_division_name),
         (sales_province_code,
          sales_province_name,
          business_division_code,
         business_division_name));