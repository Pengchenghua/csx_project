-- 采购福利BBC绩效
-- 销售明细
with sale as (
  select
    performance_region_name,
    performance_province_name,
    performance_city_name,
    a.business_type_code,
    a.business_type_name,
    a.customer_code,
    customer_name,
    -- b.classify_large_code,
    b.classify_large_name,
    -- b.classify_middle_code,
    b.classify_middle_name,
    -- b.classify_small_code,
    b.classify_small_name,
    sum(
      if(
        sdt >= '20250101'
        and sdt <= '20250131',
        sale_amt,
        0
      )
    ) bq_sale_amt,
    sum(
      if(
        sdt >= '20250101'
        and sdt <= '20250131',
        profit,
        0
      )
    ) bq_profit,
    sum(
      if(
        sdt >= '20240101'
        and sdt <= '20240131',
        sale_amt,
        0
      )
    ) hq_sale_amt,
    sum(
      if(
        sdt >= '20240101'
        and sdt <= '20240131',
        profit,
        0
      )
    ) hq_profit
  from
    csx_dws.csx_dws_sale_detail_di a
    left join (
      select
        goods_code,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name
      from
        csx_dim.csx_dim_basic_goods
      where
        sdt = 'current'
    ) b on a.goods_code = b.goods_code
  where
    a.business_type_code in ('2', '6') -- 2-福利、6-BBC
    and (
      (
        sdt >= '20250101'
        and sdt <= '20250131'
      )
      or (
        sdt >= '20240101'
        and sdt <= '20240131'
      )
    )
  group by
    performance_province_name,
    performance_region_name,
    performance_city_name,
    a.business_type_code,
    a.customer_code,
    customer_name,
    sales_user_name,
    sales_user_number,
    sales_user_position,
    business_type_name,
    -- b.classify_large_code,
    b.classify_large_name,
    -- b.classify_middle_code,
    b.classify_middle_name,
    -- b.classify_small_code,
    b.classify_small_name
)
select
  *
from
  sale
;

-- 后台毛利
with tmp_bq_back as 
(select belong_province_code,
    belong_province_name,
    belong_city_code,
    belong_city_name,
case when substr(purchase_group_code,1,1) IN ('A','P') THEN '食百'
    when purchase_group_code in ('H01','H09','H10','H11','H90') or substr( purchase_group_code,1,1)='U' then '干货加工'
    when purchase_group_code in ('H04','H05','H06','H07','H08') then '肉禽水产'
    when purchase_group_code in ('H02','H03') then '蔬菜水果'
    end classify_large_name,
    purchase_group_name,
  sum(value_tax_total) back_profit
from
     csx_dwd.csx_dwd_pss_settle_settle_bill_di
where
  (  settlement_dc_name   like '%福利%'
  or settlement_dc_name   like '%BBC%'
  )
  and sdt>='20250101'  
  -- 增加归属日期
  and belong_date >= '2025-02-01' and belong_date <'2025-03-01'
group by
 case when substr(purchase_group_code,1,1) IN ('A','P') THEN '食百'
    when purchase_group_code in ('H01','H09','H10','H11','H90') or substr( purchase_group_code,1,1)='U' then '干货加工'
    when purchase_group_code in ('H04','H05','H06','H07','H08') then '肉禽水产'
    when purchase_group_code in ('H02','H03') then '蔬菜水果'
    end,
     belong_province_code,
    belong_province_name,
    belong_city_code,
    belong_city_name,
    purchase_group_name
    ),
tmp_hq_back as 
(select belong_province_code,
    belong_province_name,
    belong_city_code,
    belong_city_name,
case when substr(purchase_group_code,1,1) IN ('A','P') THEN '食百'
    when purchase_group_code in ('H01','H09','H10','H11','H90') or substr( purchase_group_code,1,1)='U' then '干货加工'
    when purchase_group_code in ('H04','H05','H06','H07','H08') then '肉禽水产'
    when purchase_group_code in ('H02','H03') then '蔬菜水果'
    end classify_large_name,
    purchase_group_name,
  sum(value_tax_total) back_profit
from
       csx_dwd.csx_dwd_pss_settle_settle_bill_di
where
  (  settlement_dc_name   like '%福利%'
  or settlement_dc_name   like '%BBC%'
  )
  and sdt>='20240101'  and sdt<='20240331'  
  -- 增加归属日期
  and belong_date >= '2024-02-01' and belong_date <'2024-03-01'
group by
 case when substr(purchase_group_code,1,1) IN ('A','P') THEN '食百'
    when purchase_group_code in ('H01','H09','H10','H11','H90') or substr( purchase_group_code,1,1)='U' then '干货加工'
    when purchase_group_code in ('H04','H05','H06','H07','H08') then '肉禽水产'
    when purchase_group_code in ('H02','H03') then '蔬菜水果'
    end,
     belong_province_code,
    belong_province_name,
    belong_city_code,
    belong_city_name,
    purchase_group_name
    )
    select  belong_province_code,	belong_province_name,	belong_city_code,	belong_city_name,	classify_large_name,purchase_group_name,sum(back_profit)	back_profit,sum(hq_back_profit) hq_back_profit from (
    select belong_province_code,	belong_province_name,	belong_city_code,	belong_city_name,	classify_large_name,purchase_group_name,	back_profit,0 hq_back_profit from tmp_bq_back
    union all 
    select belong_province_code,	belong_province_name,	belong_city_code,	belong_city_name,	classify_large_name,purchase_group_name,0 	back_profit, back_profit hq_back_profit from tmp_hq_back
    )  a 
    group by belong_province_code,	belong_province_name,	belong_city_code,	belong_city_name,	classify_large_name,purchase_group_name