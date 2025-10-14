-- 采购季度绩效
-- 日配-采购参与销售额+M端
create table csx_analyse_tmp.csx_analyse_tmp_puarchse_sale_detail as 
with tmp_sale as (
select substr(sdt,1,6) as sales_months,
    basic_performance_region_name,
	basic_performance_province_code,
	basic_performance_province_name,
	basic_performance_city_code,
	basic_performance_city_name,
    case when classify_large_code in ('B04','B05','B06','B07','B08','B09') then '食百'else classify_large_name end classify_large_name,
    classify_middle_code,
    classify_middle_name,
    business_type_code,
    business_type_name,
    sum(sale_amt)sale_amt,
    sum(profit) profit
from csx_dws.csx_dws_sale_detail_di a
left join 
  (select
      `code`,
      name,
      extra
    from
      csx_dim.csx_dim_basic_topic_dict_df
    where
      parent_code = 'direct_delivery_type') p on cast(a.direct_delivery_type as string) =  code
left join 
	(select basic_performance_region_name,
	    basic_performance_province_code,
	    basic_performance_province_name,
	    basic_performance_city_code,
	    basic_performance_city_name,
	    shop_code
	  from csx_dim.csx_dim_shop
	    where sdt='current' 
	 ) c on a.inventory_dc_code=c.shop_code
  where sdt >= '20250701'
    and sdt <= '20250930'
    -- and channel_code in ('1','9')
    and ((business_type_code=1   and  extra='采购参与' ) or channel_code=2)
    and customer_code not in ('260128','262310','262306','262305','262311','262312') 
group by  case when classify_large_code in ('B04','B05','B06','B07','B08','B09') then '食百'else classify_large_name end ,
    classify_middle_code,
    classify_middle_name,
    business_type_code,
    business_type_name,
     substr(sdt,1,6),
    basic_performance_region_name,
	    basic_performance_province_code,
	    basic_performance_province_name,
	    basic_performance_city_code,
	    basic_performance_city_name
) ,
tmp_fl as 
(SELECT
  substr(regexp_replace(to_date(belong_date),'-',''),1,6) s_month,
  a.basic_performance_region_name,
  a.basic_performance_province_code,
  a.basic_performance_province_name,
  a.basic_performance_city_code,
  a.basic_performance_city_name,
  m.first_level_code firstLevelCode,
  m.first_level_name firstLevelName,
  m.second_level_code secondLevelCode,
  m.second_level_name secondLevelName,
  -- 分摊支出负数
  sum(
            CASE
                WHEN a.is_share_fee = '1' AND a.income_type = '1' 
                THEN -CAST(m.total_amount AS DECIMAL(26, 4))
                ELSE CAST(m.total_amount AS DECIMAL(26, 4))
            END
        ) billTotalAmount 
FROM
    csx_dwd.csx_dwd_pss_settle_settle_bill_detail_management_classification_item_di m
  inner JOIN (
    select
      belong_date,
      settle_code,
      settlement_dc_code,
      shop_name as settlement_dc_name,
      c.basic_performance_region_name,
	  c.basic_performance_province_code,
	  c.basic_performance_province_name,
	  c.basic_performance_city_code,
	  c.basic_performance_city_name,
	  a.is_share_fee,
	  a.income_type 
    from
      csx_dwd.csx_dwd_pss_settle_settle_bill_di a 
   inner join 
	(select basic_performance_region_name,
	    basic_performance_province_code,
	    basic_performance_province_name,
	    basic_performance_city_code,
	    basic_performance_city_name,
	    shop_code,
	    shop_name
	  from csx_dim.csx_dim_shop
	    where sdt='current' 
	 ) c on a.settlement_dc_code=c.shop_code
   where (settlement_dc_name not like '%项目供应商%'
  and settlement_dc_name not like '%福利%'
  and settlement_dc_name not like '%BBC%'
  and settlement_dc_name not like '%全国%'
  and settlement_dc_name not like '%合伙人%'
  and settlement_dc_name not like '%服务商%'
  and settlement_dc_name not like '%前置仓%'
  and settlement_dc_name not like '%分仓%'
  )
  ) a ON a.settle_code = m.settle_no
 where 
   to_date(belong_date) >= '${sdate}'
  and to_date(belong_date) <= '${edate}'
 group by substr(regexp_replace(to_date(belong_date),'-',''),1,6) ,
  a.basic_performance_region_name,
  a.basic_performance_province_code,
  a.basic_performance_province_name,
  a.basic_performance_city_code,
  a.basic_performance_city_name,
  m.first_level_code ,
  m.first_level_name ,
  m.second_level_code ,
  m.second_level_name  
)
select sales_months,
    basic_performance_region_name,
	basic_performance_province_code,
	basic_performance_province_name,
	basic_performance_city_code,
	basic_performance_city_name,
  business_type_name,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    sum(sale_amt)sale_amt,
    sum(profit) profit,
    sum(billTotalAmount) billTotalAmount
from
(select sales_months,
    basic_performance_region_name,
	basic_performance_province_code,
	basic_performance_province_name,
	basic_performance_city_code,
	basic_performance_city_name,
  business_type_name,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    (sale_amt)sale_amt,
    (profit) profit,
    0 billTotalAmount
from tmp_sale a 
union all 
SELECT
  s_month sales_months,
  basic_performance_region_name,
  basic_performance_province_code,
  basic_performance_province_name,
  basic_performance_city_code,
  basic_performance_city_name,
  '' business_type_name,
  case when firstLevelCode in ('B04','B05','B06','B07','B08','B09') then '食百'else firstLevelName end classify_large_name,
  secondLevelCode,
  secondLevelName,
  0 sale_amt,
  0 profit,
  billTotalAmount 
FROM tmp_fl
)a 
group by sales_months,
    basic_performance_region_name,
	basic_performance_province_code,
	basic_performance_province_name,
	basic_performance_city_code,
	basic_performance_city_name,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    business_type_name
  ;

-- 销售需要核对仓归属问题,销售与费用需要分开取20250710

select substr(sdt,1,6) as sales_months,
    performance_region_name,
	performance_province_code,
	performance_province_name,
	performance_city_code,
	performance_city_name,
    case when classify_large_code in ('B04','B05','B06','B07','B08','B09') then '食百'else classify_large_name end classify_large_name,
    classify_middle_code,
    classify_middle_name,
    business_type_code,
    business_type_name,
    sum(sale_amt)sale_amt,
    sum(profit) profit
from csx_dws.csx_dws_sale_detail_di a
left join 
  (select
      `code`,
      name,
      extra
    from
      csx_dim.csx_dim_basic_topic_dict_df
    where
      parent_code = 'direct_delivery_type') p on cast(a.direct_delivery_type as string) =  code
 
  where sdt >= '20250701'
    and sdt <= '20250930'
    -- and channel_code in ('1','9')
    and ((business_type_code=1   and  extra='采购参与' ) or channel_code=2)
group by  case when classify_large_code in ('B04','B05','B06','B07','B08','B09') then '食百'else classify_large_name end ,
    classify_middle_code,
    classify_middle_name,
    business_type_code,
    business_type_name,
    substr(sdt,1,6),
    performance_region_name,
	performance_province_code,
	performance_province_name,
	performance_city_code,
	performance_city_name
  ;
-- 后台返利费用
with tmp_fl as 
(SELECT
  substr(regexp_replace(to_date(belong_date),'-',''),1,6) s_month,
  a.basic_performance_region_name,
  a.basic_performance_province_code,
  a.basic_performance_province_name,
  a.basic_performance_city_code,
  a.basic_performance_city_name,
  m.first_level_code firstLevelCode,
  m.first_level_name firstLevelName,
  m.second_level_code secondLevelCode,
  m.second_level_name secondLevelName,
  -- 分摊支出负数
  sum(
            CASE
                WHEN a.is_share_fee = '1' AND a.income_type = '1' 
                THEN -CAST(m.total_amount AS DECIMAL(26, 4))
                ELSE CAST(m.total_amount AS DECIMAL(26, 4))
            END
        ) billTotalAmount 
FROM
    csx_dwd.csx_dwd_pss_settle_settle_bill_detail_management_classification_item_di m
  inner JOIN (
    select
      belong_date,
      settle_code,
      settlement_dc_code,
      shop_name as settlement_dc_name,
      c.basic_performance_region_name,
	  c.basic_performance_province_code,
	  c.basic_performance_province_name,
	  c.basic_performance_city_code,
	  c.basic_performance_city_name,
	  a.is_share_fee,
	  a.income_type 
    from
      csx_dwd.csx_dwd_pss_settle_settle_bill_di a 
   inner join 
	(select basic_performance_region_name,
	    basic_performance_province_code,
	    basic_performance_province_name,
	    basic_performance_city_code,
	    basic_performance_city_name,
	    shop_code,
	    shop_name
	  from csx_dim.csx_dim_shop
	    where sdt='current' 
	 ) c on a.settlement_dc_code=c.shop_code
   where (settlement_dc_name not like '%项目供应商%'
  and settlement_dc_name not like '%福利%'
  and settlement_dc_name not like '%BBC%'
  and settlement_dc_name not like '%全国%'
  and settlement_dc_name not like '%合伙人%'
  and settlement_dc_name not like '%服务商%'
  and settlement_dc_name not like '%前置仓%'
  -- and settlement_dc_name not like '%分仓%'
  )
  ) a ON a.settle_code = m.settle_no
 where 
   to_date(belong_date) >= '${sdate}'
  and to_date(belong_date) <= '${edate}'
 group by substr(regexp_replace(to_date(belong_date),'-',''),1,6) ,
  a.basic_performance_region_name,
  a.basic_performance_province_code,
  a.basic_performance_province_name,
  a.basic_performance_city_code,
  a.basic_performance_city_name,
  m.first_level_code ,
  m.first_level_name ,
  m.second_level_code ,
  m.second_level_name  
)
select * from tmp_fl
;
-- 一次性出库缺货率
select
  substr(sdt,1,6) smt,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
--   a.inventory_dc_code,
--   inventory_dc_name,
--   business_division_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  sum(if(is_out_of_stock = 1, 1, 0)) as stock_out_sku,
  count(goods_code) all_sku
from
  csx_report.csx_report_oms_out_of_stock_goods_1d a
where
  sdt >= regexp_replace(trunc('${edate}','MM'),'-','') 
    and sdt <=  regexp_replace('${edate}','-','') 
  group by substr(sdt,1,6),
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
--   a.inventory_dc_code,
--   inventory_dc_name,
--   business_division_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name

  ;
    	