--指定销售员销售情况-任静
--指定销售员销售情况-任静
select
  substr(sdt, 1, 6) s_month,
  performance_region_name,
  performance_province_name,
  performance_city_name,
  customer_code,
  customer_name,
  sales_user_number,
  sales_user_name,
  business_type_name,
  sum(sale_amt) sale_amt,
  sum(profit) as profit
from
  csx_dws.csx_dws_sale_detail_di
where
  sdt >= '20250101'
  and sdt <= '20250630'
  and (
    business_type_code in ('2', '6', '10')
    or inventory_dc_code in ('WD75', 'WD76', 'WD77', 'WD78', 'WD79', 'WD80', 'WD81')
  )
  and sales_user_number in (
    '81310456',
    '81048704',
    '81014012',
    '80946212',
    '81195268',
    '81299293',
    '80992029',
    '80946351',
    '81299149',
    '81307194',
    '81075241',
    '81310723',
    '81298720',
    '81310027',
    '81307336',
    '81307331',
    '81189001',
    '81309898'
  )
group by
  substr(sdt, 1, 6),
  performance_region_name,
  performance_province_name,
  performance_city_name,
  customer_code,
  customer_name,
  sales_user_number,
  sales_user_name,
  business_type_name