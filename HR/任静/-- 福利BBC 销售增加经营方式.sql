-- 福利BBC 销售增加经营方式
select
  substr(sdt,1,6) sale_month,
  performance_region_code ,
  performance_region_name,
  order_business_type_name,
  performance_province_code ,
  performance_province_name ,
  performance_city_code ,
  performance_city_name ,
  business_type_name,
  customer_code,
  customer_name,
  b.operation_mode_name,
  sum(sale_amt)sale_amt,
  sum(sale_amt_no_tax)sale_amt_no_tax,
  sum(sale_cost_no_tax)sale_cost_no_tax,
  sum(profit_no_tax)profit_no_tax,
  sum(profit)profit
from
  csx_dws.csx_dws_sale_detail_di a
  left join 
  (select order_code, 
    operation_mode_name,
    goods_code
  from csx_dws.csx_dws_bbc_sale_detail_di 
    where sdt>='20240501'
    group by order_code, 
    operation_mode_name,
    goods_code
  ) b on a.order_code=b.order_code and a.goods_code=b.goods_code
where
  sdt >= '20240601'
  and sdt <= '20240630' 
  -- and is_purchase_dc=1
--  and channel_code not in ('2', '4', '6', '5')
  and business_type_code in ( '2','6') -- 福利BBC
  GROUP BY 
  substr(sdt,1,6)  ,
  b.operation_mode_name,
  performance_region_code ,
  performance_region_name,
  order_business_type_name,
  performance_province_code ,
  performance_province_name ,
  performance_city_code ,
  performance_city_name ,
  business_type_name,
  customer_code,
  customer_name