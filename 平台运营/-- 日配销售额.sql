-- 日配销售额
select substr(sdt,1,6) as sales_months,
 performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    sum(sale_amt)sale_amt,
    sum(profit) profit
from csx_dws.csx_dws_sale_detail_di a
left join 
	(select shop_code,shop_low_profit_flag from csx_dim.csx_dim_shop where sdt='current') c on a.inventory_dc_code=c.shop_code
  where sdt >=  '20210901'
    and sdt <=  '20211031'
    --and channel_code in ('1','9')
    and business_type_code=1
    and shop_low_profit_flag!=1
group by  classify_large_name  ,
    classify_middle_code,
    classify_middle_name,
    business_type_code,
    business_type_name,
    substr(sdt,1,6),
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name