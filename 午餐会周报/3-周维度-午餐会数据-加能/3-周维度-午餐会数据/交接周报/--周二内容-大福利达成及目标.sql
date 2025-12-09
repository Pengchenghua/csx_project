case
        when business_type_code = 4
        and order_business_type_code = 2 then 2
        when business_type_code = 4
        and order_business_type_code <> 2 then 1
        else business_type_code
      end as
      
---------------------------- 大福利-福利BBC达成进度

select substr(sdt,1,6) smonth,
performance_region_name,
performance_province_name province_name,
performance_city_name city_group_name,
business_type_name,
sum(sale_amt) as sale_amt,
sum(profit) as profit,
case
        when business_type_code = 4
        and order_business_type_code = 2 then '福利'
        when  business_type_code in ( 2,10) then '福利'
        when business_type_code = 4
        and order_business_type_code <> 2 then '日配'
        else 
      business_type_name end  as business_type_name_new
from csx_dws.csx_dws_sale_detail_di
where sdt >=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-0),'-','')
and sdt <=regexp_replace('${sdt_yes_date}','-','')
and (business_type_code in ('2','6','10') or  business_type_code = 4 and order_business_type_code = 2)
group by substr(sdt,1,6),
business_type_name,
performance_province_name,
performance_city_name,
performance_region_name,
case
        when business_type_code = 4
        and order_business_type_code = 2 then '福利'
        when  business_type_code in ( 2,10) then '福利'
        when business_type_code = 4
        and order_business_type_code <> 2 then '日配'
        else 
      business_type_name end
      
;




-- 大福利目标
select
month as smonth,
 province_name,
city_group_name,
sum(case when business_type_code=6 then sales_value end ) as bbc_sale_amt_target,
sum(case when business_type_code in (2,10) then sales_value end ) as fl_sale_amt_target,
sum(coalesce(cast(sales_value as decimal(26,2)),0)) total_sale_amt
from csx_ods.csx_ods_csx_data_market_dws_basic_w_a_business_target_manage_df ---kpi目标
where month=substr(regexp_replace('${sdt_yes_date}','-',''),1,6)
-- and province_name not like '平台%'
and business_type_code in ('2','6','10')
group by month,
-- business_type_name,
province_name,
city_group_name
;

