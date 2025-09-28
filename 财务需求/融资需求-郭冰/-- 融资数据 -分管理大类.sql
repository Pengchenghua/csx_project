-- 融资数据 -分管理大类
select 
    quarter_of_year,
    classify_large_code,
    classify_large_name,
    sum(sale_sku)/3 sale_sku,
    sum(no_tax_sales_value)no_tax_sales_value,
    sum(no_tax_profit)no_tax_profit,
    sum(no_tax_profit)/sum(no_tax_sales_value) as no_profit_rate
from ( 
select  quarter_of_year,
   month,
    classify_large_code,
    classify_large_name,
  --  concat(substr(sdt,1,4),'-',substr(sdt,5,2))as mon,
    count(distinct goods_code) as sale_sku,
    sum(sale_amt_no_tax)/10000 as no_tax_sales_value,
    sum(profit_no_tax)/10000 as no_tax_profit
from   csx_dws.csx_dws_sale_detail_di a 
 join 
(select quarter_of_year,calday,month from csx_dim.csx_dim_basic_date where calday>='20210101' and calday<'20240101') b on a.sdt=b.calday
where sdt>='20210101' and sdt<'20240101'
and channel_code in ('1','9','7')
and business_type_code!=4
group by  classify_large_code,
    classify_large_name,
    quarter_of_year,
month
     --   concat(substr(sdt,1,4),'-',substr(sdt,5,2))
 union all 
select 
    quarter_of_year,
    month,
    '00' second_category_code,
    '全国'      second_category_name,
   -- concat(substr(sdt,1,4),'-',substr(sdt,5,2))as mon,
    count(distinct goods_code) as sale_sku,
    sum(sale_amt_no_tax)/10000 as no_tax_sales_value,
    sum(profit_no_tax)/10000 as no_tax_profit
from    csx_dws.csx_dws_sale_detail_di  a
join 
(select quarter_of_year,calday,month from csx_dim.csx_dim_basic_date where calday>='20210101' and calday<'20240101') b on a.sdt=b.calday

where sdt>='20210101' and sdt<'20240101'
and channel_code in ('1','9','7')
and business_type_code!=4
group by quarter_of_year,
month
 
)a 
group by quarter_of_year,
   classify_large_code,
    classify_large_name
order by 
quarter_of_year,
    classify_large_code,
    classify_large_name

    ;

-- 大福利品类
select 
    quarter_of_year,
    classify_large_code,
    classify_large_name,
    sum(sale_sku)/3 sale_sku,
    sum(no_tax_sales_value)no_tax_sales_value,
    sum(no_tax_profit)no_tax_profit,
    sum(no_tax_profit)/sum(no_tax_sales_value) as no_profit_rate
from ( 
select  quarter_of_year,
   month,
    classify_large_code,
    classify_large_name,
  --  concat(substr(sdt,1,4),'-',substr(sdt,5,2))as mon,
    count(distinct goods_code) as sale_sku,
    sum(sale_amt_no_tax)/10000 as no_tax_sales_value,
    sum(profit_no_tax)/10000 as no_tax_profit
from   csx_dws.csx_dws_sale_detail_di a 
 join 
(select quarter_of_year,calday,month from csx_dim.csx_dim_basic_date where calday>='20210101' and calday<'20240101') b on a.sdt=b.calday
where sdt>='20210101' and sdt<'20240101'
and channel_code in ('1','9','7')

and business_type_code in (2,6)
group by  classify_large_code,
    classify_large_name,
    quarter_of_year,
month
     --   concat(substr(sdt,1,4),'-',substr(sdt,5,2))
 union all 
select 
    quarter_of_year,
    month,
    '00' second_category_code,
    '全国'      second_category_name,
   -- concat(substr(sdt,1,4),'-',substr(sdt,5,2))as mon,
    count(distinct goods_code) as sale_sku,
    sum(sale_amt_no_tax)/10000 as no_tax_sales_value,
    sum(profit_no_tax)/10000 as no_tax_profit
from    csx_dws.csx_dws_sale_detail_di  a
join 
(select quarter_of_year,calday,month from csx_dim.csx_dim_basic_date where calday>='20210101' and calday<'20240101') b on a.sdt=b.calday

where sdt>='20210101' and sdt<'20240101'
and channel_code in ('1','9','7')
and business_type_code in (2,6)
group by quarter_of_year,
month
 
)a 
group by quarter_of_year,
   classify_large_code,
    classify_large_name
order by 
quarter_of_year,
    classify_large_code,
    classify_large_name

    ;

    -- 融资数据 - 分行业含项目供应商 
select 
    quarter_of_year,
    second_category_code,
    second_category_name,
    avg(sale_sku) sale_sku,
    sum(no_tax_sales_value)no_tax_sales_value,
    sum(no_tax_profit)no_tax_profit,
    sum(no_tax_profit)/sum(no_tax_sales_value) as no_profit_rate
from ( 
select  quarter_of_year,
   month,
    second_category_code,
    second_category_name,
  --  concat(substr(sdt,1,4),'-',substr(sdt,5,2))as mon,
    count(distinct CUSTOMER_CODE) as sale_sku,
    sum(sale_amt_no_tax)/10000 as no_tax_sales_value,
    sum(profit_no_tax)/10000 as no_tax_profit
from   csx_dws.csx_dws_sale_detail_di a 
 join 
(select quarter_of_year,calday,month from csx_dim.csx_dim_basic_date where calday>='20210101' and calday<'20240101') b on a.sdt=b.calday
where sdt>='20210101' and sdt<'20240101'
and channel_code in ('1','9','7')
-- and business_type_code!=4
group by  second_category_code,
    second_category_name,
    quarter_of_year,
month
     --   concat(substr(sdt,1,4),'-',substr(sdt,5,2)) 
union all 
select 
    quarter_of_year,
    month,
    '00' second_category_code,
    '全国'      second_category_name,
   -- concat(substr(sdt,1,4),'-',substr(sdt,5,2))as mon,
    count(distinct CUSTOMER_CODE) as sale_sku,
    sum(sale_amt_no_tax)/10000 as no_tax_sales_value,
    sum(profit_no_tax)/10000 as no_tax_profit
from    csx_dws.csx_dws_sale_detail_di  a
join 
(select quarter_of_year,calday,month from csx_dim.csx_dim_basic_date where calday>='20210101' and calday<'20240101') b on a.sdt=b.calday

where sdt>='20210101' and sdt<'20240101'
and channel_code in ('1','9','7')
-- and business_type_code!=4
group by quarter_of_year,
month 
)a 
group by quarter_of_year,
    second_category_code,
    second_category_name
order by 
quarter_of_year,
    second_category_code,
    second_category_name


-- -- 融资数据 - 大区客户数 
select 
    quarter_of_year,
    performance_region_name,
    performance_province_name,
    performance_city_name,
    business_type_name,
    CUSTOMER_CODE,
 --   avg(sale_sku) sale_sku,
    sum(no_tax_sales_value)no_tax_sales_value,
    sum(no_tax_profit)no_tax_profit,
    sum(no_tax_profit)/sum(no_tax_sales_value) as no_profit_rate
from ( 
select  quarter_of_year,
   month,
      case when a.performance_province_name='河南省' then '华北大区' 
	     when a.performance_province_name in ('安徽省','湖北省') then '华东大区' 
	else a.performance_region_name end performance_region_name,performance_city_name,
  --  concat(substr(sdt,1,4),'-',substr(sdt,5,2))as mon,
     CUSTOMER_CODE,
     performance_province_name,
    case when business_type_code in ('6','2') then '大福利' else  business_type_name end business_type_name,
    sum(sale_amt_no_tax)/10000 as no_tax_sales_value,
    sum(profit_no_tax)/10000 as no_tax_profit
from   csx_dws.csx_dws_sale_detail_di a 
 join 
(select quarter_of_year,calday,month from csx_dim.csx_dim_basic_date where calday>='20210101' and calday<'20240101') b on a.sdt=b.calday

where sdt>='20210101' and sdt<'20240101'
and channel_code in ('1','9','7')
-- and business_type_code!=4
group by  case when a.performance_province_name='河南省' then '华北大区' 
	     when a.performance_province_name in ('安徽省','湖北省') then '华东大区' 
	else a.performance_region_name end ,
    quarter_of_year,CUSTOMER_CODE,
month, case when business_type_code in ('6','2') then '大福利' else  business_type_name end,performance_province_name,performance_city_name
--      --   concat(substr(sdt,1,4),'-',substr(sdt,5,2))
 
-- union all 
-- select 
--     quarter_of_year,
--     month,
--      '全国'      second_category_name,
--   -- concat(substr(sdt,1,4),'-',substr(sdt,5,2))as mon,
--     count(distinct CUSTOMER_CODE) as sale_sku,
--     sum(sale_amt_no_tax)/10000 as no_tax_sales_value,
--     sum(profit_no_tax)/10000 as no_tax_profit
-- from    csx_dws.csx_dws_sale_detail_di  a
-- join 
-- (select quarter_of_year,calday,month from csx_dim.csx_dim_basic_date where calday>='20210101' and calday<'20240101') b on a.sdt=b.calday

-- where sdt>='20210101' and sdt<'20240101'
-- and channel_code in ('1','9','7')
-- -- and business_type_code!=4
-- group by quarter_of_year,
-- month
 
)a 

group by   quarter_of_year,
    performance_region_name,performance_province_name,performance_city_name,
    business_type_name,
    CUSTOMER_CODE  
order by 
    quarter_of_year,
    performance_region_name



-- 按照日配自营
select  'Q4' smonth,
        case when a.performance_province_name='河南省' then '华北大区' 
	     when a.performance_province_name in ('安徽省','湖北省') then '华东大区' 
	else a.performance_region_name end performance_region_name,
 		a.performance_province_name,
		a.performance_city_name,
		business_type_name,
		second_category_code,
        second_category_name,
		sum(sale_amt)sale_amt,
		sum(profit) profit,
		sum(profit)/sum(sale_amt) prorate,
		sum(sale_amt_no_tax) sale_amt_no_tax,
		sum(profit_no_tax) profit_no_tax
	from   csx_dws.csx_dws_sale_detail_di a
	where sdt>='20231001'and sdt<'20240101'
	and CHANNEL_CODE in ('1','7','9')
	and business_type_code !=4
	group by 
        case when a.performance_province_name='河南省' then '华北大区' 
	     when a.performance_province_name in ('安徽省','湖北省') then '华东大区' 
	else a.performance_region_name end  ,
 		a.performance_province_name,
		a.performance_city_name,
		business_type_name,
		    second_category_code,
    second_category_name

  ;


  --财务数据需求-1. 分城市收入及客户数 20210726
select performance_region_name, performance_province_name,substr(sdt,1,6) smonth,
  --B端自营、bbc、城市服务商、M端、日配、福利--销售额
  sum(case when channel_code in('1','9') and business_type_code <>'4' then sale_amt_no_tax end)/10000 sale_amt_no_tax_B,--销售额
 sum(case when business_type_code ='6' then sale_amt_no_tax end)/10000 sale_amt_no_tax_bbc,
  sum(case when business_type_code ='4' then sale_amt_no_tax end)/10000 sale_amt_no_tax_csfws,	
  sum(case when channel_code ='2' then sale_amt_no_tax end)/10000 sale_amt_no_tax_M,
  sum(case when business_type_code ='1' then sale_amt_no_tax end)/10000 sale_amt_no_tax_ripei,
  sum(case when business_type_code in ('2') then sale_amt_no_tax end)/10000 sale_amt_no_tax_fuli,
  
  --B端自营、bbc、B+BBC、城市服务商、M端、日配、福利--客户数
  count(distinct case when channel_code in('1','9') and business_type_code <>'4' then customer_code end) counts_B,--客户数
   count(distinct case when business_type_code ='6' then customer_code end) counts_bbc,
  count(distinct case when channel_code in('1','7','9') and business_type_code <>'4' then customer_code end) counts_B_bbc,
  count(distinct case when business_type_code ='4' then customer_code end) counts_csfws,	
  count(distinct case when channel_code ='2' then customer_code end) counts_M,
  count(distinct case when business_type_code ='1' then customer_code end) counts_ripei,
  count(distinct case when business_type_code in ('2') then customer_code end) counts_fuli,
  
  --B端自营、bbc、城市服务商、M端、日配、福利--下单天数	
  count(distinct case when channel_code in('1','9') and business_type_code <>'4' then concat(customer_code,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_B,--下单天数
  count(distinct case when business_type_code ='6' then concat(customer_code,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_bbc,--下单天数
  count(distinct case when business_type_code ='4' then concat(customer_code,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_csfws,--下单天数	
  count(distinct case when channel_code ='2' then concat(customer_code,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_M,
  count(distinct case when business_type_code ='1' then concat(customer_code,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_ripei,
  count(distinct case when business_type_code in ('2') then concat(customer_code,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_fuli
from    csx_dws.csx_dws_sale_detail_di
where sdt>='20210101'
and sdt<='20231231'
group by performance_region_name,performance_province_name,substr(sdt,1,6)
union all
select '0'performance_region_name,'全国' performance_province_name,substr(sdt,1,6) smonth,
  --B端自营、bbc、城市服务商、M端、日配、福利--销售额
  sum(case when channel_code in('1','9') and business_type_code <>'4' then sale_amt_no_tax end)/10000 sale_amt_no_tax_B,--销售额
  sum(case when business_type_code ='6' then sale_amt_no_tax end)/10000 sale_amt_no_tax_bbc,
  sum(case when business_type_code ='4' then sale_amt_no_tax end)/10000 sale_amt_no_tax_csfws,	
  sum(case when channel_code ='2' then sale_amt_no_tax end)/10000 sale_amt_no_tax_M,
  sum(case when business_type_code ='1' then sale_amt_no_tax end)/10000 sale_amt_no_tax_ripei,
  sum(case when business_type_code ='2' then sale_amt_no_tax end)/10000 sale_amt_no_tax_fuli,
  
  --B端自营、bbc、B+BBC、城市服务商、M端、日配、福利--客户数
  count(distinct case when channel_code in('1','9') and business_type_code <>'4' then customer_code end) counts_B,--客户数
  count(distinct case when business_type_code ='6' then customer_code end) counts_bbc,
  count(distinct case when channel_code in('1','7','9') and business_type_code <>'4' then customer_code end) counts_B_bbc,
  count(distinct case when business_type_code ='4' then customer_code end) counts_csfws,	
  count(distinct case when channel_code ='2' then customer_code end) counts_M,
  count(distinct case when business_type_code ='1' then customer_code end) counts_ripei,
  count(distinct case when business_type_code ='2' then customer_code end) counts_fuli,
  
  --B端自营、bbc、城市服务商、M端、日配、福利--下单天数	
  count(distinct case when channel_code in('1','9') and business_type_code <>'4' then concat(customer_code,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_B,--下单天数
  count(distinct case when business_type_code ='6' then concat(customer_code,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_bbc,--下单天数
  count(distinct case when business_type_code ='4' then concat(customer_code,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_csfws,--下单天数	
  count(distinct case when channel_code ='2' then concat(customer_code,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_M,
  count(distinct case when business_type_code ='1' then concat(customer_code,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_ripei,
  count(distinct case when business_type_code ='2' then concat(customer_code,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_fuli
from  csx_dws.csx_dws_sale_detail_di
where sdt>='20210101'
and sdt<='20231231'
group by substr(sdt,1,6);




  --财务数据需求-1. 分城市收入及客户数 20210726,B端含B+BBC ,sale_amt_no_tax_fuli包含福利+BBC

  --财务数据需求-1. 分城市收入及客户数 20210726,B端含B+BBC ,sale_amt_no_tax_fuli包含福利+BBC
select performance_region_name, performance_province_name,substr(sdt,1,6) smonth,
  --B端自营、bbc、城市服务商、M端、日配、福利--销售额
  sum(case when channel_code in('1','9','7')  then sale_amt_no_tax end)/10000 sale_amt_no_tax_B,--销售额
 -- sum(case when business_type_code ='6' then sale_amt_no_tax end)/10000 sale_amt_no_tax_bbc,
 sum(case when channel_code in('1','7','9') and business_type_code <>'4' then sale_amt_no_tax end)/10000 sale_B_bbc,
  sum(case when business_type_code ='4' then sale_amt_no_tax end)/10000 sale_amt_no_tax_csfws,	
  sum(case when channel_code ='2' then sale_amt_no_tax end)/10000 sale_amt_no_tax_M,
  sum(case when business_type_code ='1' then sale_amt_no_tax end)/10000 sale_amt_no_tax_ripei,
  sum(case when business_type_code in ('2','6') then sale_amt_no_tax end)/10000 sale_amt_no_tax_fuli,
  
  --B端自营、bbc、B+BBC、城市服务商、M端、日配、福利--客户数
  count(distinct case when channel_code in('1','9','7')  then customer_code end) counts_B,--客户数
  -- count(distinct case when business_type_code ='6' then customer_code end) counts_bbc,
  count(distinct case when channel_code in('1','7','9') and business_type_code <>'4' then customer_code end) counts_B_bbc,
  count(distinct case when business_type_code ='4' then customer_code end) counts_csfws,	
  count(distinct case when channel_code ='2' then customer_code end) counts_M,
  count(distinct case when business_type_code ='1' then customer_code end) counts_ripei,
  count(distinct case when business_type_code in ('2','6') then customer_code end) counts_fuli,

  --B端自营、bbc、城市服务商、M端、日配、福利--下单天数	
  count(distinct case when channel_code in('1','9','7')  then concat(customer_code,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_B,--下单天数
 -- count(distinct case when business_type_code ='6' then concat(customer_code,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_bbc,--下单天数
  count(distinct case when channel_code in('1','7','9') and business_type_code <>'4' then concat(customer_code,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_B_bbc,
  count(distinct case when business_type_code ='4' then concat(customer_code,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_csfws,--下单天数
  count(distinct case when channel_code ='2' then concat(customer_code,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_M,
  count(distinct case when business_type_code ='1' then concat(customer_code,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_ripei,
  count(distinct case when business_type_code in ('2','6') then concat(customer_code,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_fuli
from    csx_dws.csx_dws_sale_detail_di
where sdt>='20210101'
and sdt<='20231231'
group by performance_region_name,performance_province_name,substr(sdt,1,6)
union all
select '0'performance_region_name,'全国' performance_province_name,substr(sdt,1,6) smonth,
  --B端自营、bbc、城市服务商、M端、日配、福利--销售额
  sum(case when channel_code in('1','9','7')  then sale_amt_no_tax end)/10000 sale_amt_no_tax_B,--销售额
 -- sum(case when business_type_code ='6' then sale_amt_no_tax end)/10000 sale_amt_no_tax_bbc,
  sum(case when channel_code in('1','7','9') and business_type_code <>'4' then sale_amt_no_tax end)/10000 sale_B_bbc,
  sum(case when business_type_code ='4' then sale_amt_no_tax end)/10000 sale_amt_no_tax_csfws,	
  sum(case when channel_code ='2' then sale_amt_no_tax end)/10000 sale_amt_no_tax_M,
  sum(case when business_type_code ='1' then sale_amt_no_tax end)/10000 sale_amt_no_tax_ripei,
  sum(case when business_type_code IN('2','6') then sale_amt_no_tax end)/10000 sale_amt_no_tax_fuli,
  
  --B端自营、bbc、B+BBC、城市服务商、M端、日配、福利--客户数
  count(distinct case when channel_code in('1','9','7')  then customer_code end) counts_B,--客户数
 -- count(distinct case when business_type_code ='6' then customer_code end) counts_bbc,
  count(distinct case when channel_code in('1','7','9') and business_type_code <>'4' then customer_code end) counts_B_bbc,
  count(distinct case when business_type_code ='4' then customer_code end) counts_csfws,	
  count(distinct case when channel_code ='2' then customer_code end) counts_M,
  count(distinct case when business_type_code ='1' then customer_code end) counts_ripei,
  count(distinct case when business_type_code IN('2','6') then customer_code end) counts_fuli,
  
  --B端自营、bbc、城市服务商、M端、日配、福利--下单天数	
  count(distinct case when channel_code in('1','9','7') then concat(customer_code,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_B,--下单天数
--  count(distinct case when business_type_code ='6' then concat(customer_code,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_bbc,--下单天数
  count(distinct case when channel_code in('1','7','9') and business_type_code <>'4' then concat(customer_code,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_B_bbc,

  count(distinct case when business_type_code ='4' then concat(customer_code,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_csfws,--下单天数	
  count(distinct case when channel_code ='2' then concat(customer_code,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_M,
  count(distinct case when business_type_code ='1' then concat(customer_code,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_ripei,
  count(distinct case when business_type_code in ('2','6') then concat(customer_code,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_fuli
from  csx_dws.csx_dws_sale_detail_di
where sdt>='20210101'
and sdt<='20231231'
group by substr(sdt,1,6);
