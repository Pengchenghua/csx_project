--财务数据需求-1. 分城市收入及客户数 20210726
select province_name,substr(sdt,1,6) smonth,
  --B端自营、bbc、城市服务商、M端、日配、福利--销售额
  sum(case when channel_code in('1','9') and business_type_code <>'4' then excluding_tax_sales end)/10000 excluding_tax_sales_B,--销售额
  sum(case when business_type_code ='6' then excluding_tax_sales end)/10000 excluding_tax_sales_bbc,
  sum(case when business_type_code ='4' then excluding_tax_sales end)/10000 excluding_tax_sales_csfws,	
  sum(case when channel_code ='2' then excluding_tax_sales end)/10000 excluding_tax_sales_M,
  sum(case when business_type_code ='1' then excluding_tax_sales end)/10000 excluding_tax_sales_ripei,
  sum(case when business_type_code ='2' then excluding_tax_sales end)/10000 excluding_tax_sales_fuli,
  
  --B端自营、bbc、B+BBC、城市服务商、M端、日配、福利--客户数
  count(distinct case when channel_code in('1','9') and business_type_code <>'4' then customer_no end) counts_B,--客户数
  count(distinct case when business_type_code ='6' then customer_no end) counts_bbc,
  count(distinct case when channel_code in('1','7','9') and business_type_code <>'4' then customer_no end) counts_B_bbc,
  count(distinct case when business_type_code ='4' then customer_no end) counts_csfws,	
  count(distinct case when channel_code ='2' then customer_no end) counts_M,
  count(distinct case when business_type_code ='1' then customer_no end) counts_ripei,
  count(distinct case when business_type_code ='2' then customer_no end) counts_fuli,
  
  --B端自营、bbc、城市服务商、M端、日配、福利--下单天数	
  count(distinct case when channel_code in('1','9') and business_type_code <>'4' then concat(customer_no,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_B,--下单天数
  count(distinct case when business_type_code ='6' then concat(customer_no,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_bbc,--下单天数
  count(distinct case when business_type_code ='4' then concat(customer_no,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_csfws,--下单天数	
  count(distinct case when channel_code ='2' then concat(customer_no,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_M,
  count(distinct case when business_type_code ='1' then concat(customer_no,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_ripei,
  count(distinct case when business_type_code ='2' then concat(customer_no,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_fuli
from csx_dw.dws_sale_r_d_detail
where sdt>='20200101'
and sdt<'20211001'
group by province_name,substr(sdt,1,6)
union all
select '全国' province_name,substr(sdt,1,6) smonth,
  --B端自营、bbc、城市服务商、M端、日配、福利--销售额
  sum(case when channel_code in('1','9') and business_type_code <>'4' then excluding_tax_sales end)/10000 excluding_tax_sales_B,--销售额
  sum(case when business_type_code ='6' then excluding_tax_sales end)/10000 excluding_tax_sales_bbc,
  sum(case when business_type_code ='4' then excluding_tax_sales end)/10000 excluding_tax_sales_csfws,	
  sum(case when channel_code ='2' then excluding_tax_sales end)/10000 excluding_tax_sales_M,
  sum(case when business_type_code ='1' then excluding_tax_sales end)/10000 excluding_tax_sales_ripei,
  sum(case when business_type_code ='2' then excluding_tax_sales end)/10000 excluding_tax_sales_fuli,
  
  --B端自营、bbc、B+BBC、城市服务商、M端、日配、福利--客户数
  count(distinct case when channel_code in('1','9') and business_type_code <>'4' then customer_no end) counts_B,--客户数
  count(distinct case when business_type_code ='6' then customer_no end) counts_bbc,
  count(distinct case when channel_code in('1','7','9') and business_type_code <>'4' then customer_no end) counts_B_bbc,
  count(distinct case when business_type_code ='4' then customer_no end) counts_csfws,	
  count(distinct case when channel_code ='2' then customer_no end) counts_M,
  count(distinct case when business_type_code ='1' then customer_no end) counts_ripei,
  count(distinct case when business_type_code ='2' then customer_no end) counts_fuli,
  
  --B端自营、bbc、城市服务商、M端、日配、福利--下单天数	
  count(distinct case when channel_code in('1','9') and business_type_code <>'4' then concat(customer_no,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_B,--下单天数
  count(distinct case when business_type_code ='6' then concat(customer_no,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_bbc,--下单天数
  count(distinct case when business_type_code ='4' then concat(customer_no,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_csfws,--下单天数	
  count(distinct case when channel_code ='2' then concat(customer_no,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_M,
  count(distinct case when business_type_code ='1' then concat(customer_no,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_ripei,
  count(distinct case when business_type_code ='2' then concat(customer_no,'_',regexp_replace(split(order_time, ' ')[0], '-', '')) end) days_fuli
from csx_dw.dws_sale_r_d_detail
where sdt>='20200101'
and sdt<'20211001'
group by substr(sdt,1,6);
