-- 新客不含续签24年
with tmp_customer_active as 
			(select 
			    performance_region_name,
			    performance_province_name,
			    performance_city_name,
				customer_code,
				customer_name,
				business_type_name,
				min(first_business_sign_date	) min_first_business_sign_date	,
				min(first_business_sale_date) min_first_business_sale_date,
				substr(min(first_business_sale_date),1,6) first_sale_month
			from  csx_dws.csx_dws_crm_customer_business_active_di
			where sdt ='current' 
			and business_type_code='1'
-- 			and substr(min(first_business_sale_date),1,6)>='2024-01'
			group by customer_code,
			customer_name,
			 performance_region_name,
			    performance_province_name,
			    performance_city_name,
				customer_code,
				business_type_name
)
,
tmp_sale as 
 (select customer_code,
    substr(sdt,1,6) as sale_month,
    sum(sale_amt) sale_amt
 from csx_dws.csx_dws_sale_detail_di 
    where sdt>='20240101' and sdt<='20241231' 
    and business_type_code='1'
 group by customer_code
    ,substr(sdt,1,6)
 )select a.*,b.sale_amt from tmp_customer_active a 
 left join 
 tmp_sale b on a.customer_code=b.customer_code 
  and sale_month=a.first_sale_month
 where first_sale_month>='202401'

 