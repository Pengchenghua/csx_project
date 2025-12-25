
select * from 
(
select 
	d.performance_region_name,
	d.performance_province_name,
	d.performance_city_name,
	a.customer_code,
    regexp_replace(d.customer_name,'\n|\t|\r|\,|\"|\\\\n','') customer_name ,
	regexp_replace(a.invoice_customer_name,'\n|\t|\r|\,|\"|\\\\n','') invoice_customer_name
from 
    (SELECT 
		customer_code,
		invoice_customer_name,
        ROW_NUMBER() OVER (
            PARTITION BY invoice_customer_name  
            ORDER BY sdt DESC
        ) as rn
    FROM csx_dwd.csx_dwd_sss_invoice_di
	where sdt >='20250101'
	)a
	left join
	(select * from csx_dim.csx_dim_crm_customer_info where sdt = 'current'	
	)d on d.customer_code=a.customer_code
where rn=1
)a 
where customer_name<>invoice_customer_name;





select a.*,    sale_amt from 
(
select 
	d.performance_region_name,
	d.performance_province_name,
	d.performance_city_name,
	a.customer_code,
    regexp_replace(d.customer_name,'\n|\t|\r|\,|\"|\\\\n','') customer_name ,
	regexp_replace(a.invoice_customer_name,'\n|\t|\r|\,|\"|\\\\n','') invoice_customer_name,
	count(a.customer_code) OVER (
            PARTITION BY a.invoice_customer_name 
        ) as cut_rn
from 
    (SELECT 
		customer_code,
		invoice_customer_name
    FROM    csx_dwd.csx_dwd_sss_invoice_di
	where sdt >='20240101'
	  group by   customer_code,
		invoice_customer_name
	)a
	left join
	(select * from csx_dim.csx_dim_crm_customer_info where sdt = 'current'	
	)d on d.customer_code=a.customer_code
-- where rn=1
)a 
left join 
(select customer_code,sum(sale_amt)/10000 sale_amt
from csx_dws.csx_dws_sale_detail_di
    where sdt>='20250101'
    group by customer_code) b on a.customer_code=b.customer_code
where cut_rn>1

;

select * from 
(
select 
	d.performance_region_name,
	d.performance_province_name,
	d.performance_city_name,
	a.customer_code,
	a.sub_customer_code
	a.sub_customer_name
    regexp_replace(d.customer_name,'\n|\t|\r|\,|\"|\\\\n','') customer_name ,
	regexp_replace(a.invoice_customer_name,'\n|\t|\r|\,|\"|\\\\n','') invoice_customer_name
from 
    (SELECT 
		customer_code,
		sub_customer_code,
		sub_customer_name,
		invoice_customer_name,
        ROW_NUMBER() OVER (
            PARTITION BY invoice_customer_name  
            ORDER BY sdt DESC
        ) as rn
    FROM csx_dwd.csx_dwd_sss_invoice_di
	where sdt >='20250101'
	)a
	left join
	(select * from csx_dim.csx_dim_crm_customer_info where sdt = 'current'	
	)d on d.customer_code=a.customer_code
where rn=1
)a 
where customer_name<>invoice_customer_name