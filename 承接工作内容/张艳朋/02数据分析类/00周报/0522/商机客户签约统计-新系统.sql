============新系统数据==========
select 
	substr(business_sign_time,1,7) month 
    ,customer_code
	,customer_name
	,business_number
	,performance_region_code
	,performance_region_name
	,performance_province_code
	,performance_province_name
	,performance_city_code
	,performance_city_name
	,first_category_code
	,first_category_name
	,second_category_code
	,second_category_name
	,third_category_code
	,third_category_name
	,business_attribute_code
	,business_attribute_name
	,estimate_contract_amount
	,substr(first_sign_time,1,10) first_sign_date
	,case when substr(first_sign_time,1,7) = substr(business_sign_time,1,7) then '新签约客户' else '老签约客户' end as new_or_old_customer_mark
	,substr(business_sign_time,1,10) business_sign_date
	,substr(first_business_sign_time,1,10) first_business_sign_date
from csx_dim.csx_dim_crm_business_info
where 
	sdt='current' 
	and substr(business_sign_time,1,10) >= '2023-05-01'
    and substr(business_sign_time,1,10) <= '2023-05-31'
    and business_stage = 5 
	and status='1'
    and business_attribute_code in ('1', '2', '5')



====新系统数据+客户表信息====
select 
	a.customer_code
	,c.customer_name
	,a.business_number
	,a.performance_region_code
	,a.performance_region_name
	,a.performance_province_code
	,a.performance_province_name
	,a.performance_city_code
	,a.performance_city_name
	,c.first_category_code
	,c.first_category_name
	,c.second_category_code
	,c.second_category_name
	,c.third_category_code
	,c.third_category_name
	,a.business_attribute_code
	,a.business_attribute_name
	,a.estimate_contract_amount
	,substr(a.first_sign_time,1,7) first_sign_date
	,case when substr(a.first_sign_time,1,7) = substr(a.business_sign_time,1,7) then '新签约客户' else '老签约客户' end as new_or_old_customer_mark
	,substr(a.business_sign_time,1,10) business_sign_date
	,substr(a.first_business_sign_time,1,10) first_business_sign_date
from 	
	(select * from csx_dim.csx_dim_crm_business_info
	    where sdt='current' 
			and substr(business_sign_time,1,10) >= '${sdate}'
			and substr(business_sign_time,1,10) <= '${edate}'
			and business_stage = 5 
			and status='1'
			and business_attribute_code in ('1', '2', '5')	
	)a
	left join 
	(select * from csx_dim.csx_dim_crm_customer_info
	 where sdt='current' 
	)c on a.customer_id=c.customer_id   
where a.customer_code not in 
	('128515','128556','128454','128538','128524','128555','128527','128517','128535','128548','128560','128576','128534','128530','128521','128489','128459','128520','128565','128523','128550','128559','128481','128482','128533','128522','128546','128363','128511','128525','128458','128519','128532','128575','128545','128496','128453','128537','128570','128516','128512','128526','128540','128531','128536','128362','128509','128508','128518','128541','128567','128573','128557') 


	

	
/*========================================================旧系统============================================================		
---对应数据源5
---新签商机数据--旧系统
select 
    substr(sign_time,1,7) month,
    sales_region_code,
    sales_region_name,
    province_code,
    province_name,
    count(case when attribute = '1' then business_number else null end) rp_business_number,
    sum(case when attribute = '1' then estimate_contract_amount else null end ) rp_estimate_contract_amount,
    count(case when attribute in ('2')then business_number else null end) fl_business_number,
    sum(case when attribute in ('2') then estimate_contract_amount else null end ) fl_estimate_contract_amount,
	count(case when attribute in ('5')then business_number else null end) bbc_business_number,
    sum(case when attribute in ('5') then estimate_contract_amount else null end ) bbc_estimate_contract_amount		
from 	
    (select * from csx_dw.dws_crm_w_a_business_customer
     where sdt ='current'
    	and sign_time >= '2022-01-01 00:00:00'
        and sign_time <= '2022-07-31 23:59:59'
        and business_stage = 5
        and attribute IN ('1', '2', '5')
    ) a
left join 
    (select customer_id,customer_no
     from csx_dw.dws_crm_w_a_customer
     where sdt='current' 
    )c on a.id=c.customer_id   
where c.customer_no not in 
('128515','128556','128454','128538','128524','128555','128527','128517','128535','128548','128560','128576','128534','128530','128521','128489','128459','128520','128565','128523','128550','128559','128481','128482','128533','128522','128546','128363','128511','128525','128458','128519','128532','128575','128545','128496','128453','128537','128570','128516','128512','128526','128540','128531','128536','128362','128509','128508','128518','128541','128567','128573','128557') 
group by substr(sign_time,1,7),
    sales_region_code,
    sales_region_name,
    province_code;
		

		
	
		
---对应数据源5
---新签商机数据
insert overwrite directory '/tmp/gonghuimin/linshi04' row format delimited fields terminated by '\t'
select 
    substr(a.sign_time,1,7) smonth
	,d.customer_no
	,d.customer_name
	,a.business_number	
	,d.sales_region_name
	,d.province_name
	,d.first_category_code
	,d.first_category_name
	,d.second_category_code
	,d.second_category_name
	,d.third_category_code
	,d.third_category_name
	,a.gross_profit_rate
	,a.attribute_desc
	,a.estimate_contract_amount
	,substr(d.first_sign_time,1,10)  first_sign_time
	,case when substr(d.first_sign_time,1,7)=substr(a.sign_time,1,7) then '新签约客户' else '老签约客户' end new_or_old_customer_mark
from 
	(select * from csx_dw.dws_crm_w_a_business_customer
	where sdt ='current'
		and sign_time >= '2022-01-01 00:00:00'
		and sign_time <= '2022-07-31 23:59:59'
		and business_stage = 5
		and status='1'
		and attribute IN ('1', '2', '5')
	)a 
	left join 
	(select * from csx_dw.dws_crm_w_a_customer where sdt='current')d on a.id=d.customer_id;
*/