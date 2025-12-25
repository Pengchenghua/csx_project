select * from 
(select
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	a.customer_code,
	d.customer_name,
	e.first_sale_date,
	sum(case when sdt>='20250701' and sdt<='20250731' then sale_amt end)/10000 as by_sale_amt,
	sum(case when sdt>='20250701' and sdt<='20250731' then profit end)/10000 as by_profit,
	sum(case when sdt>='20250701' and sdt<='20250731' then profit end)/abs(sum(case when sdt>='20250701' and sdt<='20250731' then sale_amt end)) by_profit_rate,
	
	sum(case when sdt>='20250628' and sdt<='20250704' then profit end)/abs(sum(case when sdt>='20250628' and sdt<='20250704' then sale_amt end)) w1_profit_rate,

	sum(case when sdt>='20250705' and sdt<='20250711' then profit end)/abs(sum(case when sdt>='20250705' and sdt<='20250711' then sale_amt end)) w2_profit_rate

	sum(case when sdt>='20250712' and sdt<='20250718' then profit end)/abs(sum(case when sdt>='20250712' and sdt<='20250718' then sale_amt end)) w3_profit_rate,
	
	sum(case when sdt>='20250719' and sdt<='20250725' then profit end)/abs(sum(case when sdt>='20250719' and sdt<='20250725' then sale_amt end)) w4_profit_rate,
	
	sum(case when sdt>='20250726' and sdt<='20250801' then profit end)/abs(sum(case when sdt>='20250726' and sdt<='20250801' then sale_amt end)) w5_profit_rate	
	
from
	(
	select * from csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20250601' and sdt<='20250731'
		and business_type_code=1  
		and shipper_code='YHCSX' 
	) a		
	    -- -----客户数据
	left join 
		(select customer_code,customer_name 
		from csx_dim.csx_dim_crm_customer_info 
		where sdt='current' 
		and shipper_code='YHCSX'
		) d
		on a.customer_code=d.customer_code 	
	left join 
        (select
            code as type,
            max(name) as name,
            max(extra) as extra 
        from csx_dim.csx_dim_basic_topic_dict_df
        where parent_code = 'direct_delivery_type' 
        group by code 
        ) h 
        on a.direct_delivery_type=h.type 
	left join -- 客户最早销售
		(select customer_code,first_sale_date
		from csx_dws.csx_dws_crm_customer_active_di
		where sdt = 'current'
		)e on a.customer_code=e.customer_code	
    where h.extra='采购参与'			
group by 
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	a.customer_code,
	d.customer_name,
	e.first_sale_date
)a 
where first_sale_date>='20250701' -- and by_profit_rate<=0.05;