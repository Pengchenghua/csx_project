-- 彩食鲜周趋势
select
	c.csx_week,
	c.csx_week_range,
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	sum(sale_amt)/10000 as sale_amt,
	sum(profit)/10000 as profit,
	sum(profit)/abs(sum(sale_amt)) profit_rate
	
from
	(
	select *
	from 
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt >= '20250628' and sdt <= '20250731'
		and business_type_code=1  
		and shipper_code='YHCSX' 
	) a		
	left join -- 周信息
	(
	select
		calday,csx_week,csx_week_begin,csx_week_end,concat(csx_week,'(',substr(csx_week_begin,5,10),'-',substr(csx_week_end,5,10),')') as csx_week_range
	from
		csx_dim.csx_dim_basic_date
	) c on c.calday=a.sdt
	    -- -----客户数据
	left join 
		(select * 
		from csx_dim.csx_dim_crm_customer_info 
		where sdt='current' 
		and shipper_code='YHCSX'
		) d
		on a.customer_code=d.customer_code 
	left join 
		-- -----商品数据
		(select * 
		from csx_dim.csx_dim_basic_goods 
		where sdt='current' 
		) e 
		on a.goods_code=e.goods_code 	
	left join -- 客户最早销售
		(select customer_code,first_sale_date
		from csx_dws.csx_dws_crm_customer_active_di
		where sdt = 'current'
		)f on a.customer_code=f.customer_code		
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
    where h.extra='采购参与'	
		
group by 
	c.csx_week,
	c.csx_week_range,
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name;
 ;
 
 
-- 月同期环比
 
 select
	substr(a.sdt,1,6) smonth,
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	sum(sale_amt)/10000 as sale_amt,
	sum(profit)/10000 as profit,
	sum(profit)/abs(sum(sale_amt)) profit_rate
	
from
	(
	select *
	from 
		csx_dws.csx_dws_sale_detail_di
	where 
		((sdt >= '20250601' and sdt <= '20250630' )or (sdt >= '20250701' and sdt <= '20250731' ))
		and business_type_code=1  
		and shipper_code='YHCSX' 
	) a		
	left join -- 周信息
	(
	select
		calday,csx_week,csx_week_begin,csx_week_end,concat(csx_week,'(',substr(csx_week_begin,5,10),'-',substr(csx_week_end,5,10),')') as csx_week_range
	from
		csx_dim.csx_dim_basic_date
	) c on c.calday=a.sdt
	    -- -----客户数据
	left join 
		(select * 
		from csx_dim.csx_dim_crm_customer_info 
		where sdt='current' 
		and shipper_code='YHCSX'
		) d
		on a.customer_code=d.customer_code 
	left join 
		-- -----商品数据
		(select * 
		from csx_dim.csx_dim_basic_goods 
		where sdt='current' 
		) e 
		on a.goods_code=e.goods_code 	
	left join -- 客户最早销售
		(select customer_code,first_sale_date
		from csx_dws.csx_dws_crm_customer_active_di
		where sdt = 'current'
		)f on a.customer_code=f.customer_code		
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
    where h.extra='采购参与'	
		
group by 
	substr(a.sdt,1,6),
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name;
 ;