select 
    d.performance_region_name,
    d.performance_province_name,
    d.performance_city_name,
	a.inventory_dc_code,
    a.customer_code,
    d.customer_name,
	d.second_category_name,
	c.first_business_sale_date,
	c.last_business_sale_date,
	f.second_supervisor_work_no,
	f.second_supervisor_name,
	f.work_no,
	f.sales_name,
	f.rp_service_user_work_no_new,
	f.rp_service_user_name_new,
	count(distinct a.goods_code) goods_num,
	sum(sale_amt) as sale_amt,
	sum(profit) as profit,
	sum(profit)/abs(sum(sale_amt)) profit_rate,
	case when g.customer_code is not null then '断约' else '' end as dy_type
	-- sum(sale_qty) as sale_qty,
	-- sum(sale_cost) as sale_cost,
	-- sum(sale_amt)/sum(sale_qty) as avg_sj,  -- 平均售价
	-- sum(sale_cost)/sum(sale_qty) as avg_cb -- 平均成本		
from 
    (select * 
    from csx_dws.csx_dws_sale_detail_di  
    where sdt>='${sq01}'   
    and sdt<='${yes_sdt}'    
    and business_type_code=1  
    and shipper_code='YHCSX' 
    ) a 
	left join  -- 首单日期
	(
	select customer_code,
		min(first_business_sale_date) first_business_sale_date,
		max(last_business_sale_date) last_business_sale_date
	from csx_dws.csx_dws_crm_customer_business_active_di
	where sdt ='current' 
		and shipper_code='YHCSX'
		and business_type_code=1
	group by customer_code
	)c on c.customer_code=a.customer_code 	
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
		-- 销售员、销售经理、管家		
	left join  	
	(
	select
		customer_no,
		coalesce(work_no,'') work_no,
		coalesce(sales_name,'') sales_name, 
		coalesce(second_supervisor_work_no,'') second_supervisor_work_no,
		coalesce(second_supervisor_name,'') second_supervisor_name,
		coalesce(rp_service_user_work_no_new, '') as rp_service_user_work_no_new, -- 管家
		coalesce(rp_service_user_name_new, '') as rp_service_user_name_new		
	from csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
	where sdt = '${yester}'
	) f on a.customer_code = f.customer_no		
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
	-- 直送类型 详细履约模式的码表
	left join 
	(
	select `code`,name,extra
	from csx_dim.csx_dim_basic_topic_dict_df
	where parent_code = 'direct_delivery_type'
	)a2 on cast(a.direct_delivery_type as string)=a2.`code`
	-- 断约客户
	left join	
	(select customer_code,business_attribute_code
	from csx_dim.csx_dim_crm_terminate_customer_attribute
	where sdt='current' 
    and shipper_code='YHCSX'
	)g on a.customer_code=g.customer_code and a.business_type_code=g.business_attribute_code	
where h.extra='采购参与'
group by 
    d.performance_region_name,
    d.performance_province_name,
    d.performance_city_name,
	a.inventory_dc_code,
    a.customer_code,
    d.customer_name,
	d.second_category_name,
	c.first_business_sale_date,
	c.last_business_sale_date,
	f.second_supervisor_work_no,
	f.second_supervisor_name,
	f.work_no,
	f.sales_name,
	f.rp_service_user_work_no_new,
	f.rp_service_user_name_new,
	case when g.customer_code is not null then '断约' else '' end;