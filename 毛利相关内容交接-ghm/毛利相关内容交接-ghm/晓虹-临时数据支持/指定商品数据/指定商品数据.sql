select 
	a.inventory_dc_code,
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	a.customer_code,
    d.customer_name,
	a.goods_code,
	e.goods_name,
	g.regionalized_goods_name,
	e.classify_large_code,
    e.classify_large_name,
    e.classify_middle_code,
    e.classify_middle_name,
    e.classify_small_code,
    e.classify_small_name,
	
	f.rp_service_user_work_no_new,
    f.rp_service_user_name_new,
    f.work_no,
    f.sales_name,
    f.second_supervisor_work_no,
    f.second_supervisor_name,
	sum(sale_qty) as sale_qty,
	sum(sale_amt) as sale_amt,
	sum(profit)as profit,
	sum(profit)/abs(sum(sale_amt)) profit_rate,
	sum(sale_cost) sale_cost,
	sum(sale_amt)/sum(sale_qty) as avg_sj,  -- 平均售价
	sum(sale_cost)/sum(sale_qty) as avg_cb  -- 平均成本
from 
	(select *
	from csx_dws.csx_dws_sale_detail_di  
	where sdt>='20250201' and sdt<='20250421'
	and business_type_code=1
	and shipper_code='YHCSX' 
	and inventory_dc_code not in ('W0AX','W0BD','WB71','W0G6','WC51','WB06','W0T0')	
	) a 
	left join 
	(select * 
	from csx_dim.csx_dim_crm_customer_info 
	where sdt='current' 
	) d
	on a.customer_code=d.customer_code 
	left join 
	-- -----商品数据
	(select * 
	from csx_dim.csx_dim_basic_goods 
	where sdt='current' 
	) e 
	on a.goods_code=e.goods_code 

	left join  	
	-- 管家和销售员	
	(
	select
		customer_no,
		coalesce(rp_service_user_work_no_new, '') as rp_service_user_work_no_new,
		coalesce(rp_service_user_name_new, '') as rp_service_user_name_new,
		coalesce(work_no,'') work_no,
		coalesce(sales_name,'') sales_name,
		coalesce(second_supervisor_work_no,'') second_supervisor_work_no,
		coalesce(second_supervisor_name,'') second_supervisor_name			
	from csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
	where sdt = '${yester}'
	) f on a.customer_code = f.customer_no	
	
	left join
	(
	select dc_code,goods_code,regionalized_goods_name
	from csx_dim.csx_dim_basic_dc_goods
	where sdt = 'current'
	) g on g.dc_code=a.inventory_dc_code and a.goods_code =g.goods_code

group by 
	a.inventory_dc_code,
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	a.customer_code,
    d.customer_name,
	a.goods_code,
	e.goods_name,
	g.regionalized_goods_name,
	e.classify_large_code,
    e.classify_large_name,
    e.classify_middle_code,
    e.classify_middle_name,
    e.classify_small_code,
    e.classify_small_name,
	f.rp_service_user_work_no_new,
    f.rp_service_user_name_new,
    f.work_no,
    f.sales_name,
    f.second_supervisor_work_no,
    f.second_supervisor_name;
