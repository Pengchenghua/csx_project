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
	g.goods_status_name,
	e.classify_large_code,
    e.classify_large_name,
    e.classify_middle_code,
    e.classify_middle_name,
    e.classify_small_code,
    e.classify_small_name,	
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
	where sdt>='20250601' and sdt<='20250629'
	and business_type_code=1
	and shipper_code='YHCSX' 
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
	(
	select dc_code,goods_code,regionalized_goods_name,goods_status_name
	from csx_dim.csx_dim_basic_dc_goods
	where sdt = 'current'
	) g on g.dc_code=a.inventory_dc_code and a.goods_code =g.goods_code
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
	a.inventory_dc_code,
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	a.customer_code,
	d.customer_name,
	a.goods_code,
	e.goods_name,
	g.regionalized_goods_name,
	g.goods_status_name,
	e.classify_large_code,
    e.classify_large_name,
    e.classify_middle_code,
    e.classify_middle_name,
    e.classify_small_code,
    e.classify_small_name;
