日配采购参与
河南、陕西、北京、深圳
drop table if exists csx_analyse_tmp.tmp_yszx_order_detail; 
create table if not exists csx_analyse_tmp.tmp_yszx_order_detail as  
select 
	substr(a.sdt,1,6) as smonth,
	f.csx_week,
	f.csx_week_range,
	a.sdt,
    a.performance_region_name,
    a.performance_province_name,
    a.performance_city_name,
	a.original_order_code,
	a.order_channel_detail_name,
	a.order_code,
	a.channel_name,
	a.business_type_name,
	a.delivery_type_name,	
	a.inventory_dc_code,
	a.inventory_dc_name,
    a.customer_code,
    d.customer_name,
	d.second_category_name,
	a.sub_customer_code,
	a.sub_customer_name,
    a.goods_code,
    regexp_replace(e.goods_name,'\n|\t|\r|\,|\"|\\\\n','') goods_name,	
	a.unit_name,
    e.classify_middle_name,
	a2.extra as direct_delivery_large_type,
    a2.name as new_direct_delivery_type,
	sum(sale_amt) as sale_amt,
	sum(profit) as profit,
	sum(profit)/abs(sum(sale_amt)) profit_rate,
	sum(sale_qty) as sale_qty,
	sum(sale_cost) as sale_cost,
	sum(sale_amt)/sum(sale_qty) as avg_sj,  -- 平均售价
	sum(sale_cost)/sum(sale_qty) as avg_cb -- 平均成本	
	
from 
    (select * 
    from csx_dws.csx_dws_sale_detail_di  
    where sdt>='${sq01}'   
    and sdt<='${yes_sdt}'    
    and business_type_code=1  
    and shipper_code='YHCSX' 
    and performance_province_name in ('北京市')
    ) a 
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
	left join -- 周信息
	(
	select
		calday,csx_week,csx_week_begin,csx_week_end,concat(csx_week,'(',substr(csx_week_begin,5,10),'-',substr(csx_week_end,5,10),')') as csx_week_range
	from
		csx_dim.csx_dim_basic_date
	) f on f.calday=a.sdt	
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

where h.extra='采购参与'
group by 
	substr(a.sdt,1,6),
	f.csx_week,
	f.csx_week_range,
	a.sdt,
    a.performance_region_name,
    a.performance_province_name,
    a.performance_city_name,
	a.original_order_code,
	a.order_channel_detail_name,
	a.order_code,
	a.channel_name,
	a.business_type_name,
	a.delivery_type_name,	
	a.inventory_dc_code,
	a.inventory_dc_name,
    a.customer_code,
    d.customer_name,
	d.second_category_name,
	a.sub_customer_code,
	a.sub_customer_name,
    a.goods_code,
    regexp_replace(e.goods_name,'\n|\t|\r|\,|\"|\\\\n','') ,	
	a.unit_name,
    e.classify_middle_name,
	a2.extra ,
    a2.name;