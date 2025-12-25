select 
    a.performance_region_name,
    a.performance_province_name,
    a.performance_city_name,
    a.customer_code,
    d.customer_name,
    a.sub_customer_code,
    max(a.sub_customer_name) as sub_customer_name,
    e.classify_large_name,
    e.classify_middle_name,
    e.classify_small_name,
    a.goods_code,
    e.goods_name,
    e.unit_name,
	sum( case when a.sdt>='${sq01}'  and a.sdt<='${sq02}'  then a.sale_qty end ) as sale_qty_sq, --周为自然周
	sum( case when a.sdt>='${bq01}'  and a.sdt<='${bq02}'  then a.sale_qty end ) as sale_qty_bq				
from 
    (select * 
    from csx_dws.csx_dws_sale_detail_di  
    where sdt>='${sq01}'    
    and sdt<='${bq02}'    
    and business_type_code=1  
    and order_channel_code not in ('4','6','5') -- 剔除所有异常
    and refund_order_flag<>1 
    and delivery_type_code<>2 
    and shipper_code='YHCSX' 
	and customer_code in ('252181','151497','252189','252182','252183','254511','254080','252191','258912','252186','257481','252193','252195','256641','252185','252999','126377','257658','279553')
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
    left join 
    -- -----服务管家
    (select * 
    from csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df    
    where sdt='${bq02}' 
    ) f 
    on a.customer_code=f.customer_no 
    left join 
    -- 价格补救原单数据
    (select 
        original_order_code,
        customer_code,
        sub_customer_code,
        goods_code 
    from csx_dws.csx_dws_sale_detail_di  
    where sdt>=regexp_replace(add_months(trunc('${yes_date}','MM'),-1),'-','')  
    and sdt<='${bq02}'   
    and business_type_code=1  
    and order_channel_code='5' 
    group by 
        original_order_code,
        customer_code,
        sub_customer_code,
        goods_code 
    ) g 
    on a.order_code=g.original_order_code and a.customer_code=g.customer_code and a.sub_customer_code=g.sub_customer_code and a.goods_code=g.goods_code 
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
    -- and g.original_order_code is null 
    -- and c.goods_code is null 
    group by 
		a.performance_region_name,
		a.performance_province_name,
		a.performance_city_name,
		a.customer_code,
		d.customer_name,
		a.sub_customer_code,
		e.classify_large_name,
		e.classify_middle_name,
		e.classify_small_name,
		a.goods_code,
		e.goods_name,
		e.unit_name;