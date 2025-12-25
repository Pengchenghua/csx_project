-- --------要更新begin_sdt、end_sdt，这2个参数的值
/*
4、预估成本：                                
    ①“生鲜”按照如下优先级取值（其中周期进价数据剔除0.1的数据）：                                
         a.按照彩食鲜中台的供应链“下单策略”优先级取成本；                               
         b.主供应商的周期进价；                                 
         c.周期进价生效区最小进价；                               
         d.蛋取最近2周最近一次正常入库价；                                 
         e.非蛋品取近7天最近一次正常入库价；                                
         f.取近30天最后一次失效的周期进价；                                
    ②“食百”按照如下优先级取值；                                
         a.取食百近30天最后一次正常入库价；                                
         b.取基准价；                                
         c.取近30天最后一次失效的周期进价；                             
*/	

-- ---------------------------------------
-- 实时下单数据，待出库，剔除零星补货、客户自购、订单模式：0-配送
-- 订单维度：

drop table if exists csx_analyse_tmp.order_detail_qty_predict_ghm;
create table if not exists csx_analyse_tmp.order_detail_qty_predict_ghm as 
select 
    t1.*,
    t2.cost_price_for,
    t2.cost_price_type,
	t2.cost_price_for*t1.sale_qty as cost_amt,
	t1.sale_amt-(t2.cost_price_for*t1.sale_qty) as profit,
	1-(t2.cost_price_for*t1.sale_qty)/t1.sale_amt as profit_rate	
from 
    (select 
        d.performance_region_name,
        d.performance_province_name,
        d.performance_city_name,
        a.inventory_dc_code,
		a.order_code,
        a.customer_code,
        d.customer_name,
        a.sub_customer_code,
        max(a.sub_customer_name) as sub_customer_name,
        f.rp_service_user_work_no_new,
        f.rp_service_user_name_new,
        e.classify_large_code,
        e.classify_large_name,
        e.classify_middle_code,
        e.classify_middle_name,
        e.classify_small_code,
        e.classify_small_name,
        a.goods_code,
        e.goods_name,
        e.unit_name,
		sum(a.sale_qty) as sale_qty,
        sum(a.sale_amt)/sum(a.sale_qty) as sale_price,		
        sum(a.sale_amt) as sale_amt

    from 
        (select 
			*,
			cast(purchase_qty as decimal(11, 3)) * cast(purchase_unit_rate as decimal(11, 3)) as sale_qty, -- `下单数量`
			cast(sale_price as decimal(11, 3)) as sale_price_new, -- `团购价`
			cast(case 
				when order_status = 'CANCELLED' then cast(0 as decimal(11, 3))
				when order_status = 'HOME' then cast(sign_qty as decimal(11, 3)) * cast(sale_price as decimal(11, 3))
				when order_status = 'STOCKOUT' then cast(sign_qty as decimal(11, 3)) * cast(sale_price as decimal(11, 3))
				else cast(purchase_qty as decimal(11, 3)) * cast(purchase_unit_rate as decimal(11, 3)) * cast(sale_price as decimal(11, 3))
			end as decimal(11, 2)
				) as sale_amt	
		from csx_dwd.csx_dwd_csms_yszx_order_detail_di
		WHERE sdt >= '20251101'   -- 下单日期
			AND require_delivery_date>='20251117' and require_delivery_date<='20251123'  -- 配送日期
			AND order_status = 'CUTTED' -- 待出库
			AND customer_code='275371'
			AND delivery_flag not in (1,2) -- 零星补货、客户自购
			AND delivery_type_code = 0 -- 订单模式：0-配送,1-直送，2-自提，3-直通
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
        where sdt='${yes_sdt}' 
        ) f 
        on a.customer_code=f.customer_no 

    group by 
        d.performance_region_name,
        d.performance_province_name,
        d.performance_city_name,
        a.inventory_dc_code,
		a.order_code,
        a.customer_code,
        d.customer_name,
        a.sub_customer_code,
        e.classify_large_code,
        e.classify_large_name,
        e.classify_middle_code,
        e.classify_middle_name,
        e.classify_small_code,
        e.classify_small_name,
        a.goods_code,
        e.goods_name,
        e.unit_name,
        f.rp_service_user_work_no_new,
        f.rp_service_user_name_new 
    )   t1 
    -- 关联客户下期成本
    left join 
    (select * 
    from csx_analyse_tmp.csx_analyse_tmp_sale_detail_ky_target_month
    where cost_price_for>0 
    ) t2 
    on t1.inventory_dc_code=t2.inventory_dc_code and t1.customer_code=t2.customer_code and t1.sub_customer_code=t2.sub_customer_code and t1.goods_code=t2.goods_code;

	
-- 订单维度明细：
select * from csx_analyse_tmp.order_detail_qty_predict_ghm 
where cost_price_for>0 
	
-- 子客户维度：

	select 
		performance_region_name,
		performance_province_name,
		performance_city_name,
		customer_code,
		customer_name,
		sub_customer_code,
		sub_customer_name,
		classify_large_name,
		classify_middle_name,
		classify_small_name,
		goods_code,
		goods_name,
		unit_name,
		sum(sale_qty) as sale_qty,		
		sum(sale_amt)/sum(sale_qty) as sale_price,
		sum(cost_amt)/sum(sale_qty) as cost_price,		
        concat_ws(',', collect_set(cost_price_type)) as cost_price_type,
		
		sum(sale_amt) as sale_amt,
		sum(cost_amt) as cost_amt,
		sum(profit) as profit,
		1-sum(cost_amt)/sum(sale_amt) as profit_rate		
	from csx_analyse_tmp.order_detail_qty_predict_ghm 
	where cost_price_for>0 
	group by 
		performance_region_name,
		performance_province_name,
		performance_city_name,
		customer_code,
		customer_name,
		sub_customer_code,
		sub_customer_name,
		classify_large_name,
		classify_middle_name,
		classify_small_name,
		goods_code,
		goods_name,
		unit_name;