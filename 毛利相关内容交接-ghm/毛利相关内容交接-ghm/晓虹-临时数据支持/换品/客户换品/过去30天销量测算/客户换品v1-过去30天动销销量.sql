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
-- --客户商品前一个报价生效周期的销量、售价、成本，目前最新的成本值
drop table if exists csx_analyse_tmp.csx_analyse_tmp_sale_detail_ky_last_month_detail_ghm;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_sale_detail_ky_last_month_detail_ghm as 
select 
    t1.*,
    t2.cost_price_for,
    t2.cost_price_type,
    nvl(tt3.customer_price,t3.customer_price) as customer_price  
    -- t1.sale_qty_seven_day as sale_qty_for_first -- 4.21晓虹暂定预测销量用上周销量替代
from 
    (select 
        a.performance_region_name,
        a.performance_province_name,
        a.performance_city_name,
        a.inventory_dc_code,
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
        sum(a.sale_amt) as sale_amt,
        sum(a.profit) as profit,
        sum(a.sale_qty) as sale_qty,
        sum(a.sale_amt)/sum(a.sale_qty) as sale_price,
        sum(a.sale_amt-a.profit)/sum(a.sale_qty) as cost_price 
    from 
        (select * 
        from csx_dws.csx_dws_sale_detail_di  
        where sdt>='${begin_sdt}'    
        and sdt<='${end_sdt}'    
        and business_type_code=1  
        and order_channel_code not in ('4','6','5') -- 剔除所有异常
        and refund_order_flag<>1 
        and delivery_type_code<>2 
        and shipper_code='YHCSX' 
		and customer_code in 
			('126377','112554','211611','248893','225238','273028','277010','281317','256779','131129','131187','235195','235184','235207','235201','235185','183893','276637','276632','276636','278490','276605','279554','120978','120957','121291','121037','120986','120984','126387','125394','103997','277858','279372','256736','125306','279555','277437','279553'
			)
        ) a 
        left join 
        csx_analyse_tmp.csx_analyse_tmp_abnormal_goods_ky_target_month c 
        on a.performance_region_name=c.performance_region_name and a.performance_province_name=c.performance_province_name and a.performance_city_name=c.performance_city_name and a.goods_code=c.goods_code 
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
        left join 
        -- 价格补救原单数据
        (select 
            original_order_code,
            customer_code,
            sub_customer_code,
            goods_code 
        from csx_dws.csx_dws_sale_detail_di  
        where sdt>=regexp_replace(add_months(trunc('${yes_date}','MM'),-1),'-','')  
        and sdt<='${yes_sdt}'   
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
        a.inventory_dc_code,
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
    on t1.inventory_dc_code=t2.inventory_dc_code and t1.customer_code=t2.customer_code and t1.sub_customer_code=t2.sub_customer_code and t1.goods_code=t2.goods_code 
     -- 关联目前生效区主客户商品报价数据
    left join 
    (select 
        warehouse_code,
        customer_code,
        product_code,
        max(customer_price) as customer_price  
    from csx_dwd.csx_dwd_price_customer_price_guide_di
	where substr(price_begin_time, 1, 10) <= current_date 
		and substr(price_end_time, 1, 10) >= current_date 
		and (sub_customer_code is null or length(sub_customer_code)=0) 
    group by 
        warehouse_code,
        customer_code,
        product_code
    ) t3 
    on t1.inventory_dc_code=t3.warehouse_code and t1.customer_code=t3.customer_code and t1.goods_code=t3.product_code 
    -- 关联目前生效区子客户商品报价数据
    left join 
    (select 
        warehouse_code,
        customer_code,
        sub_customer_code,
        product_code,
        max(customer_price) as customer_price  
    from csx_dwd.csx_dwd_price_customer_price_guide_di
    where substr(price_begin_time, 1, 10) <= current_date 
		and substr(price_end_time, 1, 10) >= current_date 
		and sub_customer_code is not null and length(sub_customer_code) > 0
    group by 
        warehouse_code,
        customer_code,
        sub_customer_code,
        product_code
    ) tt3 
    on t1.inventory_dc_code=tt3.warehouse_code and t1.customer_code=tt3.customer_code and t1.sub_customer_code=tt3.sub_customer_code and t1.goods_code=tt3.product_code;



-- 客户商品近30天销量，最新生效区客户报价，成本价，计算新的销售额毛利额毛利率数据；	
	
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
	sale_qty,
	sale_price_new, -- 客户最新报价
	cost_price_new, -- 客户最新成本
    cost_price_type, -- 最新成本取值来源
	sale_amt_new, -- 客户最新销售额
	cost_amt_new,
	profit_new,
	profitlv_new -- 客户最新毛利率
from 
	(select 
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
        concat_ws(',', collect_set(cost_price_type)) as cost_price_type,
	
		-- 近30天数据
		sum(sale_qty) as sale_qty,
		sum(sale_amt) as sale_amt,
		sum(profit) as profit,
		sum(sale_amt)/sum(sale_qty) as sale_price,
		sum(sale_amt-profit)/sum(sale_qty) as cost_price,
		sum(profit)/sum(sale_amt) as profitlv,
		
		-- 最新客户报价数据
		sum(customer_price*sale_qty) as sale_amt_new,
		sum(cost_price_for*sale_qty) as cost_amt_new,
		sum(customer_price*sale_qty)- sum(cost_price_for*sale_qty) as profit_new,		
		sum(customer_price*sale_qty)/sum(sale_qty) as sale_price_new,
		sum(cost_price_for*sale_qty)/sum(sale_qty) as cost_price_new,
		sum(customer_price*sale_qty-cost_price_for*sale_qty)/sum(customer_price*sale_qty) as profitlv_new,
		sum(customer_price*sale_qty-cost_price_for*sale_qty)/sum(customer_price*sale_qty)-sum(profit)/sum(sale_amt) as profitlv_diff,
		(sum(customer_price*sale_qty)/sum(sale_qty)-sum(sale_amt)/sum(sale_qty))/(sum(sale_amt)/sum(sale_qty)) as sale_price_diff,
		(sum(cost_price_for*sale_qty)/sum(sale_qty)-sum(sale_amt-profit)/sum(sale_qty))/(sum(sale_amt-profit)/sum(sale_qty)) as cost_price_diff
		
	from csx_analyse_tmp.csx_analyse_tmp_sale_detail_ky_last_month_detail_ghm 
	where cost_price_for>0 and customer_price>0 
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
		unit_name
	) t;
	
	
	
	
	
	
--  客户报价生效区价格：优先使用子客户报价，如果没有子客户报价则使用区主客户报价
WITH customer_price AS (
    -- 主客户报价
    SELECT 
        warehouse_code,
        customer_code,
        CAST(NULL AS STRING) AS sub_customer_code,
        product_code,
        MAX(customer_price) AS customer_price
    FROM csx_dwd.csx_dwd_price_customer_price_guide_di
    WHERE SUBSTR(price_begin_time, 1, 10) <= CURRENT_DATE 
        AND SUBSTR(price_end_time, 1, 10) >= CURRENT_DATE 
        AND (sub_customer_code IS NULL OR LENGTH(sub_customer_code) = 0)
    GROUP BY 
        warehouse_code,
        customer_code,
        product_code
        
    UNION ALL
    
    -- 子客户报价
    SELECT 
        warehouse_code,
        customer_code,
        sub_customer_code,
        product_code,
        MAX(customer_price) AS customer_price
    FROM csx_dwd.csx_dwd_price_customer_price_guide_di
    WHERE SUBSTR(price_begin_time, 1, 10) <= CURRENT_DATE 
        AND SUBSTR(price_end_time, 1, 10) >= CURRENT_DATE 
        AND sub_customer_code IS NOT NULL 
        AND LENGTH(sub_customer_code) > 0
    GROUP BY 
        warehouse_code,
        customer_code,
        sub_customer_code,
        product_code
),
ranked_prices AS (
    SELECT 
        warehouse_code,
        customer_code,
        product_code,
        -- 优先使用子客户报价，如果没有则使用主客户报价
        COALESCE(
            MAX(CASE WHEN sub_customer_code IS NOT NULL THEN customer_price END),
            MAX(CASE WHEN sub_customer_code IS NULL THEN customer_price END)
        ) AS customer_price
    FROM customer_price
    GROUP BY 
        warehouse_code,
        customer_code,
        product_code
)
SELECT 
    warehouse_code,
    customer_code,
    product_code,
    customer_price
FROM ranked_prices
WHERE customer_price IS NOT NULL;
	

/*-- 孔云版本：客户商品近30天销售数据，最新报价数据毛利率对标，excel模板《客户最新报价汇总及明细数据》	
	
select 
	performance_region_name as `大区`,
	performance_province_name as `省区`,
	performance_city_name as `城市`,
	customer_code as `客户编码`,
	customer_name as `客户名称`,
	classify_large_name as `管理大类名称`,
	classify_middle_name as `管理中类名称`,
	classify_small_name as `管理小类名称`,
	goods_code as `商品编码`,
	goods_name as `商品名称`,
	unit_name as `规格`,
	sale_amt as `上一个报价周期销售额`,
	profit as `上一个报价周期天毛利额`,
	sale_qty as `上一个报价周期销量`,
	sale_price as `上一个报价周期平均售价`,
	cost_price as `上一个报价周期平均成本`,
	profitlv as `上一个报价周期毛利率`,
	sale_price_new as `客户最新报价`,
	cost_price_new as `客户最新成本`,
    cost_price_type as `最新成本取值来源`,
	profitlv_new as `客户最新毛利率`,
	profitlv_diff as `毛利率环比波动`,
	sale_price_diff as `售价环比波动`,
	cost_price_diff as `成本环比波动`,
	(case when classify_large_name in ('蔬菜水果','肉禽水产','干货加工') and abs(sale_price_diff)>0.3 then '是' 
	      when classify_large_name not in ('蔬菜水果','肉禽水产','干货加工') and abs(sale_price_diff)>0.1 then '是' 
	else '否'
	end) as `售价是否波动异常`
from 
	(select 
		performance_region_name,
		performance_province_name,
		performance_city_name,
		customer_code,
		customer_name,
		classify_large_name,
		classify_middle_name,
		classify_small_name,
		goods_code,
		goods_name,
		unit_name,
        concat_ws(',', collect_set(cost_price_type)) as cost_price_type,
		sum(sale_amt) as sale_amt,
		sum(profit) as profit,
		sum(sale_qty) as sale_qty,
		sum(sale_amt)/sum(sale_qty) as sale_price,
		sum(sale_amt-profit)/sum(sale_qty) as cost_price,
		sum(profit)/sum(sale_amt) as profitlv,
		sum(customer_price*sale_qty)/sum(sale_qty) as sale_price_new,
		sum(cost_price_for*sale_qty)/sum(sale_qty) as cost_price_new,
		sum(customer_price*sale_qty-cost_price_for*sale_qty)/sum(customer_price*sale_qty) as profitlv_new,
		sum(customer_price*sale_qty-cost_price_for*sale_qty)/sum(customer_price*sale_qty)-sum(profit)/sum(sale_amt) as profitlv_diff,
		(sum(customer_price*sale_qty)/sum(sale_qty)-sum(sale_amt)/sum(sale_qty))/(sum(sale_amt)/sum(sale_qty)) as sale_price_diff,
		(sum(cost_price_for*sale_qty)/sum(sale_qty)-sum(sale_amt-profit)/sum(sale_qty))/(sum(sale_amt-profit)/sum(sale_qty)) as cost_price_diff
	from csx_analyse_tmp.csx_analyse_tmp_sale_detail_ky_last_month_detail_ghm 
	where cost_price_for>0 and customer_price>0 
	group by 
		performance_region_name,
		performance_province_name,
		performance_city_name,
		customer_code,
		customer_name,
		classify_large_name,
		classify_middle_name,
		classify_small_name,
		goods_code,
		goods_name,
		unit_name
	) t 	
	


	
	
	
	
	
	
	
	
	
	
	
	
