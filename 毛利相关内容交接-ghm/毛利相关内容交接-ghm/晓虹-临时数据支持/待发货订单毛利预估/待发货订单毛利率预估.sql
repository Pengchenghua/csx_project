
-- 关联客户下期成本
    left join 
    (select * 
    from csx_analyse_tmp.csx_analyse_tmp_sale_detail_ky_target_month
    where cost_price_for>0 
    ) t2 
    on t1.inventory_dc_code=t2.inventory_dc_code and t1.customer_code=t2.customer_code and t1.sub_customer_code=t2.sub_customer_code and t1.goods_code=t2.goods_code 
	

-- 订单商品数据
drop table if exists csx_analyse_tmp.tmp_order_detail01;
create table if not exists csx_analyse_tmp.tmp_order_detail01 as 	
select 
	d.performance_region_name,
    d.performance_province_name,
    d.performance_city_name,
    a.inventory_dc_code,
	a.order_code,
    a.customer_code,
    d.customer_name,
    a.sub_customer_code,
    a.sub_customer_name,
    a.goods_code,
    e.goods_name,
	e.unit_name,
	purchase_qty_new,
	sale_price_new,
    sale_amt_new
from 
	(select 
		*,
		cast(purchase_qty as decimal(11, 3)) * cast(purchase_unit_rate as decimal(11, 3)) as purchase_qty_new, -- `下单数量`
		cast(sale_price as decimal(11, 3)) as sale_price_new, -- `团购价`
		cast(case 
			 when order_status = 'CANCELLED' then cast(0 as decimal(11, 3))
			 when order_status = 'HOME' then cast(sign_qty as decimal(11, 3)) * cast(sale_price as decimal(11, 3))
			 when order_status = 'STOCKOUT' then cast(sign_qty as decimal(11, 3)) * cast(sale_price as decimal(11, 3))
		else cast(purchase_qty as decimal(11, 3)) * cast(purchase_unit_rate as decimal(11, 3)) * cast(sale_price as decimal(11, 3))
		end as decimal(11, 2)
		) as sale_amt_new	
	from csx_dwd.csx_dwd_csms_yszx_order_detail_di
    WHERE sdt >= '20251101'   -- 下单日期今天
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

	
	

-- 订单+商品+成本
drop table if exists csx_analyse_tmp.tmp_order_analysis01;
create table if not exists csx_analyse_tmp.tmp_order_analysis01 as 	
SELECT 
    performance_region_name,
    performance_province_name,
    performance_city_name,
    inventory_dc_code,
    order_code,
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
    purchase_qty_new, -- 下单量
    sale_price_new,  -- 团购价
    cost_price_for, -- 成本价
	cost_price_type, -- 成本取值来源
    sale_amt_new,	   -- 金额
	cost_price_for*purchase_qty_new as cost_amt,
    purchase_qty_new * (sale_price_new - cost_price_for) AS profit, -- 毛利额
    1 - cost_price_for / sale_price_new AS profit_rate	 -- 商品毛利率
FROM 
(
    SELECT 
        a.*,
		e.classify_large_code,
        e.classify_large_name,
        e.classify_middle_code,
        e.classify_middle_name,
        e.classify_small_code,
        e.classify_small_name,
        t2.cost_price_for, 
        t2.cost_price_type, -- 最新成本取值来源
        COALESCE(t5.customer_price, t4.customer_price) AS customer_effect_price	
    FROM csx_analyse_tmp.tmp_order_detail01 a
    left join 
        -- -----商品数据
        (select * 
        from csx_dim.csx_dim_basic_goods 
        where sdt='current' 
        ) e 
        on a.goods_code=e.goods_code    
    -- 关联商品成本
     left join 
    (select * 
    from csx_analyse_tmp.csx_analyse_tmp_sale_detail_ky_target_month
    where cost_price_for>0 
    ) t2 
    on a.inventory_dc_code=t2.inventory_dc_code and a.customer_code=t2.customer_code and a.sub_customer_code=t2.sub_customer_code and a.goods_code=t2.goods_code 
    
    -- 关联主客户商品报价
    LEFT JOIN (
        SELECT 
            warehouse_code,
            customer_code,
            product_code,
            MAX(customer_price) AS customer_price  
        FROM csx_dwd.csx_dwd_price_customer_price_guide_di
        WHERE price_begin_time <= CURRENT_DATE 
            AND price_end_time >= CURRENT_DATE 
            AND (sub_customer_code IS NULL OR sub_customer_code = '') 
        GROUP BY 
            warehouse_code,
            customer_code,
            product_code
    ) t4 
        ON a.inventory_dc_code = t4.warehouse_code 
        AND a.customer_code = t4.customer_code 
        AND a.goods_code = t4.product_code 
    
    -- 关联子客户商品报价
    LEFT JOIN (
        SELECT 
            warehouse_code,
            customer_code,
            sub_customer_code,
            product_code,
            MAX(customer_price) AS customer_price  
        FROM csx_dwd.csx_dwd_price_customer_price_guide_di
        WHERE price_begin_time <= CURRENT_DATE 
            AND price_end_time >= CURRENT_DATE 
            AND sub_customer_code IS NOT NULL 
            AND sub_customer_code != ''
        GROUP BY 
            warehouse_code,
            customer_code,
            sub_customer_code,
            product_code
    ) t5 
        ON a.inventory_dc_code = t5.warehouse_code 
        AND a.customer_code = t5.customer_code 
        AND a.sub_customer_code = t5.sub_customer_code 
        AND a.goods_code = t5.product_code
) a;

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
	purchase_qty_new,
	sale_price_newnew,
	cost_price_newnew,
	cost_price_type,
	sale_amt_new,
	cost_amt,
	profit,
	profitlv_new
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
		sum(purchase_qty_new) as purchase_qty_new,	-- 下单量
		sum(sale_amt_new)/sum(purchase_qty_new) as sale_price_newnew, -- 团购价
		sum(cost_amt)/sum(purchase_qty_new) as cost_price_newnew, -- 成本价		
		sum(sale_amt_new) as sale_amt_new, -- 销售额
		sum(cost_amt) as cost_amt, -- 成本总额
		sum(profit) as profit,-- 毛利额
		sum(profit)/sum(sale_amt_new) as profitlv_new --毛利率 
	from csx_analyse_tmp.tmp_order_analysis01
	where cost_price_for>0 and sale_price_new>0
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
) t ;



-- 客户生效区报价

 SELECT 
            warehouse_code,
            customer_code,
			'' as sub_customer_code,
            product_code,
            product_name,
            product_unit,
            MAX(customer_price) AS customer_price  
        FROM csx_dwd.csx_dwd_price_customer_price_guide_di
        WHERE price_begin_time <= CURRENT_DATE 
            AND price_end_time >= CURRENT_DATE 
            AND (sub_customer_code IS NULL OR sub_customer_code = '') 
			AND customer_code='275371'
        GROUP BY 
            warehouse_code,
            customer_code,
            product_code,
            product_name,
            product_unit
   
	union all
    -- 关联子客户商品报价

        SELECT 
            warehouse_code,
            customer_code,
            sub_customer_code,
            product_code,
            product_name,
            product_unit,
            MAX(customer_price) AS customer_price  
        FROM csx_dwd.csx_dwd_price_customer_price_guide_di
        WHERE price_begin_time <= CURRENT_DATE 
            AND price_end_time >= CURRENT_DATE 
            AND sub_customer_code IS NOT NULL 
            AND sub_customer_code != ''
			AND customer_code='275371'
        GROUP BY 
            warehouse_code,
            customer_code,
            sub_customer_code,
            product_code,
            product_name,
            product_unit