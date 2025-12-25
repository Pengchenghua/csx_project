-- 表1：客户商品销售信息

drop table if exists csx_analyse_tmp.csx_city_customer_goods_sale;
create table if not exists csx_analyse_tmp.csx_city_customer_goods_sale as
SELECT
    a.performance_region_name,
    a.performance_province_name,
    a.performance_city_name,
    a.inventory_dc_code,
    a.customer_code,
    d.customer_name,
    d.second_category_name,
	a.sub_customer_code,
	a.sub_customer_name,
    a.goods_code,
    e.goods_name,
    e.classify_middle_name,
    SUM(sale_qty) sale_qty,
    SUM(sale_amt) as sale_amt,
    SUM(sale_cost) sale_cost,
    SUM(profit) as profit,  -- 修正：去掉多余的斜杠
    CASE 
        WHEN ABS(SUM(sale_amt)) = 0 THEN NULL 
        ELSE SUM(profit)/ABS(SUM(sale_amt)) 
    END as profit_rate  -- 增加除零保护
FROM
    (
        SELECT *
        FROM csx_dws.csx_dws_sale_detail_di
        WHERE 
            sdt >= '20250901' and sdt <= '20250930'
            and business_type_code=1  
            and shipper_code='YHCSX' 
            and order_channel_code not in ('4','6','5') -- 剔除所有异常4.返利,5.价格补救,6.调价
    ) a		
    -- -----客户数据
    LEFT JOIN 
        (SELECT * 
        FROM csx_dim.csx_dim_crm_customer_info 
        WHERE sdt='current' 
        and shipper_code='YHCSX'
        ) d
        ON a.customer_code=d.customer_code 
    LEFT JOIN 
        -- -----商品数据
        (SELECT * 
        FROM csx_dim.csx_dim_basic_goods 
        WHERE sdt='current' 
        ) e 
        ON a.goods_code=e.goods_code 	
    LEFT JOIN -- 客户业务类型最早在、最近销售日期
    (SELECT customer_code,business_type_code,MIN(first_business_sale_date) as first_business_sale_date ,MAX(last_business_sale_date) as last_business_sale_date
     FROM csx_dws.csx_dws_crm_customer_business_active_di
     WHERE sdt = 'current'
     GROUP BY customer_code,business_type_code
    ) f ON a.customer_code=f.customer_code and a.business_type_code=f.business_type_code	
    LEFT JOIN 
        (SELECT
            code as type,
            MAX(name) as name,
            MAX(extra) as extra 
        FROM csx_dim.csx_dim_basic_topic_dict_df
        WHERE parent_code = 'direct_delivery_type' 
        GROUP BY code 
        ) h 
        ON a.direct_delivery_type=h.type 
WHERE h.extra='采购参与'		
GROUP BY 
    a.performance_region_name,
    a.performance_province_name,
    a.performance_city_name,
    a.inventory_dc_code,
    a.customer_code,
    d.customer_name,
    d.second_category_name,
	a.sub_customer_code,
	a.sub_customer_name,
    a.goods_code,
    e.goods_name,
    e.classify_middle_name;


-- 表2：客户报价生效区数据
drop table if exists csx_analyse_tmp.csx_city_customer_goods_price;
create table if not exists csx_analyse_tmp.csx_city_customer_goods_price as
SELECT 
	warehouse_code,
	customer_code,
	product_code,
	sub_customer_code,
	customer_price,
	price_begin_time,
	price_end_time,  
	(CASE WHEN price_type=1 THEN '建议售价' 
		WHEN price_type=2 THEN '对标对象' 
		WHEN price_type=3 THEN '销售成本价' 
		WHEN price_type=4 THEN '上一周价格' 
		WHEN price_type=5 THEN '售价' 
		WHEN price_type=6 THEN '采购/库存成本价' 
		WHEN price_type=7 THEN '上期价格' 
	ELSE CAST(price_type AS STRING) END) as price_type, 
	
	-- 修正bmk_code提取逻辑
	(CASE WHEN get_json_object(customer_price_detail,'$.version')='2.0' 
		THEN get_json_object(customer_price_detail,'$.markets[0].marketCode') 
		ELSE get_json_object(customer_price_detail,'$.bmkCode')
	END) as bmk_code,
	
	-- 修正bmk_name提取逻辑  
	(CASE WHEN get_json_object(customer_price_detail,'$.version')='2.0' 
		THEN get_json_object(customer_price_detail,'$.markets[0].marketName') 
		ELSE get_json_object(customer_price_detail,'$.bmkName')
	END) as bmk_name,
	
	-- 直接提取price
	get_json_object(customer_price_detail, '$.price') as price,
	
	CASE WHEN source =0 THEN '中台报价系统'
		WHEN source =1 THEN 'CRM临时报价'
		WHEN source =2 THEN '换品工具'
		WHEN source =3 THEN '自动报价'
		WHEN source =4 THEN '毛利控制中心临时14'
		WHEN source =5 THEN '中台-订单无价格5'
		WHEN source =6 THEN '中台-兜底价'
		WHEN source =7 THEN '订单自动报价'
		WHEN source =8 THEN 'B端标准来单'
	ELSE CAST(source AS STRING) END
	as source
from 
	(select *,
		row_number() over(partition by warehouse_code,customer_code, sub_customer_code,product_code order by update_time desc) r_num
	FROM csx_dwd.csx_dwd_price_customer_price_guide_di
	WHERE substr(price_begin_time, 1, 10) <= current_date
		AND substr(price_end_time, 1, 10) >= current_date
	)a
where r_num =1;
		

-- 表3：关联表1表2，处理：优先取子客户报价，没有子客户报价的取客户报价，left join 方法：	
drop table if exists csx_analyse_tmp.csx_city_customer_goods_all01;
create table if not exists csx_analyse_tmp.csx_city_customer_goods_all01 as		
	SELECT 
    a.performance_region_name,
    a.performance_province_name,
    a.performance_city_name,
    a.inventory_dc_code,
    a.customer_code,
    a.customer_name,
    a.second_category_name,
    CASE 
        WHEN b.sub_customer_code IS NOT NULL THEN a.sub_customer_code
        ELSE ''
    END AS sub_customer_code,
    CASE 
        WHEN b.sub_customer_code IS NOT NULL THEN a.sub_customer_name
        ELSE ''
    END AS sub_customer_name,
    a.goods_code,
    a.goods_name,
    a.classify_middle_name,
    COALESCE(b.customer_price, c.customer_price, '') AS customer_price,
    COALESCE(b.price_begin_time, c.price_begin_time, '') AS price_begin_time,
    COALESCE(b.price_end_time, c.price_end_time, '') AS price_end_time,
    COALESCE(b.price_type, c.price_type, '') AS price_type,
    COALESCE(b.bmk_code, c.bmk_code, '') AS bmk_code,
    COALESCE(b.bmk_name, c.bmk_name, '') AS bmk_name,
    COALESCE(b.source, c.source, '') AS source,
    SUM(sale_qty) sale_qty,
	SUM(sale_amt) as sale_amt,
	SUM(sale_cost) sale_cost,
	SUM(profit) as profit,
	SUM(profit) /SUM(sale_amt)  profit_rate	
FROM csx_analyse_tmp.csx_city_customer_goods_sale a
-- 左联具体子客户报价（优先级最高）
LEFT JOIN csx_analyse_tmp.csx_city_customer_goods_price b 
    ON a.inventory_dc_code = b.warehouse_code 
    AND a.customer_code = b.customer_code 
    AND a.sub_customer_code = b.sub_customer_code 
    AND a.goods_code = b.product_code
    AND (b.sub_customer_code IS NOT NULL OR b.sub_customer_code <>'')
-- 左联客户级别报价（当没有具体子客户报价时使用）
LEFT JOIN csx_analyse_tmp.csx_city_customer_goods_price c 
    ON a.inventory_dc_code = c.warehouse_code 
    AND a.customer_code = c.customer_code 
    AND a.goods_code = c.product_code
    AND (c.sub_customer_code IS NULL OR c.sub_customer_code ='')
    AND b.warehouse_code IS NULL  -- 确保没有子客户报价时才使用客户报价
group by    
    a.performance_region_name,
    a.performance_province_name,
    a.performance_city_name,
    a.inventory_dc_code,
    a.customer_code,
    a.customer_name,
    a.second_category_name,
    CASE 
        WHEN b.sub_customer_code IS NOT NULL THEN a.sub_customer_code
        ELSE ''
    END,
    CASE 
        WHEN b.sub_customer_code IS NOT NULL THEN a.sub_customer_name
        ELSE ''
    END,
    a.goods_code,
    a.goods_name,
    a.classify_middle_name,
    COALESCE(b.customer_price, c.customer_price, ''),
    COALESCE(b.price_begin_time, c.price_begin_time, ''),
    COALESCE(b.price_end_time, c.price_end_time, ''),
    COALESCE(b.price_type, c.price_type, ''),
    COALESCE(b.bmk_code, c.bmk_code, ''),
    COALESCE(b.bmk_name, c.bmk_name, ''),
    COALESCE(b.source, c.source, '');




	
/*	
drop table if exists csx_analyse_tmp.csx_city_customer_goods_sale;
create table if not exists csx_analyse_tmp.csx_city_customer_goods_sale as
SELECT
    a.performance_region_name,
    a.performance_province_name,
    a.performance_city_name,
    a.inventory_dc_code,
    a.customer_code,
    d.customer_name,
    d.second_category_name,
	a.sub_customer_code,
	a.sub_customer_name,
    a.goods_code,
    e.goods_name,
    e.classify_middle_name,
    i.customer_price,
    i.price_begin_time,
    i.price_end_time,
    i.price_type,
    i.bmk_code,
    i.bmk_name,
    i.source,
    SUM(sale_qty) sale_qty,
    SUM(sale_amt) as sale_amt,
    SUM(sale_cost) sale_cost,
    SUM(profit) as profit,  -- 修正：去掉多余的斜杠
    CASE 
        WHEN ABS(SUM(sale_amt)) = 0 THEN NULL 
        ELSE SUM(profit)/ABS(SUM(sale_amt)) 
    END as profit_rate  -- 增加除零保护
FROM
    (
        SELECT *
        FROM csx_dws.csx_dws_sale_detail_di
        WHERE 
            sdt >= '20250901' and sdt <= '20250930'
            and business_type_code=1  
            and shipper_code='YHCSX' 
            and order_channel_code not in ('4','6','5') -- 剔除所有异常4.返利,5.价格补救,6.调价
    ) a		
    -- -----客户数据
    LEFT JOIN 
        (SELECT * 
        FROM csx_dim.csx_dim_crm_customer_info 
        WHERE sdt='current' 
        and shipper_code='YHCSX'
        ) d
        ON a.customer_code=d.customer_code 
    LEFT JOIN 
        -- -----商品数据
        (SELECT * 
        FROM csx_dim.csx_dim_basic_goods 
        WHERE sdt='current' 
        ) e 
        ON a.goods_code=e.goods_code 	
    LEFT JOIN -- 客户业务类型最早在、最近销售日期
    (SELECT customer_code,business_type_code,MIN(first_business_sale_date) as first_business_sale_date ,MAX(last_business_sale_date) as last_business_sale_date
     FROM csx_dws.csx_dws_crm_customer_business_active_di
     WHERE sdt = 'current'
     GROUP BY customer_code,business_type_code
    ) f ON a.customer_code=f.customer_code and a.business_type_code=f.business_type_code	
    LEFT JOIN 
        (SELECT
            code as type,
            MAX(name) as name,
            MAX(extra) as extra 
        FROM csx_dim.csx_dim_basic_topic_dict_df
        WHERE parent_code = 'direct_delivery_type' 
        GROUP BY code 
        ) h 
        ON a.direct_delivery_type=h.type 
    LEFT JOIN	-- 客户报价生效区数据
        (SELECT 
			-- 客户报价生效区数据
			warehouse_code,
			customer_code,
			product_code,
			sub_customer_code,
			customer_price,
			price_begin_time,
			price_end_time,  
			(CASE WHEN price_type=1 THEN '建议售价' 
				WHEN price_type=2 THEN '对标对象' 
				WHEN price_type=3 THEN '销售成本价' 
				WHEN price_type=4 THEN '上一周价格' 
				WHEN price_type=5 THEN '售价' 
				WHEN price_type=6 THEN '采购/库存成本价' 
				WHEN price_type=7 THEN '上期价格' 
			ELSE CAST(price_type AS STRING) END) as price_type, 
			
			-- 修正bmk_code提取逻辑
			(CASE WHEN get_json_object(customer_price_detail,'$.version')='2.0' 
				THEN get_json_object(customer_price_detail,'$.markets[0].marketCode') 
				ELSE get_json_object(customer_price_detail,'$.bmkCode')
			END) as bmk_code,
			
			-- 修正bmk_name提取逻辑  
			(CASE WHEN get_json_object(customer_price_detail,'$.version')='2.0' 
				THEN get_json_object(customer_price_detail,'$.markets[0].marketName') 
				ELSE get_json_object(customer_price_detail,'$.bmkName')
			END) as bmk_name,
			
			-- 直接提取price
			get_json_object(customer_price_detail, '$.price') as price,
			
			(CASE WHEN source= 4 THEN '中台报价系统'
				WHEN source= 5 THEN 'CRM临时报价'
				WHEN source= 6 THEN '换品工具'
				WHEN source= 7 THEN '自动报价'
				WHEN source= 8 THEN '毛利控制中心临时14'
				WHEN source= 9 THEN '中台-订单无价格5'
				WHEN source= 10 THEN '中台-兜底价'
				WHEN source= 27 THEN '订单自动报价'
				WHEN source= 71 THEN 'B端标准来单'
			ELSE CAST(source AS STRING) END
			) as source
		FROM csx_dwd.csx_dwd_price_customer_price_guide_di
		WHERE substr(price_begin_time, 1, 10) <= current_date
			AND substr(price_end_time, 1, 10) >= current_date
        ) i ON a.inventory_dc_code=i.warehouse_code and a.customer_code=i.customer_code and a.sub_customer_code=i.sub_customer_code and a.goods_code =i.product_code
WHERE h.extra='采购参与'		
GROUP BY 
    a.performance_region_name,
    a.performance_province_name,
    a.performance_city_name,
    a.inventory_dc_code,
    a.customer_code,
    d.customer_name,
    d.second_category_name,
	a.sub_customer_code,
	a.sub_customer_name,
    a.goods_code,
    e.goods_name,
    e.classify_middle_name,
    i.customer_price,
    i.price_begin_time,
    i.price_end_time,
    i.price_type,
    i.bmk_code,
    i.bmk_name,
    i.source;
	
-- join ，union all 方法：	
drop table if exists csx_analyse_tmp.csx_city_customer_goods_all;
create table if not exists csx_analyse_tmp.csx_city_customer_goods_all as	
	select 
		a.performance_region_name,
		a.performance_province_name,
		a.performance_city_name,
		a.inventory_dc_code,
		a.customer_code,
		a.customer_name,
		a.second_category_name,
		a.sub_customer_code,
		a.sub_customer_name,
		a.goods_code,
		a.goods_name,
		a.classify_middle_name,
		b.customer_price,
		b.price_begin_time,
		b.price_end_time,
		b.price_type,
		b.bmk_code,
		b.bmk_name,
		b.source,
		SUM(sale_qty) sale_qty,
		SUM(sale_amt) as sale_amt,
		SUM(sale_cost) sale_cost,
		SUM(profit) as profit,
		SUM(profit) /SUM(sale_amt)  profit_rate		
	from csx_analyse_tmp.csx_city_customer_goods_sale a
    join 
	(select * from csx_analyse_tmp.csx_city_customer_goods_price
	where sub_customer_code is not null ) b  on a.inventory_dc_code=b.warehouse_code and a.customer_code=b.customer_code and a.sub_customer_code=b.sub_customer_code and a.goods_code =b.product_code
    group by 
		a.performance_region_name,
		a.performance_province_name,
		a.performance_city_name,
		a.inventory_dc_code,
		a.customer_code,
		a.customer_name,
		a.second_category_name,
		a.sub_customer_code,
		a.sub_customer_name,
		a.goods_code,
		a.goods_name,
		a.classify_middle_name,
		b.customer_price,
		b.price_begin_time,
		b.price_end_time,
		b.price_type,
		b.bmk_code,
		b.bmk_name,
		b.source
      
    union all
	
	select 
		a.performance_region_name,
		a.performance_province_name,
		a.performance_city_name,
		a.inventory_dc_code,
		a.customer_code,
		a.customer_name,
		a.second_category_name,
		'' as sub_customer_code,
		'' as sub_customer_name,
		a.goods_code,
		a.goods_name,
		a.classify_middle_name,
		b.customer_price,
		b.price_begin_time,
		b.price_end_time,
		b.price_type,
		b.bmk_code,
		b.bmk_name,
		b.source,
		SUM(sale_qty) sale_qty,
		SUM(sale_amt) as sale_amt,
		SUM(sale_cost) sale_cost,
		SUM(profit) as profit,
		SUM(profit) /SUM(sale_amt)  profit_rate		
	from csx_analyse_tmp.csx_city_customer_goods_sale a
    join 
	(select * from csx_analyse_tmp.csx_city_customer_goods_price
	where sub_customer_code ='' or sub_customer_code is null ) b  on a.inventory_dc_code=b.warehouse_code and a.customer_code=b.customer_code and a.goods_code =b.product_code
	group by
		a.performance_region_name,
		a.performance_province_name,
		a.performance_city_name,
		a.inventory_dc_code,
		a.customer_code,
		a.customer_name,
		a.second_category_name,
		a.goods_code,
		a.goods_name,
		a.classify_middle_name,
		b.customer_price,
		b.price_begin_time,
		b.price_end_time,
		b.price_type,
		b.bmk_code,
		b.bmk_name,
		b.source


    union all

	select 
		a.performance_region_name,
		a.performance_province_name,
		a.performance_city_name,
		a.inventory_dc_code,
		a.customer_code,
		a.customer_name,
		a.second_category_name,
		'' as sub_customer_code,
		'' as sub_customer_name,
		a.goods_code,
		a.goods_name,
		a.classify_middle_name,
		'' as customer_price,
		null as price_begin_time,
		null as price_end_time,
		'' as price_type,
		'' as bmk_code,
		'' as bmk_name,
		'' as source,
		SUM(sale_qty) sale_qty,
		SUM(sale_amt) as sale_amt,
		SUM(sale_cost) sale_cost,
		SUM(profit) as profit,
		SUM(profit) /SUM(sale_amt)  profit_rate		
	from csx_analyse_tmp.csx_city_customer_goods_sale a
    left join 
	(select * from csx_analyse_tmp.csx_city_customer_goods_price
	where sub_customer_code is not null ) b 
	on a.inventory_dc_code=b.warehouse_code and a.customer_code=b.customer_code and a.goods_code =b.product_code and a.sub_customer_code=b.sub_customer_code 
    left join 
	(select * from csx_analyse_tmp.csx_city_customer_goods_price
	where sub_customer_code ='' or sub_customer_code is null) c 
	on a.inventory_dc_code=c.warehouse_code and a.customer_code=c.customer_code and a.goods_code =c.product_code 
	where b.warehouse_code is null and c.warehouse_code is null
	group by
		a.performance_region_name,
		a.performance_province_name,
		a.performance_city_name,
		a.inventory_dc_code,
		a.customer_code,
		a.customer_name,
		a.second_category_name,
		a.goods_code,
		a.goods_name,
		a.classify_middle_name;


