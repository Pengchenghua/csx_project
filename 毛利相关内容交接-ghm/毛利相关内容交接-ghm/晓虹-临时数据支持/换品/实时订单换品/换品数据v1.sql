-- ******************************************************************** 
-- @功能描述：
-- @创建者： 公会敏 
-- @创建者日期：2025-10-24 11:30:22 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 
-- 手工表：
-- 换品清单，csx_analyse_tmp.tmp_change_goods
-- 客服经理，csx_analyse_tmp.tmp_service_leader_name_list


-- 成本数据
drop table if exists csx_analyse_tmp.csx_analyse_tmp_cb;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_cb as 
select 
    a.* 
from 
    (select 
        t1.*,
        row_number()over(partition by t1.performance_city_name,t1.goods_code order by t1.create_time desc) as pm
    from 
          (select 
              c.performance_city_name,
              a.target_location_code as dc_code,
              a.goods_code,
              cast(a.price_include_tax as decimal(20,6)) as price,
              substr(a.create_time,1,19) as create_time 
          from 
                (select 
                    *  
                from csx_dws.csx_dws_scm_order_detail_di 
                where sdt>=regexp_replace(add_months(trunc('${yes_date}','MM'),-3),'-','') 
                and sdt<=regexp_replace('${yes_date}','-','')  
                and super_class in (1,3)  
                and assign_type<>1 -- 剔除客户指定数据 
                and (direct_delivery_type not in (1,2) or direct_delivery_type is null) -- 剔除RD/ZZ的数据
                and source_type not in (20,21) -- 剔除紧急补货及临时加单数据
                and delivery_to_direct_flag<>1 -- 剔除配转直（发车前缺货数据）
                and price_remedy_flag<>1 -- 剔除价格补救单 
                and header_status<>5 -- 剔除“已取消”订单 
                and shipper_code='YHCSX' 
                and target_location_code not in ('W0BD','WC51') 
                and price_type<>2 
                and source_type in ('1','10','19', '23','9') 
                and price_include_tax>0.1
                ) a 
                left join 
                (select * 
                from csx_dim.csx_dim_basic_goods 
                where sdt='current' 
                ) b 
                on a.goods_code=b.goods_code 
                left join 
                (select 
                    performance_region_name,
                    performance_province_name,
                    performance_city_name,
                    shop_code as dc_code,
                    shop_name as dc_name,
                    warehouse_purpose_name,
                    (case when warehouse_status=1 then '开启' 
                          when warehouse_status=2 then '禁用' 
                    end) as warehouse_status_name 
                from csx_dim.csx_dim_shop 
                where sdt='current' 
                and warehouse_purpose_name in ('大客户物流','工厂') 
                ) c 
                on a.target_location_code=c.dc_code 
          where 
          c.dc_code is not null 
          and 
          (
              (b.division_code in (10,11) and date(a.create_time)>=date_add(from_unixtime(unix_timestamp('${yes_sdt}','yyyyMMdd'),'yyyy-MM-dd'),-14) and date(a.create_time)<=date_add(from_unixtime(unix_timestamp('${yes_sdt}','yyyyMMdd'),'yyyy-MM-dd'),-1)) -- 生鲜取近7天最后一次入库数据
              or 
              (b.division_code not in (10,11) and date(a.create_time)>=date_add(from_unixtime(unix_timestamp('${yes_sdt}','yyyyMMdd'),'yyyy-MM-dd'),-60) and date(a.create_time)<=date_add(from_unixtime(unix_timestamp('${yes_sdt}','yyyyMMdd'),'yyyy-MM-dd'),-1)) -- 食百取近30天最后一次入库数据
          ) 
        ) t1 
    ) a 
where a.pm=1;

-- 订单商品数据
drop table if exists csx_analyse_tmp.tmp_order_detail;
create table if not exists csx_analyse_tmp.tmp_order_detail as 	
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
    f.rp_service_user_work_no_new,
    f.rp_service_user_name_new,
	g.service_leader_name,
    a.goods_code,
    e.goods_name,
	e.unit_name,
	purchase_qty_new,
	sale_price_new,
    sale_amt_new,	
	j.goods_code_b,
	h.goods_name as goods_name_b,
	h.unit_name as 	unit_name_b
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
    WHERE sdt = '${today}' 
         AND order_status = 'CUTTED'
         AND delivery_flag not in (1,2) -- 零星补货、客户自购
	     AND delivery_type_code = 0
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
	) f  on a.customer_code=f.customer_no
	
	-- -----服务管家经理
	left join csx_analyse_tmp.tmp_service_leader_name_list g on d.performance_city_name = g.performance_city_name	
	
	-- -----只取换品表里的数据
	join csx_analyse_tmp.tmp_change_goods j on a.inventory_dc_code= j.inventory_dc_code and a.goods_code=j.goods_code_a
	
	left join 
	-- -----商品数据
	(select * 
	from csx_dim.csx_dim_basic_goods 
	where sdt='current' 
	) h on h.goods_code=j.goods_code_b;
	

-- 结果数据
drop table if exists csx_analyse_tmp.tmp_order_analysis;
create table if not exists csx_analyse_tmp.tmp_order_analysis as 	
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
    rp_service_user_work_no_new,
    rp_service_user_name_new,
    service_leader_name, 
    goods_code,
    goods_name,
    unit_name,
    purchase_qty_new, -- 下单量
    sale_price_new,  -- 团购价
    cost_price_a, -- 成本价
    sale_amt_new,	   -- 金额
    purchase_qty_new * (sale_price_new - cost_price_a) AS profit, -- 毛利额
    1 - cost_price_a / sale_price_new AS profit_rate,	 -- 商品a毛利率
    
    goods_code_b,
    goods_name_b,
    unit_name_b,
    customer_effect_price_b, -- 商品b生效区客户报价
    customer_price_b,    -- 商品b价格
    customer_price_b_source, -- 商品b价格来源
    cost_price_b, -- 商品b成本价
    purchase_qty_new * customer_price_b AS sale_amt_new_b, -- 商品b销售额
    purchase_qty_new * (customer_price_b - cost_price_b) AS profit_b, -- 商品b毛利额
    1 - cost_price_b / customer_price_b AS profit_rate_b,    -- 商品b毛利率
    (1 - cost_price_b / customer_price_b) - (1 - cost_price_a / sale_price_new) AS profit_rate_diff -- 商品b、a毛利率差
FROM (
    SELECT 
        a.*,
        t2.price AS cost_price_a, -- 商品a成本
        t3.price AS cost_price_b,  -- 商品b成本 
        COALESCE(t5.customer_price, t4.customer_price) AS customer_effect_price_b, -- 商品b生效区价格
        
        -- 商品b售价
        CASE 
            WHEN goods_code_b IN ('846778','1065579','1140418','620','1065604') 
            THEN a.sale_price_new
            ELSE COALESCE(t5.customer_price, t4.customer_price)
        END AS customer_price_b,  
        
        CASE 
            WHEN goods_code_b IN ('846778','1065579','1140418','620','1065604') 
            THEN '取商品A售价'
            ELSE '生效区客户报价'
        END AS customer_price_b_source  	
    FROM csx_analyse_tmp.tmp_order_detail a
    
    -- 关联商品a成本
    LEFT JOIN csx_analyse_tmp.csx_analyse_tmp_cb t2 
        ON a.performance_city_name = t2.performance_city_name 
        AND a.goods_code = t2.goods_code 
    
    -- 关联商品b成本
    LEFT JOIN csx_analyse_tmp.csx_analyse_tmp_cb t3 
        ON a.performance_city_name = t3.performance_city_name 
        AND a.goods_code_b = t3.goods_code 
    
    -- 关联主客户商品b报价
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
        AND a.goods_code_b = t4.product_code 
    
    -- 关联子客户商品b报价
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
        AND a.goods_code_b = t5.product_code
) a;



-- 17点更新数据
INSERT OVERWRITE TABLE csx_analyse.csx_analyse_fr_order_change_goods_hf PARTITION (shf)
SELECT 
    *,
    '${today}' as sdt,
    IF(hour(from_unixtime(unix_timestamp())) < 21,  CONCAT('${today}','','17'), CONCAT('${today}','','21')) as shf
FROM csx_analyse_tmp.tmp_order_analysis;













CREATE TABLE csx_analyse.csx_analyse_fr_order_change_goods_hf (
    `performance_region_name` STRING COMMENT '大区',
    `performance_province_name` STRING COMMENT '省区',
    `performance_city_name` STRING COMMENT '城市',
    `inventory_dc_code` STRING COMMENT '仓库编码',
    `order_code` STRING COMMENT '订单编号',
    `customer_code` STRING COMMENT '客户编码',
    `customer_name` STRING COMMENT '客户名称',
    `sub_customer_code` STRING COMMENT '子客户编码',
    `sub_customer_name` STRING COMMENT '子客户名称',
    `rp_service_user_work_no_new` STRING COMMENT '服务管家工号',
    `rp_service_user_name_new` STRING COMMENT '服务管家姓名',
    `service_leader_name` STRING COMMENT '客服经理姓名',
    `goods_code` STRING COMMENT '商品A编码',
    `goods_name` STRING COMMENT '商品A名称',
    `unit_name` STRING COMMENT '商品A单位',
    `purchase_qty_new` DECIMAL(18,4) COMMENT '商品A下单量',
    `sale_price_new` DECIMAL(18,4) COMMENT '商品A团购价',
    `cost_price_a` DECIMAL(18,4) COMMENT '商品A成本价',
    `sale_amt_new` DECIMAL(18,4) COMMENT '商品A销售额',
    `profit` DECIMAL(18,4) COMMENT '商品A毛利额',
    `profit_rate` DECIMAL(18,6) COMMENT '商品A毛利率',
    `goods_code_b` STRING COMMENT '商品B编码',
    `goods_name_b` STRING COMMENT '商品B名称',
    `unit_name_b` STRING COMMENT '商品B单位',
    `customer_effect_price_b` DECIMAL(18,4) COMMENT '商品B生效区客户报价',
    `customer_price_b` DECIMAL(18,4) COMMENT '商品B售价',
    `customer_price_b_source` STRING COMMENT '商品B售价来源',
    `cost_price_b` DECIMAL(18,4) COMMENT '商品B成本价',
    `sale_amt_new_b` DECIMAL(18,4) COMMENT '商品B销售额',
    `profit_b` DECIMAL(18,4) COMMENT '商品B毛利额',
    `profit_rate_b` DECIMAL(18,6) COMMENT '商品B毛利率',
    `profit_rate_diff` DECIMAL(18,6) COMMENT '商品B、A毛利率差'
) COMMENT '订单换品数据												'
PARTITIONED BY (`shf` STRING COMMENT '小时分区')
STORED AS ORC
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'transactional' = 'false'
);

-- 17点更新数据
INSERT OVERWRITE TABLE csx_analyse.csx_analyse_fr_order_change_goods_hf  PARTITION (shf)
SELECT 
	*,
	'17' as shf
FROM csx_analyse_tmp.tmp_order_analysis;

-- 21点更新数据  
INSERT OVERWRITE TABLE csx_analyse.csx_analyse_fr_order_change_goods_hf  PARTITION (shf)
SELECT 
	*,
	'21' as shf
FROM csx_analyse_tmp.tmp_order_analysis 


/*
select 
	performance_region_name,
    performance_province_name,
    performance_city_name,
    inventory_dc_code,
	order_code,
    customer_code,
    customer_name,
    sub_customer_code,
    sub_customer_name,
    rp_service_user_work_no_new,
    rp_service_user_name_new,
	service_leader_name, 
    goods_code,
    goods_name,
	unit_name,
	purchase_qty_new, -- 下单量
	sale_price_new,  -- 团购价
	cost_price_a, -- 成本价
	sale_amt_new,	   -- 金额
	purchase_qty_new * (sale_price_new-cost_price_a) as profit, -- 毛利额
	1-cost_price_a/sale_price_new as profit_rate,	 -- 商品a 毛利率
	
	goods_code_b,
	goods_name_b,
	unit_name_b,
	customer_effect_price_b, -- 商品b 生效区客户报价
	customer_price_b,    -- 商品b 价格
	customer_price_b_source, -- 商品b 价格来源
	cost_price_b, -- 商品b 成本价
	purchase_qty_new*customer_price_b as sale_amt_new_b, -- 商品b 销售额，销量用商品a的
	purchase_qty_new*(customer_price_b-cost_price_b) as profit_b, -- 商品b毛利额
	1-cost_price_b/customer_price_b as profit_rate_b,    -- 商品b 毛利率
    (1-cost_price_b/customer_price_b)- (1-cost_price_a/sale_price_new) as profit_rate_diff --商品b、a毛利率差
(select 
	a.*,
	t2.price as cost_price_a , -- 商品a成本
	t3.price as cost_price_b,  -- 商品b成本 
	nvl(t5.customer_price,t4.customer_price) as customer_effect_price_b,-- 商品b生效区价格
	-- 商品b售价
    if(goods_code_b in ('846778','1065579','1140418','620','1065604'), sale_price,nvl(t5.customer_price,t4.customer_price)) as customer_price_b,  
    if(goods_code_b in ('846778','1065579','1140418','620','1065604'), '取商品A售价','生效区客户报价') as customer_price_b_source  	

from csx_analyse_tmp.tmp_order_detail a

-- 关联客户下期成本 -- a商品成本
left join  csx_analyse_tmp.csx_analyse_tmp_cb t2 on a.performance_city_name=t2.performance_city_name and a.goods_code=t2.goods_code 
 
-- 关联客户下期成本  -- -- b商品成本
left join  csx_analyse_tmp.csx_analyse_tmp_cb t3 on a.performance_city_name=t3.performance_city_name and a.goods_code_b=t3.goods_code 
 
-- 关联目前生效区主客户商品报价数据  -- b商品的生效区客户报价
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
    ) t4 
    on a.inventory_dc_code=t4.warehouse_code and a.customer_code=t4.customer_code and a.goods_code_b=t4.product_code 
    -- 关联目前生效区子客户商品报价数据 -- b商品的生效区客户报价
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
    ) t5 
	on a.inventory_dc_code=t5.warehouse_code and a.customer_code=t5.customer_code and a.sub_customer_code=t5.sub_customer_code and a.goods_code_b=t5.product_code
)a ;


select 
	a.*
	,1-cost_price_a/sale_price_new as profit_rate_a,
	purchase_qty_new*customer_price_b as sale_amt_new_b,
	1-cost_price_b/customer_price_b as profit_rate_b,
    (1-cost_price_b/customer_price_b)- (1-cost_price_a/sale_price_new) as profit_rate_diff
from 
(SELECT 
    d.performance_region_name,
    d.performance_province_name,
    d.performance_city_name,
    a.inventory_dc_code,
    a.order_code,
    a.customer_code,
    d.customer_name,
    a.sub_customer_code,
    a.sub_customer_name,
    f.rp_service_user_work_no_new,
    f.rp_service_user_name_new,
    a.goods_code,
    e.goods_name,
    e.unit_name,
    a.purchase_qty_new,
    a.sale_price_new,
    a.sale_amt_new,	
    j.goods_code_b,
    h.goods_name AS goods_name_b,
    h.unit_name AS unit_name_b,
    t2.price AS cost_price_a, 
    t3.price AS cost_price_b, 	
    CASE 
        WHEN j.goods_code_b IN ('846778','1065579','1140418','620','1065604') 
            THEN a.sale_price_new
        ELSE CAST(COALESCE(t5.customer_price, t4.customer_price) AS DECIMAL(11,3))
    END AS customer_price_b
FROM 
    (SELECT 
        *,
        CAST(purchase_qty AS DECIMAL(11, 3)) * CAST(purchase_unit_rate AS DECIMAL(11, 3)) AS purchase_qty_new,
        CAST(sale_price AS DECIMAL(11, 3)) AS sale_price_new,
        CAST(CASE 
             WHEN order_status = 'CANCELLED' THEN CAST(0 AS DECIMAL(11, 3))
             WHEN order_status = 'HOME' THEN CAST(sign_qty AS DECIMAL(11, 3)) * CAST(sale_price AS DECIMAL(11, 3))
             WHEN order_status = 'STOCKOUT' THEN CAST(sign_qty AS DECIMAL(11, 3)) * CAST(sale_price AS DECIMAL(11, 3))
        ELSE CAST(purchase_qty AS DECIMAL(11, 3)) * CAST(purchase_unit_rate AS DECIMAL(11, 3)) * CAST(sale_price AS DECIMAL(11, 3))
        END AS DECIMAL(11, 2)) AS sale_amt_new	
    FROM csx_dwd.csx_dwd_csms_yszx_order_detail_di
    WHERE sdt = '${today}' 
         AND order_status = 'CUTTED'
         AND delivery_flag not in (1,2) -- 零星补货、客户自购
	    AND delivery_type_code = 0
    ) a
LEFT JOIN 
    (SELECT * 
    FROM csx_dim.csx_dim_crm_customer_info 
    WHERE sdt = 'current' 
    AND shipper_code = 'YHCSX'
    ) d ON a.customer_code = d.customer_code 
LEFT JOIN 
    (SELECT * 
    FROM csx_dim.csx_dim_basic_goods 
    WHERE sdt = 'current' 
    ) e ON a.goods_code = e.goods_code 
LEFT JOIN 
    (SELECT * 
    FROM csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df    
    WHERE sdt = '${yes_sdt}' 
    ) f ON a.customer_code = f.customer_no
-- 关联换品表
JOIN csx_analyse_tmp.tmp_change_goods j 
    ON a.inventory_dc_code = j.inventory_dc_code 
    AND a.goods_code = j.goods_code_a
-- 关联商品B信息
LEFT JOIN 
    (SELECT * 
    FROM csx_dim.csx_dim_basic_goods 
    WHERE sdt = 'current' 
    ) h ON h.goods_code = j.goods_code_b
-- 关联商品A成本
LEFT JOIN csx_analyse_tmp.csx_analyse_tmp_cb t2 
    ON d.performance_city_name = t2.performance_city_name 
    AND a.goods_code = t2.goods_code 
-- 关联商品B成本
LEFT JOIN csx_analyse_tmp.csx_analyse_tmp_cb t3 
    ON d.performance_city_name = t3.performance_city_name 
    AND j.goods_code_b = t3.goods_code 
-- 关联主客户商品B报价
LEFT JOIN 
    (SELECT 
        warehouse_code,
        customer_code,
        product_code,
        MAX(CAST(customer_price AS DECIMAL(11,3))) AS customer_price  
    FROM csx_dwd.csx_dwd_price_customer_price_guide_di
    WHERE CAST(price_begin_time AS STRING) <= CAST(CURRENT_DATE() AS STRING)
        AND CAST(price_end_time AS STRING) >= CAST(CURRENT_DATE() AS STRING)
        AND (sub_customer_code IS NULL OR LENGTH(sub_customer_code) = 0) 
    GROUP BY warehouse_code, customer_code, product_code
    ) t4 ON a.inventory_dc_code = t4.warehouse_code 
        AND a.customer_code = t4.customer_code 
        AND j.goods_code_b = t4.product_code 
-- 关联子客户商品B报价
LEFT JOIN 
    (SELECT 
        warehouse_code,
        customer_code,
        sub_customer_code,
        product_code,
        MAX(CAST(customer_price AS DECIMAL(11,3))) AS customer_price  
    FROM csx_dwd.csx_dwd_price_customer_price_guide_di
    WHERE CAST(price_begin_time AS STRING) <= CAST(CURRENT_DATE() AS STRING)
        AND CAST(price_end_time AS STRING) >= CAST(CURRENT_DATE() AS STRING)
        AND sub_customer_code IS NOT NULL 
        AND LENGTH(sub_customer_code) > 0
    GROUP BY warehouse_code, customer_code, sub_customer_code, product_code
    ) t5 ON a.inventory_dc_code = t5.warehouse_code 
        AND a.customer_code = t5.customer_code 
        AND a.sub_customer_code = t5.sub_customer_code 
        AND j.goods_code_b = t5.product_code
)a 
where purchase_qty_new >=10 and sale_price_new is not null and customer_price_b is not null 
having (1-cost_price_b/customer_price_b)- (1-cost_price_a/sale_price_new)>0












 

 -- 创建主表（按小时分区，存储当天数据）
DROP TABLE IF EXISTS csx_analyse_tmp.tmp_order_analysis_daily;
CREATE TABLE IF NOT EXISTS csx_analyse_tmp.tmp_order_analysis_daily (
    `performance_region_name` STRING COMMENT '绩效区域名称',
    `performance_province_name` STRING COMMENT '绩效省份名称',
    `performance_city_name` STRING COMMENT '绩效城市名称',
    `inventory_dc_code` STRING COMMENT '库存仓库编码',
    `order_code` STRING COMMENT '订单编码',
    `customer_code` STRING COMMENT '客户编码',
    `customer_name` STRING COMMENT '客户名称',
    `sub_customer_code` STRING COMMENT '子客户编码',
    `sub_customer_name` STRING COMMENT '子客户名称',
    `rp_service_user_work_no_new` STRING COMMENT '服务管家工号',
    `rp_service_user_name_new` STRING COMMENT '服务管家姓名',
    `classify_large_code` STRING COMMENT '管理大类编码',
    `classify_large_name` STRING COMMENT '管理大类名称',
    `classify_middle_code` STRING COMMENT '管理中类编码',
    `classify_middle_name` STRING COMMENT '管理中类名称',
    `classify_small_code` STRING COMMENT '管理小类编码',
    `classify_small_name` STRING COMMENT '管理小类名称',
    `goods_code` STRING COMMENT '商品A编码',
    `goods_name` STRING COMMENT '商品A名称',
    `unit_name` STRING COMMENT '商品A单位名称',
    `goods_code_b` STRING COMMENT '商品B编码',
    `purchase_qty_unit` DECIMAL(20,3) COMMENT '采购数量(单位)',
    `sale_price` DECIMAL(11,3) COMMENT '销售单价',
    `sale_amt` DECIMAL(11,2) COMMENT '销售金额',
    `cost_price_a` DECIMAL(20,6) COMMENT '商品A成本价',
    `cost_price_b` DECIMAL(20,6) COMMENT '商品B成本价',
    `customer_price` DECIMAL(20,6) COMMENT '客户报价',
    `process_date` STRING COMMENT '处理日期'
) COMMENT '订单分析日表-换品分析'
PARTITIONED BY (sdt STRING COMMENT '分区日期', hour STRING COMMENT '小时分区')
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

-- 创建数据插入脚本（17点执行）
INSERT OVERWRITE TABLE csx_analyse_tmp.tmp_order_analysis_daily PARTITION(sdt='${date}', hour='17')
SELECT 
    a.*,
    t2.cost_price_for, 
    t3.cost_price_for as cost_price_for_b,     
    COALESCE(t5.customer_price, t4.customer_price) as customer_price,
    '17' as process_hour,
    '${date}' as process_date
FROM (
    SELECT 
        d.performance_region_name,
        d.performance_province_name,
        d.performance_city_name,
        a.inventory_dc_code,
        a.order_code,
        a.customer_code,
        d.customer_name,
        a.sub_customer_code,
        MAX(a.sub_customer_name) as sub_customer_name,
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
        j.goods_code_b,
        SUM(purchase_qty_new) purchase_qty_unit,
        SUM(sale_amt_new)/NULLIF(SUM(purchase_qty_new),0) as sale_price,
        SUM(sale_amt_new) as sale_amt
    FROM 
        (SELECT 
            *,
            CAST(purchase_qty AS DECIMAL(11, 3)) * CAST(purchase_unit_rate AS DECIMAL(11, 3)) as purchase_qty_new,
            CAST(sale_price AS DECIMAL(11, 3)) as sale_price_new,
            CAST(CASE 
                 WHEN order_status = 'CANCELLED' THEN CAST(0 AS DECIMAL(11, 3))
                 WHEN order_status = 'HOME' THEN CAST(sign_qty AS DECIMAL(11, 3)) * CAST(sale_price AS DECIMAL(11, 3))
                 WHEN order_status = 'STOCKOUT' THEN CAST(sign_qty AS DECIMAL(11, 3)) * CAST(sale_price AS DECIMAL(11, 3))
                 ELSE CAST(purchase_qty AS DECIMAL(11, 3)) * CAST(purchase_unit_rate AS DECIMAL(11, 3)) * CAST(sale_price AS DECIMAL(11, 3))
            END AS DECIMAL(11, 2)) as sale_amt_new
        FROM csx_dwd.csx_dwd_csms_yszx_order_detail_di
        WHERE sdt = '${date}' 
             AND order_status = 'CUTTED'
        ) a
    LEFT JOIN 
        (SELECT * 
        FROM csx_dim.csx_dim_crm_customer_info 
        WHERE sdt = 'current' 
        AND shipper_code = 'YHCSX'
        ) d ON a.customer_code = d.customer_code 
    LEFT JOIN 
        (SELECT * 
        FROM csx_dim.csx_dim_basic_goods 
        WHERE sdt = 'current' 
        ) e ON a.goods_code = e.goods_code 
    LEFT JOIN 
        (SELECT * 
        FROM csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df    
        WHERE sdt = '${date}' 
        ) f ON a.customer_code = f.customer_no     
    JOIN csx_analyse_tmp.tmp_change_goods j 
        ON a.inventory_dc_code = j.inventory_dc_code 
        AND a.goods_code = j.goods_code_a
    GROUP BY 
        d.performance_region_name,
        d.performance_province_name,
        d.performance_city_name,
        a.inventory_dc_code,
        a.order_code,
        a.customer_code,
        d.customer_name,
        a.sub_customer_code,
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
        j.goods_code_b
) a
-- 关联商品A成本
LEFT JOIN 
    (SELECT * 
    FROM csx_analyse_tmp.csx_analyse_tmp_cb
    WHERE cost_price_for > 0 
    ) t2 ON a.performance_city_name = t2.performance_city_name AND a.goods_code = t2.goods_code 
-- 关联商品B成本
LEFT JOIN 
    (SELECT * 
    FROM csx_analyse_tmp.csx_analyse_tmp_cb
    WHERE cost_price_for > 0 
    ) t3 ON a.performance_city_name = t3.performance_city_name AND a.goods_code_b = t3.goods_code 
-- 关联主客户商品报价
LEFT JOIN 
    (SELECT 
        warehouse_code,
        customer_code,
        product_code,
        MAX(customer_price) as customer_price  
    FROM csx_dwd.csx_dwd_price_customer_price_guide_di
    WHERE substr(price_begin_time, 1, 10) <= '${date}' 
        AND substr(price_end_time, 1, 10) >= '${date}' 
        AND (sub_customer_code IS NULL OR LENGTH(sub_customer_code) = 0) 
    GROUP BY warehouse_code, customer_code, product_code
    ) t4 ON a.inventory_dc_code = t4.warehouse_code 
        AND a.customer_code = t4.customer_code 
        AND a.goods_code_b = t4.product_code 
-- 关联子客户商品报价
LEFT JOIN 
    (SELECT 
        warehouse_code,
        customer_code,
        sub_customer_code,
        product_code,
        MAX(customer_price) as customer_price  
    FROM csx_dwd.csx_dwd_price_customer_price_guide_di
    WHERE substr(price_begin_time, 1, 10) <= '${date}' 
        AND substr(price_end_time, 1, 10) >= '${date}' 
        AND sub_customer_code IS NOT NULL 
        AND LENGTH(sub_customer_code) > 0
    GROUP BY warehouse_code, customer_code, sub_customer_code, product_code
    ) t5 ON a.inventory_dc_code = t5.warehouse_code 
        AND a.customer_code = t5.customer_code 
        AND a.sub_customer_code = t5.sub_customer_code 
        AND a.goods_code_b = t5.product_code;

-- 22点执行（复制17点逻辑，只需修改hour分区）
INSERT OVERWRITE TABLE csx_analyse_tmp.tmp_order_analysis_daily PARTITION(sdt='${date}', hour='22')
-- ... 与17点相同的查询逻辑，只需修改hour为'22'
;

-- 创建数据清理脚本（删除历史数据，只保留当天）
ALTER TABLE csx_analyse_tmp.tmp_order_analysis_daily DROP IF EXISTS PARTITION(sdt <> '${date}');
 
 
/*select 
	a.*,
	t2.cost_price_for, 
	t3.cost_price_for as cost_price_for_b, 	
    nvl(t5.customer_price,t4.customer_price) as customer_price  
from csx_analyse_tmp.tmp_order_detail a
 -- 关联客户下期成本 -- a商品成本
 left join 
 (select * 
 from csx_analyse_tmp.csx_analyse_tmp_cb
 where cost_price_for>0 
 ) t2 on a.inventory_dc_code=t2.inventory_dc_code and a.customer_code=t2.customer_code and a.sub_customer_code=t2.sub_customer_code and a.goods_code=t2.goods_code 
 
  -- 关联客户下期成本  -- -- b商品成本
 left join 
 (select * 
 from csx_analyse_tmp.csx_analyse_tmp_cb
 where cost_price_for>0 
 ) t3 on a.inventory_dc_code=t3.inventory_dc_code and a.customer_code=t3.customer_code and a.sub_customer_code=t3.sub_customer_code and a.goods_code_b=t3.goods_code 
 
  -- 关联目前生效区主客户商品报价数据  -- b商品的生效区客户报价
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
    ) t4 
    on a.inventory_dc_code=t4.warehouse_code and a.customer_code=t4.customer_code and a.goods_code_b=t4.product_code 
    -- 关联目前生效区子客户商品报价数据 -- b商品的生效区客户报价
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
    ) t5 
	on a.inventory_dc_code=t5.warehouse_code and a.customer_code=t5.customer_code and a.sub_customer_code=t5.sub_customer_code and a.goods_code_b=t5.product_code;
 
 
 &token=eyJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJkZGU1MmQ1NGY0OTg0YTQ3ODM5OTYzYmUzZjMyYjc4NiIsInVzZXJJZCI6IjEwMDAwMDA1NTcxNjkiLCJ1c2VyTmFtZSI6IumltuiJs-WNjiIsImlhdCI6MTc2MTI5MjEzMiwiZXhwIjoxNzYxMjk1NzMyfQ.eQrV7Kn1nSmaOG2Nc5aYwRv0ometzu27hFVkwfY0LbI
 
 