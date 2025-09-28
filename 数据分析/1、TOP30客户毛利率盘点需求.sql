-- -------------------------------------
-- ------各城市销量数据
drop table if exists csx_analyse_tmp.csx_analyse_tmp_sale_detail_ky_target_month_first_tmp;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_sale_detail_ky_target_month_first_tmp as 
select 
    t1.*,
    sum(t1.sale_amt)over(partition by t1.performance_city_name,t1.inventory_dc_code,t1.customer_code) as cus_sale_amt,
    dense_rank()over(partition by t1.performance_city_name,t1.inventory_dc_code,t1.customer_code order by t1.sale_amt desc) as pm,

    (case when t2.customer_price>0 or tt2.customer_price>0 then '是' else '否' end) as if_hav_cus_price,
    coalesce(tt2.customer_price,t2.customer_price) as sale_price_for-- 预估售价
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
    
        sum(case when a.sdt>=regexp_replace(add_months(trunc('${yes_date}','MM'),-1),'-','') and a.sdt<regexp_replace(trunc('${yes_date}','MM'),'-','') then a.sale_qty end) as sale_qty_last_month,
        sum(case when a.sdt>=regexp_replace(add_months(trunc('${yes_date}','MM'),-1),'-','') and a.sdt<regexp_replace(trunc('${yes_date}','MM'),'-','') then a.sale_amt end) as sale_amt_last_month,
        sum(case when a.sdt>=regexp_replace(add_months(trunc('${yes_date}','MM'),-1),'-','') and a.sdt<regexp_replace(trunc('${yes_date}','MM'),'-','') then a.profit end) as profit_last_month,

        sum(case when a.sdt>=regexp_replace(trunc('${yes_date}','MM'),'-','') and a.sdt<='${yes_sdt}' then a.sale_qty end) as sale_qty_this_month,
        sum(case when a.sdt>=regexp_replace(trunc('${yes_date}','MM'),'-','') and a.sdt<='${yes_sdt}' then a.sale_amt end) as sale_amt_this_month,
        sum(case when a.sdt>=regexp_replace(trunc('${yes_date}','MM'),'-','') and a.sdt<='${yes_sdt}' then a.profit end) as profit_this_month,

        sum(a.sale_amt) as sale_amt  
        
    from 
        (select * 
        from csx_dws.csx_dws_sale_detail_di  
        where sdt>=regexp_replace(add_months(trunc('${yes_date}','MM'),-1),'-','')  
        and sdt<='${yes_sdt}'    
        and business_type_code=1  
        and order_channel_code not in ('4','6','5') -- 剔除所有异常
        and refund_order_flag<>1 
        and shipper_code='YHCSX' 
        and customer_code in('243348'  
,'102924' 
,'231868' 
,'239466' 
,'115656' 
,'218936' 
,'130334' 
,'162716' 
,'226602' 
,'121584' 
,'175548' 
,'115831' 
,'116016' 
,'108152' 
,'115476' 
,'125350' 
,'250259' 
,'247826' 
,'128371' 
,'233646' 
,'252038' )
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
    ) t1 
    -- 关联目前生效区客户商品报价数据
    left join 
    (select * 
    from csx_analyse_tmp.csx_analyse_tmp_month_now_customer_price_month 
    where pm=1 
    ) t2 
    on t1.inventory_dc_code=t2.dc_code and t1.customer_code=t2.customer_code and t1.goods_code=t2.goods_code 
     -- 关联目前生效区子客户商品报价数据
    left join 
    (select * 
    from csx_analyse_tmp.csx_analyse_tmp_month_now_customer_price_month_sub 
    where pm=1 
    ) tt2 
    on t1.inventory_dc_code=tt2.dc_code and t1.customer_code=tt2.customer_code and t1.sub_customer_code=tt2.sub_customer_code and t1.goods_code=tt2.goods_code 
    -- 关联本月客户商品最后一次报价数据
    left join 
    (select * 
    from csx_analyse_tmp.csx_analyse_tmp_month_customer_price_last_month  
    where pm=1 
    ) t3 
    on t1.inventory_dc_code=t3.dc_code and t1.customer_code=t3.customer_code and t1.goods_code=t3.goods_code 
;


-- ------------------------------------------------------------------------------------------匹配成本的城市销量数据
-- -------------------------------------
-- ------各城市销量数据
drop table if exists csx_analyse_tmp.csx_analyse_tmp_sale_detail_ky_target_month_tmp;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_sale_detail_ky_target_month_tmp as 
select 
    t1.*,
    (case when t1.classify_large_code not in ('B01','B02','B03') and t5.received_price1>0 then t5.received_price1  
          when t1.classify_large_code not in ('B01','B02','B03') then t4.standard_price 
          when t1.classify_large_code in ('B01','B02','B03') and t1.purchase_price_final>0 then t1.purchase_price_final 
          when t1.classify_large_code in ('B01','B02','B03') and t2.purchase_price>0 then t2.purchase_price 
          when t1.classify_large_code in ('B01','B02','B03') and t3.purchase_price>0 then t3.purchase_price  
          when t1.classify_middle_name='蛋' and t5.received_price1>0 then t5.received_price1 
          when t1.classify_large_code in ('B01','B02','B03') and t1.classify_middle_name<>'蛋' then t5.received_price1 
          when t6.purchase_price>0 then t6.purchase_price 
    end) as purchase_price_final_last,
    nvl((case when t1.classify_large_code not in ('B01','B02','B03') and t5.received_price1>0 then t5.received_price1  
              when t1.classify_large_code not in ('B01','B02','B03') then t4.standard_price 
              when t1.classify_large_code in ('B01','B02','B03') and t1.purchase_price_final>0 then t1.purchase_price_final 
              when t1.classify_large_code in ('B01','B02','B03') and t2.purchase_price>0 then t2.purchase_price 
              when t1.classify_large_code in ('B01','B02','B03') and t3.purchase_price>0 then t3.purchase_price  
              when t1.classify_middle_name='蛋' and t5.received_price1>0 then t5.received_price1 
              when t1.classify_large_code in ('B01','B02','B03') and t1.classify_middle_name<>'蛋' then t5.received_price1 
              when t6.purchase_price>0 then t6.purchase_price 
         end),0
        ) as cost_price_for,
    (case when t1.classify_large_code not in ('B01','B02','B03') and t5.received_price1>0 then '近30天最后一次正常入库价'  
          when t1.classify_large_code not in ('B01','B02','B03') then '基准价' 
          when t1.classify_large_code in ('B01','B02','B03') and t1.purchase_price_final>0 and t1.order_strategy_final=1 then '下单策略-委外配置' 
          when t1.classify_large_code in ('B01','B02','B03') and t1.purchase_price_final>0 and t1.order_strategy_final=2 then '下单策略-中标第一优先级供应商' 
          when t1.classify_large_code in ('B01','B02','B03') and t1.purchase_price_final>0 and t1.order_strategy_final=3 then '下单策略-手工维护的最低价供应商' 
          when t1.classify_large_code in ('B01','B02','B03') and t1.purchase_price_final>0 and t1.order_strategy_final=4 then '下单策略-日采主供应商' 
          when t1.classify_large_code in ('B01','B02','B03') and t2.purchase_price>0 then '日采主供应商' 
          when t1.classify_large_code in ('B01','B02','B03') and t3.purchase_price>0 then '周期进价生效区最小值'  
          when t1.classify_middle_name='蛋' and t5.received_price1>0 then '近15天最后一次正常入库价' 
          when t1.classify_large_code in ('B01','B02','B03') and t1.classify_middle_name<>'蛋' then '近7天最后一次正常入库价' 
          when t6.purchase_price>0 then '最后一次失效周期进价' 
    end) as cost_price_type 
from 
    (select 
        a1.*,
        a2.supplier_code,
        a2.price_type as str_cost_price_type,
        a2.order_strategy_final,-- 成本策略
        a2.float_ratio_final as cost_float_ratio_final,
        a2.purchase_price,
        (case when a2.price_type=1 then a1.sale_price_for*(1-a2.float_ratio_final) else a2.purchase_price end) as purchase_price_final,
        coalesce(g1.supplier_code,g2.supplier_code,g3.supplier_code,g4.supplier_code) as main_supplier_code -- 主供应商编码 
        -- nvl((case when a2.price_type=1 then a1.sale_price_for*(1-a2.float_ratio_final) else a2.purchase_price end),a1.cost_price) as cost_price_for
    from 
        csx_analyse_tmp.csx_analyse_tmp_sale_detail_ky_target_month_first_tmp a1 
        left join 
        (select 
            * 
        from csx_analyse_tmp.csx_analyse_tmp_customer_goods_strategy_config_supplier_cost_final 
        where pm=1 
        ) a2 
        on a1.performance_city_name=a2.performance_city_name and a1.customer_code=a2.customer_code and a1.sub_customer_code=a2.sub_customer_code and a1.goods_code=a2.goods_code 

        -- ---------------------主供应商数据
        left join 
        -- 4级策略(商品)
        (select * 
        from csx_analyse_tmp.csx_analyse_tmp_scm_main_supplier 
        where pm=1 
        ) g1 
        on a1.performance_city_name=g1.performance_city_name and a1.goods_code=g1.product_code 
        left join 
        -- 4级策略(小类)
        (select * 
        from csx_analyse_tmp.csx_analyse_tmp_scm_main_supplier 
        where pm=1 
        ) g2 
        on a1.performance_city_name=g2.performance_city_name and a1.classify_small_code=g2.product_code 
        left join 
        -- 4级策略(中类)
        (select * 
        from csx_analyse_tmp.csx_analyse_tmp_scm_main_supplier 
        where pm=1 
        ) g3 
        on a1.performance_city_name=g3.performance_city_name and a1.classify_middle_code=g3.product_code 
        left join 
        -- 4级策略(大类)
        (select * 
        from csx_analyse_tmp.csx_analyse_tmp_scm_main_supplier 
        where pm=1 
        ) g4 
        on a1.performance_city_name=g4.performance_city_name and a1.classify_large_code=g4.product_code  
    ) t1 
    -- 主供应商周期进价
    left join 
    (select * 
    from csx_analyse_tmp.csx_analyse_tmp_product_purchase_cycle_price_sg 
    where pm=1 
    ) t2 
    on t1.performance_city_name=t2.performance_city_name and t1.main_supplier_code=t2.supplier_code and t1.goods_code=t2.product_code 
    -- 周期进价最小值
    left join 
    (select * 
    from csx_analyse_tmp.csx_analyse_tmp_product_purchase_cycle_price_min 
    where pm=1 
    ) t3 
    on t1.performance_city_name=t3.performance_city_name and t1.goods_code=t3.product_code 
    -- 食百基准价
    left join 
    (select * 
    from csx_analyse_tmp.csx_analyse_tmp_standard_price_uphold 
    where pm=1 
    ) t4 
    on t1.performance_city_name=t4.performance_city_name and t1.goods_code=t4.product_code 
    -- 蛋及食百最近一次入库价
    left join 
    (select * 
    from csx_analyse_tmp.last_rk_price 
    where pm=1 
    ) t5 
    on t1.performance_city_name=t5.performance_city_name and t1.goods_code=t5.goods_code  
    -- 最后一次失效的周期进价
    left join 
    (select * 
    from csx_analyse_tmp.csx_analyse_tmp_product_purchase_cycle_price_invaild_last 
    where pm=1 
    ) t6 
    on t1.performance_city_name=t6.performance_city_name and t1.goods_code=t6.product_code  
;

select 
    t.performance_region_name as `大区`,
    t.performance_province_name as `省区`,
    t.performance_city_name as `城市`,
    t.customer_code as `客户编码`,
    t.customer_name as `客户名称`,
    t.inventory_dc_code as `仓`,
    dense_rank()over(partition by t.performance_city_name,t.customer_code order by t.sale_amt desc) as `客户商品销售额排名`,
    t.goods_code as `商品编码`,
    t.goods_name as `商品名称`,
    t.unit_name as `单位`,
    t.classify_middle_name as `管理中类`,

    t.sale_qty_for as `预测销量`,
    t.sale_qty_last_month as `上月销量`,
    t.sale_amt_last_month as `上月销售额`,
    t.profit_last_month as `上月毛利额`,
    t.profitlv_last_month as `上月毛利率`,

    t.sale_qty_this_month as `本月销量`,
    t.sale_amt_this_month as `本月销售额`,
    t.profit_this_month as `本月毛利额`,
    t.profitlv_this_month as `本月毛利率`,

    t.sale_price_for as `生效区客户报价`,
    t.sale_amt_for as `预测销售额`,

    t.cost_price_for as `预测成本`,
    t.sale_cost_for as `预测总成本`,

    t.profit_for as `预测毛利额`,
    t.profitlv_for as `预测毛利率`  
from 
    (select 
        performance_region_name,
        performance_province_name,
        performance_city_name,
        customer_code,
        customer_name,
        inventory_dc_code,
        goods_code,
        goods_name,
        unit_name,
        classify_middle_name,

        sum(sale_qty_last_month) as sale_qty_for,
        sum(sale_qty_last_month) as sale_qty_last_month,
        sum(sale_amt_last_month) as sale_amt_last_month,
        sum(profit_last_month) as profit_last_month,
        sum(profit_last_month)/abs(sum(sale_amt_last_month)) as profitlv_last_month,

        sum(sale_qty_this_month) as sale_qty_this_month,
        sum(sale_amt_this_month) as sale_amt_this_month,
        sum(profit_this_month) as profit_this_month,
        sum(profit_this_month)/abs(sum(sale_amt_this_month)) as profitlv_this_month,

        sum(sale_amt) as sale_amt,

        sum(sale_price_for*(nvl(sale_qty_last_month,0)+nvl(sale_qty_this_month,0)))/sum(nvl(sale_qty_last_month,0)+nvl(sale_qty_this_month,0)) as sale_price_for,
        sum(sale_price_for*(sale_qty_last_month)) as sale_amt_for,

        sum(cost_price_for*(nvl(sale_qty_last_month,0)+nvl(sale_qty_this_month,0)))/sum(nvl(sale_qty_last_month,0)+nvl(sale_qty_this_month,0)) as cost_price_for,
        sum(cost_price_for*(sale_qty_last_month)) as sale_cost_for,

        sum(sale_price_for*(sale_qty_last_month))-sum(cost_price_for*(sale_qty_last_month)) as profit_for,
        (sum(sale_price_for*(sale_qty_last_month))-sum(cost_price_for*(sale_qty_last_month)))/abs(sum(sale_price_for*(sale_qty_last_month))) as profitlv_for 

    from csx_analyse_tmp.csx_analyse_tmp_sale_detail_ky_target_month_tmp 
    group by 
        performance_region_name,
        performance_province_name,
        performance_city_name,
        customer_code,
        customer_name,
        inventory_dc_code,
        goods_code,
        goods_name,
        unit_name,
        classify_middle_name 
    ) t 
 union all     
select 
    t.performance_region_name as `大区`,
    t.performance_province_name as `省区`,
    t.performance_city_name as `城市`,
    t.customer_code as `客户编码`,
    t.customer_name as `客户名称`,
    t.inventory_dc_code as `仓`,
    dense_rank()over(partition by t.performance_city_name,t.customer_code order by t.sale_amt desc) as `客户商品销售额排名`,
    t.goods_code as `商品编码`,
    t.goods_name as `商品名称`,
    t.unit_name as `单位`,
    t.classify_middle_name as `管理中类`,

    t.sale_qty_for as `预测销量`,
    t.sale_qty_last_month as `上月销量`,
    t.sale_amt_last_month as `上月销售额`,
    t.profit_last_month as `上月毛利额`,
    t.profitlv_last_month as `上月毛利率`,

    t.sale_qty_this_month as `本月销量`,
    t.sale_amt_this_month as `本月销售额`,
    t.profit_this_month as `本月毛利额`,
    t.profitlv_this_month as `本月毛利率`,

    t.sale_price_for as `生效区客户报价`,
    t.sale_amt_for as `预测销售额`,

    t.cost_price_for as `预测成本`,
    t.sale_cost_for as `预测总成本`,

    t.profit_for as `预测毛利额`,
    t.profitlv_for as `预测毛利率`  
from 
    (select 
        performance_region_name,
        performance_province_name,
        performance_city_name,
        customer_code,
        customer_name,
        inventory_dc_code,
        '合计'goods_code,
        '合计'goods_name,
        '合计'unit_name,
        '合计'classify_middle_name,

        sum(sale_qty_last_month) as sale_qty_for,
        sum(sale_qty_last_month) as sale_qty_last_month,
        sum(sale_amt_last_month)/10000 as sale_amt_last_month,
        sum(profit_last_month)/10000 as profit_last_month,
        sum(profit_last_month)/abs(sum(sale_amt_last_month)) as profitlv_last_month,

        sum(sale_qty_this_month) as sale_qty_this_month,
        sum(sale_amt_this_month)/10000 as sale_amt_this_month,
        sum(profit_this_month)/10000 as profit_this_month,
        sum(profit_this_month)/abs(sum(sale_amt_this_month)) as profitlv_this_month,

        sum(sale_amt)/10000 as sale_amt,

        sum(sale_price_for*(nvl(sale_qty_last_month,0)+nvl(sale_qty_this_month,0)))/sum(nvl(sale_qty_last_month,0)+nvl(sale_qty_this_month,0)) as sale_price_for,
        sum(sale_price_for*(sale_qty_last_month))/10000 as sale_amt_for,

        sum(cost_price_for*(nvl(sale_qty_last_month,0)+nvl(sale_qty_this_month,0)))/sum(nvl(sale_qty_last_month,0)+nvl(sale_qty_this_month,0)) as cost_price_for,
        sum(cost_price_for*(sale_qty_last_month))/10000 as sale_cost_for,

       ( sum(sale_price_for*(sale_qty_last_month))-sum(cost_price_for*(sale_qty_last_month)))/10000 as profit_for,
        (sum(sale_price_for*(sale_qty_last_month))-sum(cost_price_for*(sale_qty_last_month)))/abs(sum(sale_price_for*(sale_qty_last_month))) as profitlv_for 

    from csx_analyse_tmp.csx_analyse_tmp_sale_detail_ky_target_month_tmp 
    group by 
        performance_region_name,
        performance_province_name,
        performance_city_name,
        customer_code,
        customer_name,
        inventory_dc_code
        
    ) t 
    