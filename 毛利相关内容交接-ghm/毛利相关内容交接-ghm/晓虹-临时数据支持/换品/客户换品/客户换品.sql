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

-- 生鲜近7天，食百近30天成本数据
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


-- ---------------------------------------
-- --客户商品前一个报价生效周期的销量、售价、成本，目前最新的成本值
drop table if exists csx_analyse_tmp.csx_analyse_tmp_sale_detail_ky_last_month_detail_ghm;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_sale_detail_ky_last_month_detail_ghm as 
select 
    t1.*,
    case  
		when t1.customer_code='257658' then tt2.price
		else t2.cost_price_for
	end as cost_price_for,
    case 
		when t1.customer_code='257658' then '近期入库价' 
		else t2.cost_price_type
		end as cost_price_type, -- 最新成本取值来源

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
		and customer_code in ('252181','151497','252189','252182','252183','254511','254080','252191','258912','252186','257481','252193','252195','256641','252185','252999','126377','257658','279553')
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
    -- 关联客户下期成本
    left join csx_analyse_tmp.csx_analyse_tmp_cb tt2 
    on t1.performance_city_name=tt2.performance_city_name and t1.goods_code=tt2.goods_code	
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
	sale_qty*sale_price_new as sale_amt,
	sale_qty*cost_price_new as cost_amt,
	(sale_price_new-cost_price_new)*sale_qty as profit,
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
		sub_customer_code,
		sub_customer_name,
		classify_large_name,
		classify_middle_name,
		classify_small_name,
		goods_code,
		goods_name,
		unit_name
	) t 