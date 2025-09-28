
-- 福利销售   
with a as ( 
   select 
        substr(sdt,1,4) as syear,
        substr(sdt,1,6) as smonth,
        -- weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)) week,
        performance_region_name,
        performance_province_name,
        performance_city_name,
        inventory_dc_code,
        business_type_code,
        business_type_name,
        -- (case when (credit_pay_type_name='餐卡' or credit_pay_type_code='F11') then '餐卡'
        --              when (credit_pay_type_name='福利' or credit_pay_type_code='F10') then '福利' 
        --     else '福利' end) type_flag,
        customer_code,
        customer_name,
        goods_code,
        sum(sale_qty)sale_qty,
        sum(sale_amt)/10000 as sale_amt, 
        sum(profit)/10000 as profit 
    from csx_dws.csx_dws_sale_detail_di 
    where ((sdt >='20240501' and sdt <='20240604')
        or (sdt >='20230501' and sdt <='20230630')
        or (sdt >='20220501' and sdt <='20220630')
        )
        and business_type_code='2'
     --    and inventory_dc_code not in ('W0K9','WB26','W0A3','WB62','WC02') -- W0K9是监狱日采，WB26是小店过机，W0A3是日配仓库，WB62是京东仓库
    group by 
        substr(sdt,1,4),
        substr(sdt,1,6),
                -- weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)),
        performance_region_name,
        performance_province_name,
        performance_city_name,
        inventory_dc_code,
        customer_code,
        customer_name,
        goods_code,
        business_type_name,
        business_type_code
        ) 
    select syear,
        smonth,
        performance_province_name,
        performance_city_name,
        customer_code,
        customer_name,
        -- a.goods_code,
       	-- goods_name,
       	-- unit_name,
        -- brand_name,
        -- classify_large_code,
        -- classify_large_name,
        -- classify_middle_code,
        -- classify_middle_name,
        -- classify_small_code	,
        -- classify_small_name,
        sum(sale_qty) sale_qty,
        sum(sale_amt)sale_amt ,
        sum(profit)profit,
        sum(profit)/sum(sale_amt) as profit_rate
        from a 
    join 
    (select goods_code,
        goods_name,
        unit_name,
        brand_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code	,
        classify_small_name
    from csx_dim.csx_dim_basic_goods 
        where sdt='current') b on a.goods_code=b.goods_code
        where performance_region_name='华南大区'
    group by syear,
        smonth,
        performance_province_name,
        customer_code,
        customer_name,
        performance_city_name
        -- operation_mode_name,
        -- a.goods_code,
        -- goods_name,unit_name,
        -- brand_name,classify_large_code,
        -- classify_large_name,
        -- classify_middle_code,
        -- classify_middle_name,
        -- classify_small_code	,
        -- classify_small_name
    ;

--  bbc 

   with a as ( 
   select 
        substr(sdt,1,4) as syear,
        substr(sdt,1,6) as smonth,
        -- weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)) week,
        performance_region_name,
        performance_province_name,
        performance_city_name,
        inventory_dc_code,
        6 as business_type_code,
        'BBC' as business_type_name,
        (case when (credit_pay_type_name='餐卡' or credit_pay_type_code='F11') then '餐卡'
                     when (credit_pay_type_name='福利' or credit_pay_type_code='F10') then '福利' 
            else '福利' end) type_flag,
        operation_mode_name,
        customer_code,
        customer_name,
        goods_code,
        sum(sale_qty)sale_qty,
        sum(sale_amt)/10000 as sale_amt, 
        sum(profit)/10000 as profit 
    from csx_dws.csx_dws_bbc_sale_detail_di  
    where ((sdt >='20240501' and sdt <='20240604')
        or (sdt >='20230501' and sdt <='20230630')
        or (sdt >='20220501' and sdt <='20220630')
        )
     --    and inventory_dc_code not in ('W0K9','WB26','W0A3','WB62','WC02') -- W0K9是监狱日采，WB26是小店过机，W0A3是日配仓库，WB62是京东仓库
    group by 
        substr(sdt,1,4),
        substr(sdt,1,6),
                -- weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)),
        performance_region_name,
        performance_province_name,
        performance_city_name,
        inventory_dc_code,
        (case when (credit_pay_type_name='餐卡' or credit_pay_type_code='F11') then '餐卡'
                     when (credit_pay_type_name='福利' or credit_pay_type_code='F10') then '福利' 
            else '福利' end),
        operation_mode_name,
        customer_code,
        customer_name,
        goods_code
        ) 
    select syear,
        smonth,
        performance_province_name,
        performance_city_name,
        customer_code,
        customer_name,
        type_flag,
        -- a.goods_code,
       	-- goods_name,
       	-- unit_name,
        -- brand_name,
        -- classify_large_code,
        -- classify_large_name,
        -- classify_middle_code,
        -- classify_middle_name,
        -- classify_small_code	,
        -- classify_small_name,
        sum(sale_qty) sale_qty,
        sum(sale_amt)sale_amt ,
        sum(profit)profit,
        sum(profit)/sum(sale_amt) as profit_rate
        from a 
    join 
    (select goods_code,
        goods_name,
        unit_name,
        brand_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code	,
        classify_small_name
    from csx_dim.csx_dim_basic_goods 
        where sdt='current') b on a.goods_code=b.goods_code
        where performance_region_name='华南大区'
    group by syear,
        smonth,
        performance_province_name,
        customer_code,
        customer_name,
        type_flag,
        performance_city_name
        -- operation_mode_name,
        -- a.goods_code,
        -- goods_name,unit_name,
        -- brand_name,classify_large_code,
        -- classify_large_name,
        -- classify_middle_code,
        -- classify_middle_name,
        -- classify_small_code	,
        -- classify_small_name