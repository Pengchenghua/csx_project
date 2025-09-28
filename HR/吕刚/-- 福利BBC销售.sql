-- 福利BBC销售

with tmp_sale_detail as (select
        
        concat(substr(sdt, 1, 4), 'Q', floor(substr(sdt, 5, 2) / 3.1) + 1) as sale_quarter,
        substr(sdt, 1, 6) month,
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        business_type_code,
        business_type_name,
        customer_code,
         b.classify_large_code,
          b.classify_large_name,
          b.classify_middle_code,
          b.classify_middle_name,
        -- customer_name,
        -- inventory_dc_code,
        -- inventory_dc_name,
        -- delivery_type_name,
        -- -- 配送类型名称
        -- classify_large_code,
        -- classify_middle_code,
        -- classify_small_code,
        -- goods_code,
        -- goods_name,
        -- count(distinct sdt) count_day,
        sum(sale_qty) sale_qty,
        sum(sale_cost) sale_cost,
        sum(sale_amt) sale_amt,
        sum(profit) profit,
        sum(sale_amt_no_tax) sale_amt_no_tax,
        sum(profit_no_tax) profit_no_tax
      from
        csx_dws.csx_dws_sale_detail_di a 
          left join 
        (select goods_code,
            classify_large_code,
            classify_large_name,
            classify_middle_code,
            classify_middle_name
        from csx_dim.csx_dim_basic_goods
        where sdt='current'
            ) b on a.goods_code=b.goods_code
      where
        sdt between  '20240101' and '20241231'
        and channel_code in ('1', '7', '9')
        and business_type_code in ('2') --  and performance_region_name in ('华南大区', '华北大区', '华西大区', '华东大区', '华中大区')
      group by   concat(substr(sdt, 1, 4), 'Q', floor(substr(sdt, 5, 2) / 3.1) + 1) ,
        substr(sdt, 1, 6) ,
        
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        business_type_code,
        business_type_name,
        customer_code,
         b.classify_large_code,
          b.classify_large_name,
          b.classify_middle_code,
          b.classify_middle_name
      union all
      select
        
        concat(substr(sdt, 1, 4), 'Q', floor(substr(sdt, 5, 2) / 3.1) + 1) as sale_quarter,
        substr(sdt, 1, 6) month,
        
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        6 as business_type_code,
        'BBC' as business_type_name,
        customer_code,
        -- customer_name,
        -- inventory_dc_code,
        -- inventory_dc_name,
        -- delivery_type_name,
        -- -- 配送类型名称
          b.classify_large_code,
          b.classify_large_name,
          b.classify_middle_code,
          b.classify_middle_name,
        -- classify_small_code,
        -- goods_code,
        -- goods_name,
        -- count(distinct sdt) count_day,
        sum(sale_qty) sale_qty,
        sum(sale_cost) sale_cost,
        sum(sale_amt) sale_amt,
        sum(profit) profit,
        sum(sale_amt_no_tax) sale_amt_no_tax,
        sum(profit_no_tax) profit_no_tax
      from
        csx_dws.csx_dws_bbc_sale_detail_di a 
        left join 
        (select goods_code,
            classify_large_code,
            classify_large_name,
            classify_middle_code,
            classify_middle_name
        from csx_dim.csx_dim_basic_goods
        where sdt='current'
            ) b on a.goods_code=b.goods_code
      where
        sdt between '20240101' and '20241231'
        and channel_code in ('1', '7', '9') -- and business_type_code in ('2','6')
        --	and performance_region_name in ('华南大区','华北大区','华西大区','华东大区','华中大区')
        group by     
       
        concat(substr(sdt, 1, 4), 'Q', floor(substr(sdt, 5, 2) / 3.1) + 1) ,
        substr(sdt, 1, 6) ,
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        customer_code,
         b.classify_large_code,
          b.classify_large_name,
          b.classify_middle_code,
          b.classify_middle_name
    ) select 
        substr(month, 1, 4) s_year,
        month,
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        business_type_code,
        business_type_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        sum(sale_qty) sale_qty,
        sum(sale_cost) sale_cost,
        sum(sale_amt) sale_amt,
        sum(profit) profit,
        sum(profit)/sum(sale_amt) profit_rate
    from tmp_sale_detail
    group by substr(month, 1, 4) ,
        month,
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        business_type_code,
        business_type_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name
 
    ;