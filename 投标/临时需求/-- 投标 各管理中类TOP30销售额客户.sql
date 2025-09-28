-- 投标 各管理中类TOP30销售额客户
with sale as (
    select performance_region_name,
        performance_province_name,
        a.customer_code,
        b.customer_name,
        b.second_category_name,
        c.classify_middle_code,
        c.classify_middle_name,
        sum(sale_amt) sale_amt,
        sum(profit) profit
    from csx_dws.csx_dws_sale_detail_di AS a
        join (
            select customer_name,
                customer_code,
                second_category_name
            from csx_dim.csx_dim_crm_customer_info
            where sdt = 'current'
        ) b on a.customer_code = b.customer_code
        left join (
            select goods_code,
                classify_middle_code,
                classify_middle_name
            from csx_dim.csx_dim_basic_goods
            where sdt = 'current'
        ) c on a.goods_code = c.goods_code
    where sdt >= '20210101'
        and sdt <= '20240630'
        and company_code='2115'
        -- and business_type_code in (1, 2, 6, 4)
        and business_type_code !=9
    GROUP BY performance_region_name,
        performance_province_name,
        a.customer_code,
        b.customer_name,
        b.second_category_name,
        c.classify_middle_code,
        c.classify_middle_name
)
select *
FROM (
        select *,profit/sale_amt as profit_rate,
            row_number() over(
                partition by classify_middle_name,performance_province_name
                order by sale_amt DESC
            ) as rn
        from sale
    ) a
where rn < 31