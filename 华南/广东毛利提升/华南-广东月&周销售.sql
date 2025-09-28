WITH sale as (
    select substr(sdt, 1, 6) as months,
        customer_code,
        customer_name,
        delivery_type_name,
        direct_delivery_type_name,
        classify_large_name,
        classify_middle_name,
        sum(
            case
                when substr(sdt, 1, 6) = '202405' then sale_amt
            end
        ) as sale_amt,
        sum(
            case
                when substr(sdt, 1, 6) = '202405' then profit
            end
        ) as profit,
        sum(
            case
                when substr(sdt, 1, 6) = '202405' then sale_qty
            end
        ) as sale_qty,
        sum(
            case
                when substr(sdt, 1, 6) = '202405' then sale_cost
            end
        ) as sale_cost,
        sum(
            case
                when substr(sdt, 1, 6) = '202404' then sale_amt
            end
        ) as last_sale_amt,
        sum(
            case
                when substr(sdt, 1, 6) = '202404' then profit
            end
        ) as last_profit,
        sum(
            case
                when substr(sdt, 1, 6) = '202404' then sale_qty
            end
        ) as last_sale_qty,
        sum(
            case
                when substr(sdt, 1, 6) = '202404' then sale_cost
            end
        ) as last_sale_cost
    from csx_analyse.csx_analyse_bi_sale_detail_di
    where (
            (
                sdt >= '20240501'
                and sdt <= '20240528'
            )
            or (
                sdt >= '20240401'
                and sdt <= '20240428'
            )
        )
        and shop_low_profit_flag = 0
        and inventory_dc_code='W0BK'
        and customer_name not like '%光明鸽%'
        and order_channel_code not in ('26', '25')
        -- and customer_code in ('128371',
        --                     '195996',
        --                     '155386',
        --                     '155386',
        --                     '248266',
        --                     '245859'
        --                     )
    group by substr(sdt, 1, 6),
        customer_code,
        customer_name,
        delivery_type_name,
        direct_delivery_type_name,
        classify_large_name,
        classify_middle_name
)
select customer_code,
    customer_name,
    delivery_type_name,
    direct_delivery_type_name,
    classify_large_name,
    classify_middle_name,
    sum(sale_amt) as sale_amt,
    sum(sale_cost) as sale_cost,
    sum(profit) as profit,
    sum(sale_qty) as sale_qty,
    sum(last_sale_amt) as last_sale_amt,    
    sum(last_sale_qty) as last_sale_qty,
    sum(last_sale_cost) as last_sale_cost,
    sum(last_profit) as last_profit
from sale
    group by customer_code,
    customer_name,
    delivery_type_name,
    direct_delivery_type_name,
    classify_large_name,
    classify_middle_name

-- 武警二支队，南科大，中兴，腾讯金百味，建设银行

-- 周

    select csx_week,
        concat(csx_week_begin,'~',csx_week_end) as week_date,
        customer_code,
        customer_name,
        delivery_type_name,
        direct_delivery_type_name,
        classify_large_name,
        classify_middle_name,
        sum(sale_amt  ) as sale_amt,
        sum(profit    ) as profit,
        sum(sale_qty  ) as sale_qty,
        sum(sale_cost ) as sale_cost
    from   csx_analyse.csx_analyse_bi_sale_detail_di
    where sdt>='20240427'
        and shop_low_profit_flag = 0
        and customer_name not like '%光明鸽%'
        and order_channel_code not in ('26', '25')
        and inventory_dc_code='W0BK'
        --   and customer_code in ('128371',
        --                     '195996',
        --                     '155386',
        --                     '155386',
        --                     '248266',
        --                     '245859'
        --                     )
    group by csx_week,
        customer_code,
        customer_name,
        delivery_type_name,
        direct_delivery_type_name,
        classify_large_name,
        classify_middle_name,
        concat(csx_week_begin,'~',csx_week_end) 