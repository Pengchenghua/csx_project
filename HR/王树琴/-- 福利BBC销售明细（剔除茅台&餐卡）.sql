-- /*
-- 2408260610719664 在运费单及结算单中，只有一条数据，但是在销售中有区分京东、自营，这样就会造成运费与结算单重复的数据出现
-- */
-- 福利BBC销售明细（剔除茅台&餐卡）
with tmp_flbbc_customer_goods_sale as (
  select
    month,
    a.group_flag,
    a.type_flag,
    a.operation_mode_name,
    a.performance_region_name,
    a.performance_province_name,
    a.performance_city_name,
    a.delivery_type_name,
    a.business_type_code,
    -- 配送类型名称
    a.customer_code,
    d.customer_name,
    e.first_business_sale_date,
    e.last_business_sale_date,
    f.first_sale_date,
    f.last_sale_date,
    d.first_category_name,
    d.second_category_name,
    d.third_category_name,
    a.business_type_name,
    d.sales_user_number,
    d.sales_user_name,
    h.classify_large_name,
    h.classify_middle_name,
    h.classify_small_name,
    a.goods_code,
    h.goods_name,
    (count_day) count_day,
    (sale_qty) sale_qty,
    (sale_cost) sale_cost,
    (sale_amt) sale_amt,
    (profit) profit,
    sale_amt_no_tax,
    profit_no_tax
  from
    (
      select
        '福利' group_flag,
        '福利' type_flag,
        '福利' as operation_mode_name,
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
        -- customer_name,
        inventory_dc_code,
        -- inventory_dc_name,
        delivery_type_name,
        -- 配送类型名称
        classify_large_code,
        classify_middle_code,
        classify_small_code,
        goods_code,
        goods_name,
        count(distinct sdt) count_day,
        sum(sale_qty) sale_qty,
        sum(sale_cost) sale_cost,
        sum(sale_amt) sale_amt,
        sum(profit) profit,
        sum(sale_amt_no_tax) sale_amt_no_tax,
        sum(profit_no_tax) profit_no_tax
      from
        csx_dws.csx_dws_sale_detail_di
      where
        sdt >= '20231201'
        and sdt < '20240301'
        and channel_code in ('1', '7', '9')
        and business_type_code in ('2') --  and performance_region_name in ('华南大区', '华北大区', '华西大区', '华东大区', '华中大区')
      group by
        concat(substr(sdt, 1, 4), 'Q', floor(substr(sdt, 5, 2) / 3.1) + 1),
        substr(sdt, 1, 6),
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        customer_code,
        business_type_code,
        business_type_name,
        inventory_dc_code,
        delivery_type_name,
        -- goods_code
        classify_large_code,
        classify_middle_code,
        classify_small_code,
        goods_code,
        goods_name
      union all
      select
        'BBC' group_flag,
        if(
          credit_pay_type_name = '餐卡'
          or credit_pay_type_code = 'F11',
          '餐卡',
          '福利'
        ) type_flag,
        operation_mode_name,
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
        inventory_dc_code,
        -- inventory_dc_name,
        delivery_type_name,
        -- 配送类型名称
        classify_large_code,
        classify_middle_code,
        classify_small_code,
        goods_code,
        goods_name,
        count(distinct sdt) count_day,
        sum(sale_qty) sale_qty,
        sum(sale_cost) sale_cost,
        sum(sale_amt) sale_amt,
        sum(profit) profit,
        sum(sale_amt_no_tax) sale_amt_no_tax,
        sum(profit_no_tax) profit_no_tax
      from
        csx_dws.csx_dws_bbc_sale_detail_di
      where
        sdt >= '20231201'
        and sdt < '20240301'
        and channel_code in ('1', '7', '9') -- and business_type_code in ('2','6')
        --	and performance_region_name in ('华南大区','华北大区','华西大区','华东大区','华中大区')
      group by
        if(
          credit_pay_type_name = '餐卡'
          or credit_pay_type_code = 'F11',
          '餐卡',
          '福利'
        ),
        operation_mode_name,
        concat(substr(sdt, 1, 4), 'Q', floor(substr(sdt, 5, 2) / 3.1) + 1),
        substr(sdt, 1, 6),
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        customer_code,
        -- business_type_code,business_type_name,
        inventory_dc_code,
        delivery_type_name,
        -- goods_code
        classify_large_code,
        classify_middle_code,
        classify_small_code,
        goods_code,
        goods_name
    ) a
    left join (
      select
        customer_code,
        customer_name,
        performance_region_name,
        performance_province_name,
        performance_city_name,
        sales_user_number,
        sales_user_name,
        first_category_name,
        second_category_name,
        third_category_name
      from
        csx_dim.csx_dim_crm_customer_info
      where
        sdt = 'current' -- 'current'
    ) d on d.customer_code = a.customer_code
    left join -- 业务类型首单日期
    (
      select
        customer_code,
        business_type_code,
        min(first_business_sale_date) first_business_sale_date,
        max(last_business_sale_date) last_business_sale_date
      from
        csx_dws.csx_dws_crm_customer_business_active_di
      where
        sdt = 'current'
      group by
        customer_code,
        business_type_code
    ) e on e.customer_code = a.customer_code
    and e.business_type_code = a.business_type_code
    left join -- 客户首单日期
    (
      select
        customer_code,
        first_sale_date,
        last_sale_date
      from
        csx_dws.csx_dws_crm_customer_active_di
      where
        sdt = 'current'
    ) f on f.customer_code = a.customer_code
    left join (
      select
        shop_code,
        shop_name
      from
        csx_dim.csx_dim_shop
      where
        sdt = 'current'
    ) g on a.inventory_dc_code = g.shop_code
    left join (
      select
        goods_code,
        goods_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name
      from
        csx_dim.csx_dim_basic_goods
      where
        sdt = 'current'
    ) h on h.goods_code = a.goods_code
)
select
  concat('202312-', '202402') as sdt_s,
  month,
  group_flag,
  type_flag,
  operation_mode_name,
  performance_region_name,
  performance_province_name,
  performance_city_name,
  a.customer_code,
  customer_name,
  first_category_name,
  second_category_name,
  third_category_name,
  business_type_name,
  sales_user_number,
  sales_user_name,
  max(max_create_date) max_create_date,
  -- 最近商机创建时间
  sum(count_day) count_day,
  sum(sale_qty) sale_qty,
  sum(sale_cost) sale_cost,
  sum(sale_amt) sale_amt,
  sum(profit) profit,
  sum(profit) / sum(sale_amt) profit_rate,
  sum(sale_amt_no_tax) sale_amt_no_tax,
  sum(profit_no_tax) profit_no_tax,
  sum(profit_no_tax) / sum(sale_amt_no_tax) profit_reta_no_tax
from
  tmp_flbbc_customer_goods_sale a
  left join (
    select
      substr(regexp_replace(to_date(create_time), '-', ''), 1, 6) smonth,
      customer_code,
      business_type_code,
      max(create_time) max_create_date
    from
      csx_dim.csx_dim_crm_business_info
    where
      sdt = 'current'
      and business_type_code in (2, 6)
      and create_time >= '2023-08-01 00:00:00'
      and create_time < '2024-10-01 00:00:00' --  and customer_code='105561'
    group by
      customer_code,
      business_type_code,
      substr(regexp_replace(to_date(create_time), '-', ''), 1, 6)
  ) b on a.customer_code = b.customer_code
  and a.business_type_code = b.business_type_code
  and a.month = b.smonth
where
  goods_code not in ('8718', '8708', '8649', '840509') -- and type_flag !='餐卡'
group by
  month,
  group_flag,
  type_flag,
  operation_mode_name,
  performance_region_name,
  performance_province_name,
  performance_city_name,
  a.customer_code,
  customer_name,
  first_category_name,
  second_category_name,
  third_category_name,
  business_type_name,
  sales_user_number,
  sales_user_name;