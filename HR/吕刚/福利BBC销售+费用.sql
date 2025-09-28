
-- 福利BBC销售&费用
-- 福利BBC销售+费用
--   drop table csx_analyse_tmp.csx_analyse_tmp_flbbs_sale_detail ;
   create table csx_analyse_tmp.csx_analyse_tmp_flbbs_sale_detail as 
   select
        '福利' group_flag,
        '福利' type_flag,
        '福利' as operation_mode_name,
        concat(substr(sdt, 1, 4), 'Q', floor(substr(sdt, 5, 2) / 3.1) + 1) as sale_quarter,
        substr(sdt, 1, 6) month,
        sdt,
        order_code,
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
        -- inventory_dc_code,
        -- -- inventory_dc_name,
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
        csx_dws.csx_dws_sale_detail_di
      where
        sdt between '20240601' and '20241130'
        and channel_code in ('1', '7', '9')
        and business_type_code in ('2') --  and performance_region_name in ('华南大区', '华北大区', '华西大区', '华东大区', '华中大区')
      group by   concat(substr(sdt, 1, 4), 'Q', floor(substr(sdt, 5, 2) / 3.1) + 1) ,
        substr(sdt, 1, 6) ,
        sdt,
        order_code,
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        business_type_code,
        business_type_name,
        customer_code
      union all
      select
        'BBC' group_flag,
        if(credit_pay_type_name = '餐卡'  or credit_pay_type_code = 'F11', '餐卡',  '福利') type_flag,
        operation_mode_name,
        concat(substr(sdt, 1, 4), 'Q', floor(substr(sdt, 5, 2) / 3.1) + 1) as sale_quarter,
        substr(sdt, 1, 6) month,
        sdt,
        order_code,
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
        csx_dws.csx_dws_bbc_sale_detail_di
      where
        sdt between '20240601' and '20241130'
        and channel_code in ('1', '7', '9') -- and business_type_code in ('2','6')
        --	and performance_region_name in ('华南大区','华北大区','华西大区','华东大区','华中大区')
        group by     
        if(
          credit_pay_type_name = '餐卡'
          or credit_pay_type_code = 'F11',
          '餐卡',
          '福利'
        )  ,
        operation_mode_name,
        concat(substr(sdt, 1, 4), 'Q', floor(substr(sdt, 5, 2) / 3.1) + 1) ,
        substr(sdt, 1, 6) ,
        sdt,
        order_code,
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        customer_code
        -- customer_name,
        -- inventory_dc_code
 
    ;



-- 计算客户上月1号至今每日的当日应收金额、昨日应收金额、资金占用费


-- drop table  csx_analyse_tmp.csx_tmp_sss_customer_receivable_amount;
 create table csx_analyse_tmp.csx_tmp_sss_customer_receivable_amount as
with tmp_sss_customer_receivable_amount as (
  -- 计算应收金额和剩余金额
  select
    sdt,
    customer_code,
    business_type_code,
    type_flag,
    operation_mode_name,
    sum(coalesce(receivable_amount, 0)) as receivable_amount
    -- 应收金额
    -- sum(coalesce(residue_amt_sss, 0)) as residue_amt_sss -- 剩余金额 认领未核销金额
  from
    (
      select
        -- regexp_replace(to_date(a .happen_date),'-','') as sdt,
        sdt,
        a.customer_code,
        type_flag,
        operation_mode_name,
        coalesce(b.business_type_code, 1) as business_type_code,
        sum(coalesce(a .unpaid_amount, 0)) as receivable_amount,
        -- 应收金额
        0 as residue_amt_sss -- 剩余金额 认领未核销金额
      from
        (
          -- 20230214日客户扩展到信控切表 历史得用客户表 因此3月后用新表
          select
            source_bill_no,
            -- 来源单号
             case when substr(source_bill_no,1,1) ='B' then substr(source_bill_no, 2,length(source_bill_no)-2)  -- 涉及 BBC单号有字母开头的处理
              else source_bill_no end as new_source_bill_no,
            customer_code,
            -- 客户编码
            happen_date,
            -- 发生时间
            unpaid_amount,
            -- 未回款金额
            source_sys,
            -- 来源系统 MALL b端销售 BBC bbc端 BEGIN 期初
            sdt
          from
            csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di -- 销售结算对账开票结算详情表（新表）
          where
            sdt >= '20240601'
            and sdt<='20241210'
            and regexp_replace(date(happen_date), '-', '') <= sdt
           -- and source_bill_no='R2408010410590817A'
            -- and customer_code='130536'
            and regexp_replace(date(happen_date), '-', '')>='20240601'
        ) a
        left join 
        (select order_code ,
            business_type_code,
            type_flag,
            operation_mode_name
        from csx_analyse_tmp.csx_analyse_tmp_flbbs_sale_detail 
            group by order_code ,
            business_type_code,
            type_flag,
            operation_mode_name
        ) b on a .new_source_bill_no = b.order_code
        and if(a .source_sys <> 'BEGIN', true, false)
        and b.business_type_code is not null 
      group by
        --  regexp_replace(to_date(a .happen_date),'-',''),
        sdt,
        a .customer_code ,
        type_flag,
        operation_mode_name,
        coalesce(b.business_type_code, 1) 

    ) a
  group by
    sdt,
    customer_code,
    business_type_code,
    type_flag,
    operation_mode_name
)
select
  sdt,
  customer_code,
  business_type_code,
  type_flag,
  operation_mode_name,
--   sum(coalesce(residue_amt_sss, 0)) as residue_amt_sss,
  -- 剩余金额 认领未核销金额
  sum(coalesce(receivable_amount, 0)) as receivable_amount,
  -- 当日应收
  if(
      sum(receivable_amount) > 0,
      sum(receivable_amount),
      0
    )  * 0.06 / 365 as capital_takes_up -- 资金占用费
from
  (
    select
      sdt,
      customer_code,
      business_type_code,
      type_flag,
      operation_mode_name,
      receivable_amount 
    from
      tmp_sss_customer_receivable_amount 
    
  ) a
where
  business_type_code in ('2','6')
  and sdt >='20240601'
--   and customer_code='130536'
group by
  sdt,
  customer_code,
  business_type_code,
  type_flag,
  operation_mode_name;
  
  

  
-- 1、优先处理BBC运费
create table csx_analyse_tmp.csx_analyse_tmp_bbc_expense_detail as 

select group_flag,
    a.type_flag,
    a.operation_mode_name,
    -- a.sale_quarter,
    a.month,
    a.sdt,
    a.order_code,
    a.business_type_code,
    -- a.business_type_name,
    a.customer_code,
    -- a.wms_order_code,
    sum(bbc_express_amount) bbc_express_amount
from  
(
select group_flag,
    a.type_flag,
    a.operation_mode_name,
    a.sale_quarter,
    a.month,
    a.sdt,
    a.order_code,
    a.business_type_code,
    a.business_type_name,
    a.customer_code,
    a.wms_order_code,
    bbc_express_amount
    from csx_analyse_tmp.csx_analyse_tmp_flbbs_sale_detail a
    left join (
        select merchant_order_number,
            a.customer_code,
            out_order_code,
            -- 运费
            coalesce(
                sum(
                    cast(
                        cast(settlement_amount as decimal(20, 6)) / 1.06 as decimal(20, 6)
                    )
                ),
                0
            ) as bbc_express_amount
        from csx_report.csx_report_tms_transport_bbc_expense_detail a
        where a.sdt = regexp_replace(date_sub(current_date(), 1), '-', '')
            and regexp_replace(substr(bill_belongs_end, 1, 10), '-', '') >= regexp_replace(
                add_months(trunc('2024-11-12', 'MM'), -5),
                '-',
                ''
            )
            and regexp_replace(substr(bill_belongs_end, 1, 10), '-', '') <= regexp_replace(date_sub(current_date(), 1), '-', '')
        group by a.customer_code,
            merchant_order_number,
            out_order_code
    ) b on a.customer_code = b.customer_code
    and a.order_code = b.merchant_order_number
    and a.wms_order_code = b.out_order_code
    -- where merchant_order_number='2409060610814112'
) a 
group by group_flag,
    a.type_flag,
    a.operation_mode_name,
    -- a.sale_quarter,
    a.month,
    a.sdt,
    a.order_code,
    a.business_type_code,
    -- a.business_type_name,
    a.customer_code;
    
    
    -- select * from  csx_analyse_tmp.csx_analyse_tmp_bbc_expense_detail  where customer_code='118121' and month='202409'

-- 2、处理福利运费 
drop table  csx_analyse_tmp.csx_analyse_tmp_tms_entrucking_order;
create table csx_analyse_tmp.csx_analyse_tmp_tms_entrucking_order as 
with tmp_tms_order_detail as 
      (select
        regexp_replace(send_date, '-', '') as sdt,
        customer_code,
        shipped_order_code,
        if(access_caliber = 2, 99, business_type_code) as business_type_code,
        sum(
            cast(
              excluding_tax_avg_transport_amount as decimal(20, 2)
            )
          ) as transport_amount -- 未税运费
      from
           csx_dws.csx_dws_tms_entrucking_order_detail_new_di
      where
        sdt >= '20240601'
        and regexp_replace(send_date, '-', '') >= regexp_replace( add_months(trunc('2024-11-12', 'MM'), -5), '-', '')
        and regexp_replace(send_date, '-', '') <= regexp_replace(date_sub(current_date(), 1), '-', '')
        and supper_order_type_name in ('B端', 'M端', 'BBC')
      group by
        regexp_replace(send_date, '-', ''),
        customer_code,
        shipped_order_code,
        if(access_caliber = 2, 99, business_type_code)
      )
   select a.order_code,
        a.business_type_code,
        group_flag,
        a.type_flag,
        a.operation_mode_name,
        -- business_type_name,
        a.customer_code,
        transport_amount
    from 
   (select 
        order_code,
        group_flag,
        a.type_flag,
        a.operation_mode_name,
        business_type_code,
        business_type_name,
        customer_code
    from csx_analyse_tmp.csx_analyse_tmp_flbbs_sale_detail a 
    group by order_code, business_type_code, business_type_name, customer_code,
    group_flag,
    a.type_flag,
    a.operation_mode_name
   ) a 
    left join tmp_tms_order_detail b on a.customer_code = b.customer_code and a.order_code = b.shipped_order_code
     where b.shipped_order_code is not null 
    ;
    
    -- select * from csx_analyse_tmp.csx_analyse_tmp_tms_entrucking_order
			
			
-- 结果表
with tmp_flbbc_sale as (
  select
    a.group_flag,
    a.type_flag,
    a.operation_mode_name,
    a.sale_quarter,
    a.month,
    a.sdt,
    a.order_code,
    a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name,
    a.business_type_code,
    a.business_type_name,
    a.customer_code,
    sum(sale_qty) sale_qty,
    sum(sale_cost) sale_cost,
    sum(sale_amt) sale_amt,
    sum(profit) profit,
    sum(sale_amt_no_tax) sale_amt_no_tax,
    sum(profit_no_tax) profit_no_tax
  from
    csx_analyse_tmp.csx_analyse_tmp_flbbs_sale_detail a
    --   where customer_code='237565'
  group by
    a.group_flag,
    a.type_flag,
    a.operation_mode_name,
    a.sale_quarter,
    a.month,
    a.sdt,
    a.order_code,
    a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name,
    a.business_type_code,
    a.business_type_name,
    a.customer_code
) 
--   select * from tmp_flbbc_sale where order_code='2409090610840609'
,
tmp_sale_expense as (
  select 
    a.group_flag,
    a.type_flag,
    a.operation_mode_name,
    a.sale_quarter,
    a.month,
    -- a.sdt,
    -- a.order_code,
    a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name,
    a.business_type_code,
    a.business_type_name,
    a.customer_code,
    sum(sale_qty) sale_qty,
    sum(sale_cost) sale_cost,
    sum(sale_amt) sale_amt,
    sum(profit) profit,
    sum(sale_amt_no_tax) sale_amt_no_tax,
    sum(profit_no_tax) profit_no_tax,
    sum(coalesce(bbc_express_amount, 0)) bbc_express_amount,
    sum(transport_amount) transport_amount
  from
    (
      select
        a.group_flag,
        a.type_flag,
        a.operation_mode_name,
        a.sale_quarter,
        a.month,
        -- a.sdt,
        a.order_code,
        a.performance_region_code,
        a.performance_region_name,
        a.performance_province_code,
        a.performance_province_name,
        a.performance_city_code,
        a.performance_city_name,
        a.business_type_code,
        a.business_type_name,
        a.customer_code,
        sum(sale_qty) sale_qty,
        sum(sale_cost) sale_cost,
        sum(sale_amt) sale_amt,
        sum(profit) profit,
        sum(sale_amt_no_tax) sale_amt_no_tax,
        sum(profit_no_tax) profit_no_tax
      from
        tmp_flbbc_sale a
      group by
        a.group_flag,
        a.type_flag,
        a.operation_mode_name,
        a.sale_quarter,
        a.month,
        -- a.sdt,
        a.order_code,
        a.performance_region_code,
        a.performance_region_name,
        a.performance_province_code,
        a.performance_province_name,
        a.performance_city_code,
        a.performance_city_name,
        a.business_type_code,
        a.business_type_name,
        a.customer_code
    ) a
    left join (
      select
        group_flag,
        type_flag,
        operation_mode_name,
        -- month,
        order_code,
        business_type_code,
        customer_code,
        sum(coalesce(bbc_express_amount,0)) bbc_express_amount
      from
        csx_analyse_tmp.csx_analyse_tmp_bbc_expense_detail
      group by
        group_flag,
        type_flag,
        operation_mode_name,
        -- month,
        order_code,
        business_type_code,
        customer_code
    ) b on a.order_code = b.order_code
    and a.operation_mode_name = b.operation_mode_name
    and a.type_flag = b.type_flag
    and a.business_type_code = b.business_type_code
    and a.group_flag = b.group_flag 
    and a.customer_code=b.customer_code
    left join
      csx_analyse_tmp.csx_analyse_tmp_tms_entrucking_order c 
      on a.order_code=c.order_code 
      and a.operation_mode_name=c.operation_mode_name 
      and a.type_flag=c.type_flag
      and a.customer_code=c.customer_code
    --   where a.customer_code='112919'
  group by
    a.group_flag,
    a.type_flag,
    a.operation_mode_name,
    a.sale_quarter,
    a.month,
    -- a.sdt,
    -- a.order_code,
    a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name,
    a.business_type_code,
    a.business_type_name,
    a.customer_code
)

--   and order_code = '2409090610840609'
 ,
  tmp_sss_detail as (
    select
      substr(sdt, 1, 6) smonth,
      customer_code,
      business_type_code,
      operation_mode_name,
      -- 剩余金额 认领未核销金额
      sum(if(sdt = max_calday, receivable_amount, 0)) as receivable_amount,
      -- 当日应收
      if(
        sum(receivable_amount) > 0,
        sum(receivable_amount),
        0
      ) * 0.06 / 365 as capital_takes_up -- 资金占用费
    from
      csx_analyse_tmp.csx_tmp_sss_customer_receivable_amount a
      join (
        select
          month_of_year,
          max(calday) max_calday
        from
          csx_dim.csx_dim_basic_date
        where
          calday >= '20240101'
          and calday <= regexp_replace(date_sub(current_date, 1), '-', '')
        group by
          month_of_year
      ) d on substr(a.sdt, 1, 6) = d.month_of_year -- where customer_code='237565'
    group by
      substr(sdt, 1, 6),
      customer_code,
      business_type_code,
      operation_mode_name
  ),
  tmp_customer_income_statement as (
    SELECT
      COALESCE (a.smonth, b.smt) AS smonth,
      COALESCE (a.region_code, b.performance_region_code) AS region_code,
      COALESCE (a.region_name, b.performance_region_name) AS region_name,
      COALESCE (a.province_code, b.performance_province_code) AS province_code,
      COALESCE (a.province_name, b.performance_province_name) AS province_name,
      COALESCE (a.city_group_code, b.performance_city_code) AS city_group_code,
      COALESCE (a.city_group_name, b.performance_city_name) AS city_group_name,
      COALESCE (a.customer_code, b.customer_code) AS customer_code,
      COALESCE (a.business_type_name, b.business_type_name) AS business_type_name,
      COALESCE (b.other_expenses, 0) AS other_expenses,
      COALESCE(b.other_expenses, 0) / (a.sales_value) as sales_ratio
    FROM
      (
        SELECT
          substr(sdt, 1, 6) smonth,
          performance_region_code AS region_code,
          performance_region_name AS region_name,
          performance_province_code AS province_code,
          performance_province_name AS province_name,
          performance_city_code AS city_group_code,
          performance_city_name AS city_group_name,
          customer_code,
          business_type_name,
          sum(sale_amt_no_tax) sales_value
        FROM
          csx_report.csx_report_sss_customer_income_statement_di
        WHERE
          sdt >= '20240601'
          and sdt <= '20241130'
        GROUP BY
          substr(sdt, 1, 6),
          performance_region_code,
          performance_region_name,
          performance_province_code,
          performance_province_name,
          performance_city_code,
          performance_city_name,
          customer_code,
          business_type_name
      ) a
      LEFT JOIN (
        SELECT
          smt,
          performance_region_code,
          performance_region_name,
          performance_province_code,
          performance_province_name,
          performance_city_code,
          performance_city_name,
          a.customer_code,
          customer_name,
          business_type_name,
          SUM(
            IF (expense_type_name = '其他费用', expense_amt_no_tax, 0)
          ) AS other_expenses,
          SUM(
            IF (expense_type_name = '后端费用', expense_amt_no_tax, 0)
          ) AS back_end_loads,
          SUM(
            IF (expense_type_name = '后台净收支', expense_amt_no_tax, 0)
          ) AS background_net_income,
          SUM(
            IF (expense_type_name = '未税运费', expense_amt_no_tax, 0)
          ) AS transport_amount
        FROM
          csx_dim.csx_dim_sss_customer_income_write_expense_mi a
        WHERE
          smt >= '202408'
          and smt <= '202410'
        GROUP BY
          smt,
          performance_region_code,
          performance_region_name,
          performance_province_code,
          performance_province_name,
          performance_city_code,
          performance_city_name,
          a.customer_code,
          customer_name,
          business_type_name
      ) b ON a.smonth = b.smt
      AND a.province_code = b.performance_province_code
      AND a.customer_code = b.customer_code
      and a.business_type_name = b.business_type_name
    where
      coalesce(COALESCE(b.other_expenses, 0) / (a.sales_value), 0) <> 0
  )
select
  '202408-202410' as qj,
  a.group_flag,
  a.month,
  a.type_flag,
  a.operation_mode_name,
  a.performance_region_name,
  a.performance_province_name,
  a.performance_city_name,
  a.business_type_code,
  a.business_type_name,
  a.customer_code,
  customer_name,
  sales_user_number,
  sales_user_name,
  first_category_name,
  second_category_name,
  third_category_name,
  max_business_create_date,
  (sale_qty) sale_qty,
  (sale_cost) sale_cost,
  (sale_amt) sale_amt,
  (profit) profit,
  profit / sale_amt as profit_rate,
  (sale_amt_no_tax) sale_amt_no_tax,
  (profit_no_tax) profit_no_tax,
  profit_no_tax / sale_amt_no_tax as profit_rate_no_tax,
  coalesce(receivable_amount, '') receivable_amount,
  -- 当日应收
  coalesce(capital_takes_up, '') capital_takes_up,
  coalesce(bbc_express_amount, '') bbc_express_amount,
  coalesce(transport_amount, '') transport_amount,
  coalesce(bbc_express_amount, 0) + coalesce(transport_amount, 0) as express_amt,
  coalesce(sales_ratio, '') sales_ratio,
  f.close_bill_amount -- 核销金额
from
  tmp_sale_expense a
  left join tmp_sss_detail b on a.customer_code = b.customer_code
  and a.month = b.smonth
  and a.operation_mode_name = b.operation_mode_name
  and a.business_type_code = b.business_type_code
  left join (
    select
      smonth,
      case
        when business_type_name = 'BBC' then '6'
        when business_type_name = '福利业务' then '2'
        else business_type_name
      end business_type_code,
      customer_code,
      sales_ratio
    from
      tmp_customer_income_statement
  ) c on a.customer_code = c.customer_code
  and a.business_type_code = c.business_type_code
  and a.month = c.smonth
  left join (
    select
      customer_code,
      business_type_code,
      max(create_time) max_business_create_date
    from
      csx_dim.csx_dim_crm_business_info
    where
      sdt = 'current'
      and business_type_code in (2, 6)
      and create_time >= '2024-08-01 00:00:00'
      and create_time < '2024-11-01 00:00:00' --  and customer_code='105561'
    group by
      customer_code,
      business_type_code
  ) e on a.customer_code = e.customer_code
  and a.business_type_code = e.business_type_code
  left join (
    select
      customer_code,
      customer_name,
      sales_user_number,
      sales_user_name,
      first_category_name,
      second_category_name,
      third_category_name
    from
      csx_dim.csx_dim_crm_customer_info
    where
      sdt = 'current'
  ) m on a.customer_code = m.customer_code -- 回款情况
  left join (
    select
      month,
      group_flag,
      type_flag,
      operation_mode_name,
      business_type_code,
      business_type_name,
      customer_code,
      sum(close_bill_amount) close_bill_amount -- 核销金额
    from
      csx_analyse_tmp.csx_analyse_tmp_close_bill_amt_order
    group by
      month,
      group_flag,
      type_flag,
      operation_mode_name,
      business_type_code,
      business_type_name,
      customer_code
  ) f on a.customer_code = f.customer_code
  and a.month = f.month
  and a.operation_mode_name = f.operation_mode_name
  and a.business_type_code = f.business_type_code
  and a.group_flag = f.group_flag
  and a.type_flag = f.type_flag -- where a.customer_code='237565'
;


-- 资金占用

-- 计算客户上月1号至今每日的当日应收金额、昨日应收金额、资金占用费
drop table csx_tmp.csx_tmp_sss_customer_receivable_amount;
create table csx_tmp.csx_tmp_sss_customer_receivable_amount as
 with tmp_sss_customer_receivable_amount as (
  -- 计算应收金额和剩余金额
  select
    sdt,
    customer_code,
    business_type_code,
    sum(coalesce(receivable_amount, 0)) as receivable_amount,
    -- 应收金额
    sum(coalesce(residue_amt_sss, 0)) as residue_amt_sss -- 剩余金额 认领未核销金额
  from
    (
      select
        a.sdt,
        a.customer_code,
        coalesce(b.business_type_code, c.business_type_code, 1) as business_type_code,
        sum(coalesce(a.unpaid_amount, 0)) as receivable_amount,
        -- 应收金额
        0 as residue_amt_sss -- 剩余金额 认领未核销金额
      from
        (
          -- 20230214日客户扩展到信控切表 历史得用客户表 因此3月后用新表
          select
            source_bill_no,
            -- 来源单号
            customer_code,
            -- 客户编码
            happen_date,
            -- 发生时间
            unpaid_amount,
            -- 未回款金额
            source_sys,
            -- 来源系统 MALL b端销售 BBC bbc端 BEGIN 期初
            sdt
          from
            csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di -- 销售结算对账开票结算详情表（新表）
          where
            sdt >= regexp_replace(
              last_day(add_months('${yesterday}', -2)),
              '-',
              ''
            )
            and sdt >= '20230301'
            and regexp_replace(date(happen_date), '-', '') <= sdt
          union all
          select
            source_bill_no,
            -- 来源单号
            customer_code,
            -- 客户编码
            happen_date,
            -- 发生时间
            unpaid_amount,
            -- 未回款金额
            source_sys,
            -- 来源系统 MALL b端销售 BBC bbc端 BEGIN 期初
            sdt
          from
            csx_dws.csx_dws_sss_order_invoice_bill_settle_detail_di -- 旧表
          where
            sdt >= regexp_replace(
              last_day(add_months('${yesterday}', -2)),
              '-',
              ''
            )
            and sdt < '20230301'
            and regexp_replace(date(happen_date), '-', '') <= sdt
        ) a
        left join csx_dws.csx_dws_sale_order_business_info_df b on a.source_bill_no = b.order_code
        and if(a.source_sys <> 'BEGIN', true, false)
        left join (
          select
            customer_code,
            min(business_type_code) as business_type_code
          from
            csx_dws.csx_dws_sale_order_business_info_df
          group by
            customer_code
        ) c on a.customer_code = c.customer_code
        and if(a.source_sys = 'BEGIN', true, false)
      group by
        a.sdt,
        a.customer_code,
        coalesce(b.business_type_code, c.business_type_code, 1)
      union all
      select
        sdt,
        customer_code,
        case
          credit_business_attribute_code
          when '1' then 1
          when '2' then 2
          when '3' then 5
          when '4' then 9
          when '5' then 6
          when '6' then 3
          else 1
        end as business_type_code,
        sum(0 - residue_amt_sss) as receivable_amount,
        -- 应收金额
        sum(residue_amt_sss) as residue_amt_sss -- 剩余金额 认领未核销金额
      from
        csx_dws.csx_dws_sss_customer_credit_invoice_bill_settle_stat_di
      where
        sdt >= regexp_replace(
          last_day(add_months('${yesterday}', -2)),
          '-',
          ''
        )
        and residue_amt_sss <> 0
      group by
        sdt,
        customer_code,
        case
          credit_business_attribute_code
          when '1' then 1
          when '2' then 2
          when '3' then 5
          when '4' then 9
          when '5' then 6
          when '6' then 3
          else 1
        end
    ) a
  group by
    sdt,
    customer_code,
    business_type_code
)
select
  sdt,
  customer_code,
  business_type_code,
  sum(coalesce(residue_amt_sss, 0)) as residue_amt_sss,
  -- 剩余金额 认领未核销金额
  sum(coalesce(receivable_amount, 0)) as receivable_amount,
  -- 当日应收
  sum(coalesce(receivable_amount_dod, 0)) as receivable_amount_dod,
  -- 前1日应收
  (
    if(
      sum(receivable_amount) > 0,
      sum(receivable_amount),
      0
    ) + if(
      sum(receivable_amount_dod) > 0,
      sum(receivable_amount_dod),
      0
    )
  ) / 2 * 0.06 / 365 as capital_takes_up -- 资金占用费
from
  (
    select
      sdt,
      customer_code,
      business_type_code,
      receivable_amount,
      0 receivable_amount_dod,
      residue_amt_sss
    from
      tmp_sss_customer_receivable_amount
    union all
    select
      regexp_replace(
        date_sub(
          to_date(from_unixtime(unix_timestamp(sdt, 'yyyyMMdd'))),
          -1
        ),
        '-',
        ''
      ) as sdt,
      customer_code,
      business_type_code,
      0 receivable_amount,
      receivable_amount as receivable_amount_dod,
      0 residue_amt_sss
    from
      tmp_sss_customer_receivable_amount
  ) a
where
  sdt >= regexp_replace(
    add_months(trunc('${yesterday}', 'MM'), -1),
    '-',
    ''
  )
  and sdt <= regexp_replace(date_sub(current_date(), 1), '-', '')
group by
  sdt,
  customer_code,
  business_type_code;
  
  

-- 8-10月销售单的回款情况
drop table  csx_analyse_tmp.csx_analyse_tmp_close_bill_amt_order;
create table csx_analyse_tmp.csx_analyse_tmp_close_bill_amt_order as 
  select a.source_bill_no_new,
		 a.month,
		 a.group_flag,
		 a.type_flag,
		 a.operation_mode_name,
		 a.business_type_code,
		 a.business_type_name,
		 a.customer_code,
		 a.sale_amt,
		 b.order_amt, -- 源单据对账金额
		 b.close_bill_amount -- 核销金额
  from 
    (select -- order_code,
		case when business_type_code='6' and substr(split(a.order_code,'-')[0],1,1)='B' and substr(split(a.order_code,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(a.order_code,'-')[0],2,length(split(a.order_code,'-')[0])-2)
			 when business_type_code='6' and substr(split(a.order_code,'-')[0],1,1)='B' and substr(split(a.order_code,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(a.order_code,'-')[0],2,length(split(a.order_code,'-')[0])-1)
			 else split(a.order_code,'-')[0]
			 end as source_bill_no_new,
		month,
		group_flag,
		type_flag,
		operation_mode_name,
		business_type_code,
		business_type_name,
		customer_code,
		sum(sale_amt) sale_amt
    from csx_analyse_tmp.csx_analyse_tmp_flbbs_sale_detail a
        group by -- order_code ,
		case when business_type_code='6' and substr(split(a.order_code,'-')[0],1,1)='B' and substr(split(a.order_code,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(a.order_code,'-')[0],2,length(split(a.order_code,'-')[0])-2)
			 when business_type_code='6' and substr(split(a.order_code,'-')[0],1,1)='B' and substr(split(a.order_code,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(a.order_code,'-')[0],2,length(split(a.order_code,'-')[0])-1)
			 else split(a.order_code,'-')[0]
			 end,			
		month,
		group_flag,
		type_flag,
		operation_mode_name,
		business_type_code,
		business_type_name,
		customer_code
    )a 
	left join 	  
	(
      select
        -- source_bill_no, -- 来源单号
         case when substr(source_bill_no,1,1) ='B' then substr(source_bill_no, 2,length(source_bill_no)-2)  -- 涉及 BBC单号有字母开头的处理
          else split(source_bill_no,'-')[0] end as new_source_bill_no,
        customer_code, -- 客户编码
        max(happen_date) happen_date, -- 发生时间
        source_sys, -- 来源系统 MALL b端销售 BBC bbc端 BEGIN 期初
		sum(close_bill_amount) order_amt, -- 源单据对账金额
		sum(close_bill_amount) close_bill_amount -- 核销金额
      from csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di -- 销售结算对账开票结算详情表（新表）
      where sdt = '20250131' -- 现在算截止到10月底的回款 下月算截止到11月底的回款
        and regexp_replace(date(happen_date), '-', '') between '20240601' and '20241130'
       -- and source_bill_no='R2408010410590817A'
        -- and customer_code='130536'
		group by 
         case when substr(source_bill_no,1,1) ='B' then substr(source_bill_no, 2,length(source_bill_no)-2)  -- 涉及 BBC单号有字母开头的处理
          else split(source_bill_no,'-')[0] end,customer_code,source_sys			  
	)b on b.new_source_bill_no = a.source_bill_no_new and b.customer_code = a.customer_code	
;  
  