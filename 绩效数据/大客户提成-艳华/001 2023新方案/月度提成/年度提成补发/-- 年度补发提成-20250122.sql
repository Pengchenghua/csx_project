-- 年度补发提成-20250122
with tmp_service_sales_tc as (
  select
    '服务管家' as person_flag,
    smt,
    region_name,
    province_name,
    city_group_name,
    sales_user_work_no,
    sales_user_name,
    sales_profit_basic,
    sales_profit_finish,
    sales_target_rate,
    sales_target_rate_tc,
    sum(pay_amt) pay_amt,
    sum(tc_sales) tc_sales,
    -- sum(sum(sales_profit_basic))over(partition by city_group_name,sales_user_work_no)     as sales_profit_basic_new,
    -- sum(sum(sales_profit_finish))over (partition by city_group_name,sales_user_work_no) as sales_profit_new,
    -- sum(sum(sales_profit_finish))over (partition by city_group_name,sales_user_work_no)/sum(sum(sales_profit_basic))over(partition by city_group_name,sales_user_work_no)  as sales_profit_target_rate_new,
    -- if(sum(sum(sales_profit_finish))over (partition by city_group_name,sales_user_work_no)/sum(sum(sales_profit_basic))over(partition by city_group_name,sales_user_work_no)>=1,1,sum(sum(sales_profit_finish))over (partition by city_group_name,sales_user_work_no)/sum(sum(sales_profit_basic))over(partition by city_group_name,sales_user_work_no)
    -- ) sales_profit_target_rate_tc_new,
    sum(tc_sales_new) tc_sales_new,
    sum(tc_sales_new) - sum(tc_sales) as tc_sales_dff_bu -- 需补发提成
  from
    (
      -- 日配管家
      select
        smt,
        region_name,
        province_name,
        city_group_name,
        rp_service_user_work_no as sales_user_work_no,
        rp_service_user_name as sales_user_name,
        (rp_service_profit_basic) as sales_profit_basic,
        (rp_service_profit_finish) as sales_profit_finish,
        rp_service_target_rate as sales_target_rate,
        (rp_service_target_rate_tc) as sales_target_rate_tc,
        sum(rp_pay_amt) as pay_amt,
        sum(tc_rp_service) as tc_sales,
        -- sum(rp_service_profit_basic)over(partition by rp_service_user_work_no,city_group_name) as sales_profit_basic_new,
        -- sum(rp_service_profit_finish)over(partition by rp_service_user_work_no,city_group_name) as sales_profit_new,
        -- rp_service_profit_target_rate as sales_profit_target_rate_new,
        -- (rp_service_target_rate_tc) as sales_profit_target_rate_tc_new,
        sum(original_tc_rp_service) as tc_sales_new
      from
        csx_analyse.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail
      where
        smt between '202401' and '202412'
        and rp_service_user_work_no <> ''
      group by
        smt,
        region_name,
        province_name,
        city_group_name,
        rp_service_user_work_no,
        rp_service_user_name,
        rp_service_profit_basic,
        rp_service_profit_finish,
        rp_service_target_rate,
        rp_service_target_rate_tc
      union all
      select
        smt,
        region_name,
        province_name,
        city_group_name,
        fl_service_user_work_no as sales_user_work_no,
        fl_service_user_name as sales_user_name,
        (fl_service_profit_basic) as sales_profit_basic,
        (fl_service_profit_finish) as sales_profit_finish,
        fl_service_target_rate as sales_target_rate,
        (fl_service_target_rate_tc) as sales_target_rate_tc,
        sum(fl_pay_amt) as pay_amt,
        sum(tc_fl_service) as tc_sales,
        -- sum(fl_service_profit_basic)over(partition by city_group_name,fl_service_user_work_no)     as sales_profit_basic_new,
        -- sum(fl_service_profit_finish)over (partition by city_group_name,fl_service_user_work_no) as sales_profit_new,
        -- fl_service_profit_target_rate_new as sales_profit_target_rate_new,
        -- (fl_service_target_rate_tc) as sales_profit_target_rate_tc_new,
        sum(original_tc_fl_service) as tc_sales_new
      from
        csx_analyse.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail
      where
        smt between '202401' and '202412'
        and fl_service_user_work_no <> ''
      group by
        smt,
        region_name,
        province_name,
        city_group_name,
        fl_service_user_work_no,
        fl_service_user_name,
        (fl_service_profit_basic),
        (fl_service_profit_finish),
        fl_service_target_rate,
        fl_service_target_rate_tc
      union all
      select
        smt,
        region_name,
        province_name,
        city_group_name,
        bbc_service_user_work_no as sales_user_work_no,
        bbc_service_user_name as sales_user_name,
        (bbc_service_profit_basic) as sales_profit_basic,
        (bbc_service_profit_finish) as sales_profit_finish,
        bbc_service_target_rate as sales_target_rate,
        (bbc_service_target_rate_tc) as sales_target_rate_tc,
        sum(bbc_pay_amt) as pay_amt,
        sum(tc_bbc_service) as tc_sales,
        -- sum(bbc_service_profit_basic)over(partition by city_group_name,bbc_service_user_work_no)  as sales_profit_basic_new,
        -- sum(bbc_service_profit_finish)over(partition by city_group_name,bbc_service_user_work_no)as sales_profit_new,
        -- bbc_service_profit_target_rate_new as sales_profit_target_rate_new,
        -- (bbc_service_target_rate_tc) as sales_profit_target_rate_tc_new,
        sum(original_tc_bbc_service) as tc_sales_new
      from
        csx_analyse.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail
      where
        smt between '202401' and '202412'
        and bbc_service_user_work_no <> ''
      group by
        smt,
        region_name,
        province_name,
        city_group_name,
        bbc_service_user_work_no,
        bbc_service_user_name,
        (bbc_service_profit_basic),
        (bbc_service_profit_finish),
        bbc_service_target_rate,
        bbc_service_target_rate_tc
    ) a
  group by
    smt,
    region_name,
    province_name,
    city_group_name,
    sales_user_work_no,
    sales_user_name,
    sales_profit_basic,
    sales_profit_finish,
    sales_target_rate,
    sales_target_rate_tc
),
tmp_sale_tc as (
  select
    person_flag,
    smt,
    region_name,
    province_name,
    city_group_name,
    work_no,
    sales_name,
    sales_profit_basic,
    sales_profit_finish,
    sales_target_rate,
    sales_target_rate_tc,
    (pay_amt) pay_amt,
    (tc_sales) tc_sales,
    sum(sales_profit_basic) over(partition by work_no, city_group_name) as sales_profit_basic_new,
    sum(sales_profit_finish) over(partition by work_no, city_group_name) as sales_profit_new,
    sum(sales_profit_finish) over(partition by work_no, city_group_name) / sum(sales_profit_basic) over(partition by work_no, city_group_name) sales_profit_target_rate_new,
    if(
      sum(sales_profit_finish) over(partition by work_no, city_group_name) / sum(sales_profit_basic) over(partition by work_no, city_group_name) >= 1,
      1,
      sum(sales_profit_finish) over(partition by work_no, city_group_name) / sum(sales_profit_basic) over(partition by work_no, city_group_name)
    ) sales_profit_target_rate_tc_new,
    tc_sales_new,
    (tc_sales_new) - (tc_sales) as tc_sales_dff_bu -- 需补发提成
  from
    (
      select
        '销售员' as person_flag,
        smt,
        region_name,
        province_name,
        city_group_name,
        work_no,
        sales_name,
        sales_profit_basic,
        -- 销售基准毛利额
        sales_profit_finish,
        -- 实际毛利额
        sales_target_rate,
        -- 销售员毛利额_达成率
        sales_target_rate_tc,
        -- 销售员毛利额_达成系数
        sum(pay_amt) pay_amt,
        -- 回款金额
        sum(tc_sales) tc_sales,
        -- 销售员提成额
        -- sum(sum(sales_profit_basic))over(partition by work_no,city_group_name) as sales_profit_basic_new,
        -- sum(sum(sales_profit_finish))over(partition by work_no,city_group_name) as sales_profit_new,
        -- sum(sum(sales_profit_finish))over(partition by work_no,city_group_name)/sum(sum(sales_profit_basic))over(partition by work_no,city_group_name)  sales_profit_target_rate_new,
        -- sales_target_rate_tc sales_profit_target_rate_tc_new,   -- 年度达成系数
        sum(original_tc_sales) tc_sales_new,
        -- 原始提成额
        sum(original_tc_sales) - sum(tc_sales) as tc_sales_dff_bu -- 需补发提成
      from
        csx_analyse.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail
      where
        smt between '202401' and '202412'
        and work_no <> ''
      group by
        smt,
        region_name,
        province_name,
        city_group_name,
        work_no,
        sales_name,
        sales_profit_basic,
        sales_profit_finish,
        sales_target_rate,
        sales_target_rate_tc
    ) a
)

select 

  person_flag,
  smt,
  region_name,
  province_name,
  city_group_name,
  sales_user_work_no,
  sales_user_name,
  sales_profit_basic,
  sales_profit_finish,
  sales_target_rate,
  sales_target_rate_tc,
  pay_amt,
  tc_sales,
  sales_profit_basic_new,
  sales_profit_new,
  sales_profit_target_rate_new,
  if(sales_profit_target_rate_tc_new>=1,1,0) sales_profit_target_rate_tc_new,
  tc_sales_new,
  if(sales_profit_target_rate_tc_new>=1,(tc_sales_new) - (tc_sales),0) as tc_sales_dff_bu -- 需补发提成
from
(
select
  person_flag,
  smt,
  region_name,
  province_name,
  city_group_name,
  work_no as sales_user_work_no,
  sales_name as sales_user_name,
  sales_profit_basic,
  sales_profit_finish,
  sales_target_rate,
  sales_target_rate_tc,
  pay_amt,
  tc_sales,
  sales_profit_basic_new,
  sales_profit_new,
  sales_profit_target_rate_new,
  sales_profit_target_rate_tc_new,
  tc_sales_new,
  (tc_sales_new) - (tc_sales) as tc_sales_dff_bu -- 需补发提成
from
  tmp_sale_tc
union all
select
  person_flag,
  smt,
  region_name,
  province_name,
  city_group_name,
  sales_user_work_no,
  sales_user_name,
  sales_profit_basic,
  sales_profit_finish,
  sales_target_rate,
  sales_target_rate_tc,
  (pay_amt) pay_amt,
  (tc_sales) tc_sales,
  sum(sales_profit_basic) over(partition by city_group_name, sales_user_work_no) as sales_profit_basic_new,
  sum(sales_profit_finish) over (partition by city_group_name, sales_user_work_no) as sales_profit_new,
  sum(sales_profit_finish) over (partition by city_group_name, sales_user_work_no) / sum(sales_profit_basic) over(partition by city_group_name, sales_user_work_no) as sales_profit_target_rate_new,
  if(
    sum(sales_profit_finish) over (partition by city_group_name, sales_user_work_no) / sum(sales_profit_basic) over(partition by city_group_name, sales_user_work_no) >= 1,
    1,
    0
  ) sales_profit_target_rate_tc_new,
  (tc_sales_new) tc_sales_new,
  (tc_sales_new) - (tc_sales) as tc_sales_dff_bu -- 需补发提成
from
  tmp_service_sales_tc
  ) a 