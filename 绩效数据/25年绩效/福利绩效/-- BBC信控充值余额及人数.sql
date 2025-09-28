-- BBC信控充值余额及人数
select
  a.s_month,
  a.performance_region_name,
  a.performance_province_name,
  a.performance_city_name,
  a.customer_code,
  a.customer_name,
  sales_user_number,
  sales_user_name,
  second_category_name,
  credit_balance,
  count_person
from
  (
    select
    	 substr(sdt,1,6)s_month,
      performance_region_name,
      performance_province_name,
      performance_city_name,
      customer_code,
      customer_name,
      second_category_name,
      credit_balance,
      count_person
    from
      csx_analyse.csx_analyse_fr_bbc_wshop_user_credit_di
    where
      substr(sdt,1,6) = '${smt}'
      and (
        credit_balance is not null
        or count_person is not null
      )
  ) a
  left join (
    select
      customer_code,
      sales_user_number,
      sales_user_name
    from
      csx_dim.csx_dim_crm_customer_info
    where sdt='current'
  ) b on a.customer_code = b.customer_code
  ;

with tmp_bbc_pay_log as (
select create_month,customer_code,
  sum(
    case
      when pay_or_income = '支出' then pay_amt_new
    end
  ) as customer_pay_amt,
  -- 客户支出金额
  sum(
    case
      when pay_or_income = '收入' then pay_amt_new
    end
  ) as customer_income,
  -- 客户收入金额
  count(
    distinct case
      when pay_or_income = '支出' then telephone
    end
  ) as count_person_pay,
  count(
    distinct case
      when pay_or_income = '收入' then telephone
    end
  ) as count_person_income
from (
    select substr(regexp_replace(substr(cast(create_time as string), 1, 10), '-', ''),1,6) as create_month,
     customer_code,
      pay_type,
      pay_amt,
      telephone,
      case
        when pay_type in (1, 2) then '支出'
        when pay_type in (3, 5, 7, 4, 6, 8, 9) then '收入'
      end as pay_or_income,
      case
        when pay_type in (2, 4, 6, 8, 9) then 0 - pay_amt
        else pay_amt
      end as pay_amt_new
    from csx_dwd.csx_dwd_bbc_wshop_credit_pay_log_df
    where pay_type in (1, 2, 3, 4, 6, 8, 9)
      and regexp_replace(substr(cast(create_time as string), 1, 10), '-', '') between '${SDATE}' and '${EDATE}'
  ) a
group by customer_code,
create_month
)
select create_month,
  performance_region_name,
  performance_province_name,
  performance_city_name,
  customer_code,
  customer_name,
  sales_user_number,
 sales_user_name,
  customer_pay_amt,  -- 客户支出金额
  customer_income, -- 客户收入金额
  count_person_pay, -- 支出人数
  count_person_income -- 收入人数
 from tmp_bbc_pay_log a 
 left join (SELECT customer_code,customer_name,performance_city_name,
 performance_province_name,
 performance_region_name,
 sales_user_number,
 sales_user_name
  FROM csx_dim.csx_dim_crm_customer_info WHERE sdt='current') b
) b on a.customer_code = b.customer_code;