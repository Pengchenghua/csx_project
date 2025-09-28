-- 年度销售员新客履约汇总

  select
    province_name,
    a.city_group_name,
    a.business_type_code,
    a.business_type_name,
    a.end_date,
    a.smonth,
    a.customer_no,
    a.customer_name,
    sales_user_number,
    sales_user_name,
    sale_amt
  from
    (
      select
        distinct *
      from
        (
          select
            *
          from
            csx_analyse.csx_analyse_sale_d_customer_sign_new_about_di
          where
            smonth between '202401' and '202412'
          union all
          select
            *
          from
            csx_analyse.csx_analyse_sale_d_customer_new_about_di
          where
            smonth between '202401' and '202412'
        ) a
    ) a
    left join (
      select
        substr(sdt, 1, 6) smonth,
        customer_code,
        business_type_code,
        sales_user_number,
        sales_user_name,
        sum(sale_amt) as sale_amt
      from
        csx_dws.csx_dws_sale_detail_di
      where
        sdt >= '20240101'
        and sdt <= '20241231'
        and business_type_code in (1, 2, 6)
        and channel_code in ('1', '7', '9')
      group by
        substr(sdt, 1, 6),
        customer_code,
        business_type_code,
        sales_user_number,
        sales_user_name
    ) b on a.customer_no = b.customer_code
    and a.business_type_code = b.business_type_code
    and a.smonth = b.smonth



-- 逾期
 
  	 select
         sdt,
         channel_name,
         performance_province_name province_name,
         performance_city_name city_group_name,
         customer_code,
         customer_name,
         sales_employee_code,
         sales_employee_name,
         sum(overdue_amount)overdue_amount,
         sum(receivable_amount)  receivable_amount --应收账款
         
       from 
        -- csx_dws.csx_dws_sss_customer_credit_invoice_bill_settle_stat_di
        -- 取SAP 
           csx_dws.csx_dws_sap_customer_credit_settle_detail_di 
	 -- csx_dws_sss_customer_invoice_bill_settle_stat_di
       where (sdt='20241211' )  
         and channel_code  in ('1','7','9')
	 --  and customer_code not in ('120459','121206')
       group by sdt,
         channel_name,
         performance_province_name,
         performance_city_name,
         customer_code,
         sales_employee_code,
         sales_employee_name,
         customer_name
  ;


  -- 销售额
  select
  customer_code,
  customer_name,
  performance_province_name province_name,
  performance_city_name city_group_name,
  substr(sdt, 1, 6) smonth,
  business_type_name,
  sales_user_number,
  sales_user_name,
  sum(sale_amt) as sale_amt,
  sum(profit) profit,
  sum(profit_no_tax) excluding_tax_profit,
  sum(sale_amt_no_tax) excluding_tax_sales
from
  csx_dws.csx_dws_sale_detail_di
where
  sdt >= '20240101'
  and sdt <= '20241211' -- and customer_code not in ('120459','121206')
  and business_type_code in ('1', '2', '4', '6') --     and channel_code in ('1','7','9')
group by
  customer_code,
  customer_name,
  performance_province_name,
  performance_city_name,
  substr(sdt, 1, 6),
  business_type_name,
  sales_user_number,sales_user_name