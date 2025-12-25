-- 大客户提成销售查询，这里扣减毛利后的销售毛利
--  BBC剔除永辉生活、永辉线下购，剔除前置仓

select *
from csx_analyse.csx_analyse_fr_tc_customer_sale_fwf_business
where smt = '202511'
    and customer_code = '130860'

-- 回款查询，涉及到业务发生日期不同，特别是BBC 
select bill_type
        ,source_bill_no
        ,source_bill_no_new
        ,customer_code
        ,credit_code
        ,happen_date
        ,company_code
        ,source_sys
        ,reconciliation_period
        ,bill_date
        ,overdue_date
        ,paid_date_new as paid_date       
        ,order_amt
        ,unpay_amt
        ,history_pay_amt
        ,pay_amt_old
        ,pay_amt
        ,smonth
        ,smt
        ,sdt
        ,bbc_bill_flag
 from csx_analyse.csx_analyse_customer_verification_detail_mf   a
 where smt=substr(regexp_replace(last_day(add_months('2025-12-08',-1)),'-',''),1,6)
-- and  source_bill_no='2511290614738253'
    and customer_code='130860'
