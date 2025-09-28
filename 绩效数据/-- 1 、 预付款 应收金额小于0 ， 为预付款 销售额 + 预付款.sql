-- 1 、 预付款 应收金额小于0 ， 为预付款 销售额 + 预付款
with tmp_sale_detail as 
(select sdt,performance_province_name,
    performance_city_name,
    customer_code,
    customer_name,
    credit_code,
    company_code,
    sum(sale_amt) as sale_amt,
    sum(profit) as profit
from csx_dws.csx_dws_sale_detail_di
where sdt >= '20250501'
    and sdt <= '20250531'
    and business_type_code=6
group by sdt,performance_province_name,
    performance_city_name,
    customer_code,
    customer_name,
    credit_code,
    company_code
),
tmp_receive_detail as
(select sdt,
    customer_code,
    credit_code,
    company_code,
    receivable_amount
from csx_dws.csx_dws_sss_customer_credit_invoice_bill_settle_stat_di
where sdt >= '20250430'
    and shipper_code = 'YHCSX'
    -- and receivable_amount < 0
    )
select a.sdt,
    a.performance_province_name,
    a.performance_city_name,
    a.customer_code,
    a.customer_name,
    a.credit_code,
    a.company_code,
    a.sale_amt as sale_amt,
    a.profit as profit,
    receivable_amount,
    -- 当应收小于0，销售额+应收绝对值，计入预付款金额
    if(b.receivable_amount<0, a.sale_amt+abs(b.receivable_amount), a.sale_amt) as yufu_amount
from tmp_sale_detail a 
left join tmp_receive_detail b ON
a.sdt = b.sdt
and a.customer_code = b.customer_code
and a.company_code = b.company_code
and a.credit_code = b.credit_code
