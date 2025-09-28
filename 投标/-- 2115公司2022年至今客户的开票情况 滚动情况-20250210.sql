-- 2115公司2022年至今客户的开票情况 滚动情况-20250210
with tmp_invoice as ( 
select  happen_month,
        CAST(SUBSTR(happen_month, 1, 4) AS INT) * 100 + CAST(SUBSTR(happen_month, 6, 2) AS INT) AS month_sort,
        b.bloc_code,     --  集团编码
        b.bloc_name,     --  集团名称                
        a.company_code,
        b.performance_region_name,     --  销售大区名称(业绩划分)
        b.performance_province_name,     --  销售归属省区名称
        b.performance_city_name,     --  城市组名称(业绩划分)
        a.customer_code,
        b.customer_name,     --  客户名称
        b.first_category_name,     --  一级客户分类名称
        b.second_category_name,     --  二级客户分类名称
        invoice_amount
from
(        
select
        a.customer_code,
        a.company_code,
        substr(regexp_replace(happen_date,'-',''),1,6) as happen_month,
        sum(a.invoice_amount) invoice_amount
from 
(
        select 
        case when source_sys='BBC' and substr(split(source_bill_no,'-')[0],1,1)='B' and substr(split(source_bill_no,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(source_bill_no,'-')[0],2,length(split(source_bill_no,'-')[0])-2)
                 when source_sys='BBC' and substr(split(source_bill_no,'-')[0],1,1)='B' and substr(split(source_bill_no,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(source_bill_no,'-')[0],2,length(split(source_bill_no,'-')[0])-1)
                 else split(source_bill_no,'-')[0]
                 end as source_bill_no_new,
                bill_type,  -- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
                sdt,
                source_sys,        -- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初
                source_bill_no,        -- 来源单号        
                customer_code,        -- 客户编码
                credit_code,        -- 信控号
                happen_date,        -- 发生时间
                order_amt,        -- 源单据对账金额
                company_code,        -- 签约公司编码
                residue_amt,        -- 剩余预付款金额_预付款客户抵消订单金额后
                residue_amt_sss,        -- 剩余预付款金额_原销售结算
                unpaid_amount,        -- 未回款金额_抵消预付款后
                unpaid_amount_sss,        -- 未回款金额_原销售结算
                bad_debt_amount,        -- 坏账金额
                account_period_code,        -- 账期编码
                account_period_name,        -- 账期名称
                account_period_value,        -- 账期值
                invoice_amount          -- 开票金额
        from csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di
        where sdt=regexp_replace(add_months(date_sub(current_date,1),0),'-','')
        and date_format(happen_date,'yyyy-MM-dd')>='2023-01-01'
        and company_code='2115'
)a
left join 
(
        select 
        -- order_code,
        case when business_type_code='6' and substr(split(order_code,'-')[0],1,1)='B' and substr(split(order_code,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(order_code,'-')[0],2,length(split(order_code,'-')[0])-2)
                 when business_type_code='6' and substr(split(order_code,'-')[0],1,1)='B' and substr(split(order_code,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(order_code,'-')[0],2,length(split(order_code,'-')[0])-1)
                 else split(order_code,'-')[0]
                 end as order_code_new,        
        business_type_code,
        business_type_name,
        sum(sale_amt) as sale_amt,
        sum(profit) as profit
        from csx_dws.csx_dws_sale_detail_di
        where channel_code in('1','7','9')
        -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
        and business_type_code in('1')
        group by 
        case when business_type_code='6' and substr(split(order_code,'-')[0],1,1)='B' and substr(split(order_code,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(order_code,'-')[0],2,length(split(order_code,'-')[0])-2)
                 when business_type_code='6' and substr(split(order_code,'-')[0],1,1)='B' and substr(split(order_code,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(order_code,'-')[0],2,length(split(order_code,'-')[0])-1)
                 else split(order_code,'-')[0]
                 end,                
        business_type_code,
        business_type_name
)b on a.source_bill_no_new=b.order_code_new
group by a.customer_code,a.company_code,substr(regexp_replace(happen_date,'-',''),1,6)
)a
left join 
(
        select  
                bloc_code,     --  集团编码
                bloc_name,     --  集团名称
                parent_id,customer_id,
                customer_code,
                customer_name,     --  客户名称
                first_category_name,     --  一级客户分类名称
                second_category_name,     --  二级客户分类名称
                performance_region_name,     --  销售大区名称(业绩划分)
                performance_province_name,     --  销售归属省区名称
                performance_city_name     --  城市组名称(业绩划分)
        from csx_dim.csx_dim_crm_customer_info
        where sdt='current'
        and customer_type_code=4
)b on a.customer_code=b.customer_code
-- where a.customer_code in ('100326')
order by happen_month
),
tmp_invoice_month as 
(select happen_month,
        -- CAST(SUBSTR(happen_month, 1, 4) AS INT) * 100 + CAST(SUBSTR(happen_month, 6, 2) AS INT) AS month_sort,
        bloc_code,     --  集团编码
        bloc_name,     --  集团名称                
        company_code,
        performance_region_name,     --  销售大区名称(业绩划分)
        performance_province_name,     --  销售归属省区名称
        performance_city_name,     --  城市组名称(业绩划分)
        customer_code,
        customer_name,     --  客户名称
        first_category_name,     --  一级客户分类名称
        second_category_name,     --  二级客户分类名称
        invoice_amount,
        SUM(invoice_amount) OVER (partition by customer_code
            ORDER BY happen_month
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS rolling_total
from tmp_invoice
order by happen_month
)
 select * from (
SELECT 
    LAG(happen_month, 11) OVER (partition by customer_code ORDER BY  happen_month asc ) AS start_month,
   FROM_UNIXTIME(unix_timestamp(from_unixtime(UNIX_TIMESTAMP(happen_month,'yyyyMM'),'yyyy-MM-01'),'yyyy-MM-01') - (11 * 30 * 24 * 60 * 60),'yyyyMM') AS  formatted_date,
    happen_month AS end_month,
    bloc_code,     --  集团编码
    bloc_name,     --  集团名称                
    company_code,
    performance_region_name,     --  销售大区名称(业绩划分)
    performance_province_name,     --  销售归属省区名称
    performance_city_name,     --  城市组名称(业绩划分)
    customer_code,
    customer_name,
    invoice_amount,
    rolling_total
FROM tmp_invoice_month
WHERE rolling_total IS NOT NULL
) a 
where 1=1 
order by customer_code,end_month
;
