 -- 2020年至今福利开票回款金额及开票商品明细
with tmp_close_bill_account_record_di as 
(select  
  customer_code,
  credit_code,
  max(to_date(paid_time)) paid_date,
  close_bill_code           -- 销售单号
from
  csx_dwd.csx_dwd_sss_close_bill_account_record_di
where
  delete_flag = 0
  and sdt>='20200101'
  group by  customer_code,
    credit_code,
    close_bill_code   -- 销售单号
  ),
  -- 注意如果查询失败需要建临时表
  tmp_sss_customer_statement_account_di as 

 (
select
  company_code,
  customer_code,
  max(to_date(invoice_time)) invoice_date,
  source_bill_no, 
  sum(residue_amt)residue_amt
from
    csx_dwd.csx_dwd_sss_invoice_di a
  left join (
    select
      order_code,
      source_bill_no, -- 销售单号
      sum(residue_amt) residue_amt
    from
      csx_dwd.csx_dwd_sss_kp_apply_goods_group_detail_di
    where
        sdt>='20200101'
    --   and invoice_status_code=2
    --   and sync_status=1   -- 发票更新状态
    --   and cx_invoice_no_code is null 
    group by
      order_code,
      source_bill_no
  ) b on a.order_code = b.order_code

where
     sdt>='20200101'
--   and source_bill_no='OM25050100002822'
  group by company_code,
  customer_code,
  to_date(invoice_time),
  source_bill_no
 )  
  ,
  tmp_sale_detail_di as
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
        -- and business_type_code in('2')
        group by 
        case when business_type_code='6' and substr(split(order_code,'-')[0],1,1)='B' and substr(split(order_code,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(order_code,'-')[0],2,length(split(order_code,'-')[0])-2)
                 when business_type_code='6' and substr(split(order_code,'-')[0],1,1)='B' and substr(split(order_code,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(order_code,'-')[0],2,length(split(order_code,'-')[0])-1)
                 else split(order_code,'-')[0]
                 end,                
        business_type_code,
        business_type_name
),
tmp_order_credit_invoice_bill_settle_detail_di as 
 (
        select 
        case when source_sys='BBC' and substr(split(source_bill_no,'-')[0],1,1)='B' and substr(split(source_bill_no,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(source_bill_no,'-')[0],2,length(split(source_bill_no,'-')[0])-2)
                 when source_sys='BBC' and substr(split(source_bill_no,'-')[0],1,1)='B' and substr(split(source_bill_no,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(source_bill_no,'-')[0],2,length(split(source_bill_no,'-')[0])-1)
                when source_sys='BBC' and bbc_bill_flag=1 and  substr(source_bill_no,1,1) in ('R','S') then regexp_replace(source_bill_no,'^[A-Za-z]+|[A-Za-z]+$','')
              else split(source_bill_no,'-')[0]
                 end as source_bill_no_new,
                bill_code,  -- 对账编号
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
                invoice_amount ,         -- 开票金额
                paid_amt
        from csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di
        where sdt=regexp_replace(add_months(date_sub(current_date,1),0),'-','')
        and date_format(happen_date,'yyyy-MM-dd')>='2020-01-01'
        -- and company_code in('2116')
) ,
tmp_business_info_di as 
( select 
        customer_code,
        business_type_code,
        business_type_name,
        contract_type,
        contract_sign_amount,
        contract_begin_date,
        contract_end_date,
        business_sign_time,
        row_number()over(partition by customer_code,business_type_code order by business_sign_time desc ) rn 
    from  csx_dim.csx_dim_crm_business_info
      where sdt='current'
        and status='1'
        and business_stage = 5
        and business_type_code in (2,6)
),
tmp_invoice_bill_detail  as 
( select a.source_bill_no_new,
    a.bill_code,  -- 对账编号
    a.bill_type,  -- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
    a.sdt,
    a.source_sys,        -- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初
    a.source_bill_no,        -- 来源单号        
    a.customer_code,        -- 客户编码
    a.credit_code,        -- 信控号
    a.happen_date,        -- 发生时间
    a.order_amt,        -- 源单据对账金额
    a.company_code,        -- 签约公司编码
    a.residue_amt,        -- 剩余预付款金额_预付款客户抵消订单金额后
    a.residue_amt_sss,        -- 剩余预付款金额_原销售结算
    a.unpaid_amount,        -- 未回款金额_抵消预付款后
    a.unpaid_amount_sss,        -- 未回款金额_原销售结算
    a.bad_debt_amount,        -- 坏账金额
    a.account_period_code,        -- 账期编码
    a.account_period_name,        -- 账期名称
    a.account_period_value,        -- 账期值
    a.invoice_amount ,         -- 开票金额
    a.paid_amt,
    -- b.paid_date,
    -- c.invoice_date,
    d.business_type_code,
    d.business_type_name
from tmp_order_credit_invoice_bill_settle_detail_di a 
left join tmp_sale_detail_di d on a.source_bill_no_new=d.order_code_new
    where d.business_type_code in('2','6')
)
,
tmp_full_order_detail_di as 
(select a.source_bill_no_new,
    a.bill_code,  -- 对账编号
    a.bill_type,  -- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
    a.sdt,
    a.source_sys,        -- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初
    a.source_bill_no,        -- 来源单号        
    a.customer_code,        -- 客户编码
    a.credit_code,        -- 信控号
    a.happen_date,        -- 发生时间
    a.order_amt,        -- 源单据对账金额
    a.company_code,        -- 签约公司编码
    a.residue_amt,        -- 剩余预付款金额_预付款客户抵消订单金额后
    a.residue_amt_sss,        -- 剩余预付款金额_原销售结算
    a.unpaid_amount,        -- 未回款金额_抵消预付款后
    a.unpaid_amount_sss,        -- 未回款金额_原销售结算
    a.bad_debt_amount,        -- 坏账金额
    a.account_period_code,        -- 账期编码
    a.account_period_name,        -- 账期名称
    a.account_period_value,        -- 账期值
    a.invoice_amount ,         -- 开票金额
    a.paid_amt,
    b.paid_date,
    c.invoice_date,
    a.business_type_code,
    a.business_type_name
from tmp_invoice_bill_detail a
left join tmp_close_bill_account_record_di b on a.source_bill_no=b.close_bill_code
left join tmp_sss_customer_statement_account_di c on a.source_bill_no=c.source_bill_no 
)
select 
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
        c.contract_sign_amount,
        c.contract_begin_date,
        c.contract_end_date,
         paid_amt,
         paid_date,
         rp_invoice_amount,
         invoice_date
from
(        
select
        a.customer_code,
        a.company_code,
        business_type_code,
        business_type_name,
        sum(paid_amt ) paid_amt,
        max(paid_date) paid_date,
        sum(a.invoice_amount ) rp_invoice_amount,
        max(invoice_date) invoice_date
from 
  tmp_full_order_detail_di a 
group by a.customer_code,
a.company_code,
business_type_name,
business_type_code
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
left join 
(select * from 
tmp_business_info_di where rn=1
) c on a.customer_code=c.customer_code and c.business_type_code=a.business_type_code;
;