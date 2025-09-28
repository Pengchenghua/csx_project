-- 部队回款&开票金额涉及到子客户
-- drop table  csx_analyse_tmp.csx_analyse_tmp_close_bill_amt_order;
-- create table csx_analyse_tmp.csx_analyse_tmp_close_bill_amt_order as 
--  
with 
tmp_bill_order as ( select a.source_bill_no_new,
		 sub_customer_code,
		 sub_customer_name,
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
		customer_code	,
		sub_customer_code,
		sub_customer_name,
		business_type_code,
		business_type_name,
 		sum(sale_amt) sale_amt
    from   csx_dws.csx_dws_sale_detail_di a
    where sdt>='20230101'
    and customer_code in ('128509'
,'128511'
,'128512'
,'128531'
,'128534'
,'128559'
,'128573'
,'128548'
,'128565'
,'128454'
,'128533'
,'128575'
,'128362'
,'128453'
,'128496'
,'128517'
,'128520'
,'128536'
,'128489'
,'128560'
,'128524'
,'128515'
,'128363'
,'128521'
)
        group by -- order_code ,
		case when business_type_code='6' and substr(split(a.order_code,'-')[0],1,1)='B' and substr(split(a.order_code,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(a.order_code,'-')[0],2,length(split(a.order_code,'-')[0])-2)
			 when business_type_code='6' and substr(split(a.order_code,'-')[0],1,1)='B' and substr(split(a.order_code,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(a.order_code,'-')[0],2,length(split(a.order_code,'-')[0])-1)
			 else split(a.order_code,'-')[0]
			 end,			
		customer_code	,
		sub_customer_code,
		sub_customer_name,
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
      where sdt = '20250212' -- 现在算截止到10月底的回款 下月算截止到11月底的回款
        and regexp_replace(date(happen_date), '-', '') between '20230101' and '20250213'
       -- and source_bill_no='R2408010410590817A'
        -- and customer_code='130536'
		group by 
         case when substr(source_bill_no,1,1) ='B' then substr(source_bill_no, 2,length(source_bill_no)-2)  -- 涉及 BBC单号有字母开头的处理
          else split(source_bill_no,'-')[0] end,customer_code,source_sys			  
	)b on b.new_source_bill_no = a.source_bill_no_new and b.customer_code = a.customer_code	
),
-- 发票表
tmp_invoice as (
  select
    sdt invoice_date,
    invoice_no,
    -- 发票号码
    order_code,
    -- 订单编号
    invoice_code,
    -- 发票代码
    company_code,
    -- 公司代码
    customer_code,
    -- 客户编码
    sub_customer_name	, -- 子客户名称
    total_amount,
    -- 总金额
    invoice_customer_name,
    
    -- 客户开发票名称
    if(offline_flag_code = 1, '是', '否') as offline_flag,
    -- 是否线下开票 0 否 1 是
    regexp_replace(invoice_remark, '\\n|\\r|\\t', '') invoice_remark -- 发票的备注
    -- row_number() over(partition by invoice_no order by sdt desc)	as num1
  from
    csx_dwd.csx_dwd_sss_invoice_di
  where
    
    sdt >= '20230101' -- and invoice_no in('05502566','85686779')
    and delete_flag = '0'
    and sync_status = 1
    and customer_code in ('128509'
,'128511'
,'128512'
,'128531'
,'128534'
,'128559'
,'128573'
,'128548'
,'128565'
,'128454'
,'128533'
,'128575'
,'128362'
,'128453'
,'128496'
,'128517'
,'128520'
,'128536'
,'128489'
,'128560'
,'128524'
,'128515'
,'128363'
,'128521'
)
    -- and company_code = '2115'
)
select customer_code,sub_customer_name,sum(close_bill_amount) close_bill_amount,sum(invoice_amt) invoice_amt from (
select customer_code,sub_customer_name,sum(close_bill_amount) as close_bill_amount,0 invoice_amt  from tmp_bill_order 
group by customer_code,sub_customer_name
union all 
select customer_code,sub_customer_name,0 close_bill_amount , sum(total_amount) invoice_amt from tmp_invoice
group by customer_code,sub_customer_name
) a 
group by customer_code,sub_customer_name
;  
  