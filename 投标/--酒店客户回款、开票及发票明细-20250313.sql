--酒店客户回款、开票及发票明细
-- 自2022.1.1以来，所有酒店类日配客户业绩清单，字段需体现：
-- 采购人、项目名称、供货范围、签约日期、合同起止日期、合同金额、开票金额、回款金额、开票明细（含具体产品品类，如：畜肉类-猪肉、牛肉，蔬菜类-上海青、菠菜。。。等详细到具体产品名称）
-- 以上清单，2115主体的一份，全国彩食鲜的一份


with 
tmp_bill_order as (
select company_code,
     performance_province_name,
     a.source_bill_no_new,
		 a.customer_code,
		 customer_name,
		 a.business_type_code,
		 a.business_type_name,
		 a.sale_amt,
		 b.order_amt, -- 源单据对账金额
		 b.close_bill_amount -- 核销金额
  from 
    (select -- order_code,
		case when business_type_code='6' and substr(split(a.order_code,'-')[0],1,1)='B' and substr(split(a.order_code,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(a.order_code,'-')[0],2,length(split(a.order_code,'-')[0])-2)
			 when business_type_code='6' and substr(split(a.order_code,'-')[0],1,1)='B' and substr(split(a.order_code,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(a.order_code,'-')[0],2,length(split(a.order_code,'-')[0])-1)
			 else split(a.order_code,'-')[0]
			 end as source_bill_no_new,
		performance_province_name,
		customer_code	,
		customer_name,
		business_type_code,
		business_type_name,
		company_code,
 		sum(sale_amt) sale_amt
    from   csx_dws.csx_dws_sale_detail_di a
    where sdt>='20220101'
        and (customer_name like '%酒店%' or second_category_code='330')
        group by -- order_code ,
		case when business_type_code='6' and substr(split(a.order_code,'-')[0],1,1)='B' and substr(split(a.order_code,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(a.order_code,'-')[0],2,length(split(a.order_code,'-')[0])-2)
			 when business_type_code='6' and substr(split(a.order_code,'-')[0],1,1)='B' and substr(split(a.order_code,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(a.order_code,'-')[0],2,length(split(a.order_code,'-')[0])-1)
			 else split(a.order_code,'-')[0]
			 end,			
		customer_name,
		business_type_code,
		business_type_name,
		customer_code,
		performance_province_name,
		company_code
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
      where sdt = '20250312' -- 现在算截止到10月底的回款 下月算截止到11月底的回款
        and regexp_replace(date(happen_date), '-', '') between '20220101' and '20250312'
		group by 
         case when substr(source_bill_no,1,1) ='B' then substr(source_bill_no, 2,length(source_bill_no)-2)  -- 涉及 BBC单号有字母开头的处理
          else split(source_bill_no,'-')[0] end,customer_code,source_sys			  
	)b on b.new_source_bill_no = a.source_bill_no_new and b.customer_code = a.customer_code	
),
-- 发票表
tmp_invoice as (
  select
    performance_province_name,
    sdt invoice_date,
    invoice_no,
    -- 发票号码
    order_code,
    -- 订单编号
    invoice_code,
    -- 发票代码
    company_code,
    -- 公司代码
    a.customer_code,
    -- 客户编码
    b.customer_name	,  
    total_amount,
    -- 总金额
    invoice_customer_name,
    
    -- 客户开发票名称
    if(offline_flag_code = 1, '是', '否') as offline_flag,
    -- 是否线下开票 0 否 1 是
    regexp_replace(invoice_remark, '\\n|\\r|\\t', '') invoice_remark -- 发票的备注
    -- row_number() over(partition by invoice_no order by sdt desc)	as num1
  from
          csx_dwd.csx_dwd_sss_invoice_di a 
 join 
      (select performance_province_name,
            customer_code,
            customer_name,
            first_category_name,
            second_category_code,
            second_category_name
      from csx_dim.csx_dim_crm_customer_info
        where sdt='current'
        and (customer_name like '%酒店%' or second_category_code='330')
    ) b on a.customer_code=b.customer_code
  where
    sdt >= '20220101' -- and invoice_no in('05502566','85686779')
    and delete_flag = '0'
    and sync_status = 1
    
    -- and company_code = '2115'
)
select company_code,performance_province_name,customer_code,customer_name,sum(close_bill_amount) close_bill_amount,sum(invoice_amt) invoice_amt from (
select company_code,performance_province_name,customer_code,customer_name,sum(close_bill_amount) as close_bill_amount,0 invoice_amt  from tmp_bill_order 
group by company_code, customer_code,customer_name,performance_province_name
union all 
select company_code,performance_province_name,customer_code,customer_name,0 close_bill_amount , sum(total_amount) invoice_amt from tmp_invoice
group by customer_code,customer_name,performance_province_name,company_code
) a 
group by company_code,customer_code,customer_name,performance_province_name
;  

-- 发票表
with tmp_invoice as (
  select
    performance_province_name,
    sdt invoice_date,
    invoice_no,
    -- 发票号码
    order_code,
    -- 订单编号
    invoice_code,
    -- 发票代码
    company_code,
    -- 公司代码
    a.customer_code,
    -- 客户编码
    b.customer_name,
    total_amount,
    -- 总金额
    invoice_customer_name,
    -- 客户开发票名称
    if(offline_flag_code = 1, '是', '否') as offline_flag,
    -- 是否线下开票 0 否 1 是
    regexp_replace(invoice_remark, '\\n|\\r|\\t', '') invoice_remark -- 发票的备注
    -- row_number() over(partition by invoice_no order by sdt desc)	as num1
  from
    csx_dwd.csx_dwd_sss_invoice_di a
    join (
      select
        performance_province_name,
        customer_code,
        customer_name,
        first_category_name,
        second_category_code,
        second_category_name
      from
        csx_dim.csx_dim_crm_customer_info
      where
        sdt = 'current'
        and (
          customer_name like '%酒店%'
          or second_category_code = '330'
        )
    ) b on a.customer_code = b.customer_code
  where
    sdt >= '20220101' -- and invoice_no in('05502566','85686779')
    and delete_flag = '0'
    and sync_status = 1 -- and company_code = '2115'
),
tmp_invoice_detail as (
  select
    b.goods_name,
    b.classify_large_name,
    b.classify_middle_name,
    b.classify_small_name,
    order_code,
    source_bill_no,
    bill_code,
    apply_amount
  from
    csx_dwd.csx_dwd_sss_kp_apply_goods_group_detail_di a
    left join (
      select
        goods_code,
        goods_name,
        classify_large_name,
        classify_middle_name,
        classify_small_name
      from
        csx_dim.csx_dim_basic_goods
      where
        sdt = 'current'
    ) b on a.goods_code = b.goods_code
  where
    sdt >= '20220101'
  group by
    b.goods_name,
    b.classify_large_name,
    b.classify_middle_name,
    b.classify_small_name,
    order_code,
    source_bill_no,
    bill_code,
    apply_amount
)
select
  a.company_code,
  a.customer_code,
  a.customer_name,
  a.goods_name,
  classify_large_name,
  classify_middle_name,
  classify_small_name
from
  (
    select
      a.company_code,
      a.customer_code,
      a.customer_name,
      b.goods_name,
      classify_large_name,
      classify_middle_name,
      classify_small_name
    from
      tmp_invoice a
      left join tmp_invoice_detail b on a.order_code = b.order_code
  ) a
group by
  a.company_code,
  a.customer_code,
  a.customer_name,
  a.goods_name,
  classify_large_name,
  classify_middle_name,
  classify_small_name
  ;
  
  
  
  