-- 查看开票与发票录入日期

drop table csx_analyse_tmp.csx_analyse_tmp_temp_pss_01;
create   table csx_analyse_tmp.csx_analyse_tmp_temp_pss_01 as 
select a.purchase_order_code,           -- 采购单号
       a.business_order_code,               -- -- 1 入库单(批次单号) 2、结算单
       a.check_ticket_order_code,       -- -- 勾票单号
       a.bill_code,      -- 对帐单号
       a.payment_order_code,        -- 实际付款单号
       a.statement_date,        -- 对帐日期
       a.finance_statement_date,    -- 财务对帐日期
       a.pay_create_date,   -- -付款生成日期
       a.paid_date,         -- 付款日期
       b.sign_date,         -- 供应商签单日期
       c.audit_date,        -- 票核日期
       c.invoice_sub_date,  -- 发票录入日期
       payment_date,      -- 付款日期
       review_date,          -- 审核时间 
       payment_status_code,
       check_ticket_status_code
from 
(select purchase_order_code,  -- 采购单号
    business_order_code,   -- 1 入库单(批次单号) 2、结算单
    payment_order_code,     -- 实际付款单号
    check_ticket_order_code,    -- 勾票单号
    happen_date , 				-- 发生日期（入库单日期）结算单(归属日期)
    bill_code, 				 -- 对帐单号
    payment_status_code,     -- 付款状态 0-未生成 1 已生成未审核 2 已生成已审核  3已发起付款 4 已付款成功
   to_date(bill_time) as  statement_date, -- 对帐日期
   to_date(finance_bill_time) as  finance_statement_date, -- 财务对帐日期
   to_date(pay_create_time) as  pay_create_date,        -- 付款生成日期
   to_date(paid_time) paid_date   ,            -- 付款日期
   check_ticket_status_code
from   csx_dwd.csx_dwd_pss_statement_source_bill_di 
where sdt>= '${sdate}'
    and sdt<='${enddate}'
) a 
left join
-- 对帐单
(select bill_code,   -- 对帐单号
        check_ticket_order_code, -- 勾票单号
        to_date(sign_date) as sign_date,     --  供应商签单日期
        to_date(audit_time) as audit_date  -- 票核日期
from csx_dwd.csx_dwd_pss_statement_statement_account_di  
where sdt>= '${sdate}'
    and sdt<='${enddate}'
group by
        bill_code,   -- 对帐单号
        check_ticket_order_code, -- 勾票单号
        to_date(sign_date) ,     --  供应商签单日期
        to_date(audit_time)
) b on a.bill_code=b.bill_code
left join
-- 勾票表
(
select check_ticket_order_code,     -- 勾票单号
        to_date(check_ticket_date) as check_date,         -- 勾票日期
        to_date(invoice_submit_date)as invoice_sub_date,   -- 发票录入日期
        to_date(audit_time) as audit_date          -- 票核日期
from csx_dwd.csx_dwd_pss_statement_check_ticket_di
where sdt>= '${sdate}'
    and sdt<='${enddate}'
group by check_ticket_order_code,     -- 勾票单号
        to_date(check_ticket_date) ,         -- 勾票日期
        to_date(invoice_submit_date),   -- 发票录入日期
        to_date(audit_time) 
) c on a.check_ticket_order_code=c.check_ticket_order_code 
left join 

-- 付款表
(
select payment_order_code,      -- 付款单号
    to_date(payment_date ) as payment_date,       -- 付款时间
    to_date(audit_time) as review_date         -- 审核时间 
from csx_dwd.csx_dwd_pss_statement_payment_di
where sdt>= '${sdate}'
    and sdt<='${enddate}'
group by 
 payment_order_code,      -- 付款单号
    to_date(payment_date ) ,       -- 付款时间
    to_date(audit_time)            
) d on a.payment_order_code=d.payment_order_code

;


--  采购订单 创建日期 入库日期 、关单日期  管理分类
 
with entry as (
select source_bill_no,  -- 采购订单
order_code,
    link_in_out_order_code,          -- 批次单号
    company_code,
    company_name,
    a.purchase_org_code,
    a.purchase_org_name,
    happen_dc_code,
    settle_location_code,
    a.classify_large_code,
    a.classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    supplier_code,
    business_date,
    total_amount,
    receive_amt

from 
(select source_bill_no,  -- 采购订单
    link_in_out_order_code,          -- 关联单号
    company_code,
    company_name,
    a.purchase_org_code,
    a.purchase_org_name,
    happen_dc_code,
    supplier_code,
    settle_location_code,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    to_date(business_date) business_date,
    sum(total_amount) total_amount
from    csx_dwd.csx_dwd_pss_settle_inout_detail_di a  -- 采购订单
left join 
(SELECT goods_code,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name
FROM csx_dim.csx_dim_basic_goods    -- 商品资料表
WHERE sdt='current') b on a.product_code=b.goods_code
where  sdt>= '${sdate}'
    and sdt<='${edate}'
    and a.source_order_type_code = 1
group by 
     source_bill_no,  -- 采购订单
    link_in_out_order_code,          -- 批次单号
    company_code,
    company_name,
    happen_dc_code,
    settle_location_code,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    a.purchase_org_code,
    a.purchase_org_name,
    supplier_code,
    to_date(business_date)
) a 
left join
--  入库批次表
(select 
    order_code,
    to_date(receive_time) as receive_date	,
    to_date(close_time) receive_close_date,
    to_date(sign_date ) post_date,
    sum(receive_amt) receive_amt,
    classify_small_code,
    original_order_code
    from    csx_dws.csx_dws_wms_entry_batch_di 
  --   where batch_code='TK190925001725'
  where sdt>='${ysdate}'
 group by  to_date(receive_time) 	,
    to_date(close_time),
    order_code,
    original_order_code,
    to_date(sign_date ),
    classify_small_code
    ) as c on a.source_bill_no=c.original_order_code and a.classify_small_code=c.classify_small_code
)
select coalesce(order_code,source_bill_no,'') as purchase_no,
-- link_in_out_order_code as batch_co,          --  批次单号
    company_code,
    company_name,
    a.purchase_org_code,
    a.purchase_org_name,
    happen_dc_code as receive_dc_id,
    -- d.shop_name as receive_dc_name ,
    settle_location_code as settle_dc_id,
    -- f.shop_name as settle_dc_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    a.supplier_code,
    supplier_name,
    total_amount,
    receive_amt,
    business_date,
    coalesce(statement_date,'')as statement_date,        --  对帐日期
   coalesce(finance_statement_date,'') as finance_statement_date,     --  财务对帐日期
   coalesce(pay_create_date,'')as pay_create_date,   -- - 付款生成日期
   coalesce(payment_date,'')as payment_date,         --  付款日期
   coalesce(sign_date,'')as sign_date,                  --  供应商签单日期
   coalesce(audit_date,       '')as audit_date,        --  票核日期
   coalesce(invoice_sub_date, '')as invoice_sub_date,  --  发票录入日期
   coalesce(review_date,      '')as review_date        --  付款审核日期
from entry a 
left join 
csx_analyse_tmp.csx_analyse_tmp_temp_pss_01 b on source_bill_no=purchase_order_code and a.link_in_out_order_code=b.business_order_code
left join
(select a.supplier_code supplier_code,
    a.supplier_name supplier_name,
    reconciliation_code	 as reconciliation_tag,
    b.dic_value  as reconciliation_tag_name,
    account_group as account_group,
    c.dic_value as account_group_name
 from csx_dim.csx_dim_basic_supplier a 
 left join
 (select dic_type,dic_key,dic_value from csx_dim.csx_dim_csx_basic_data_md_dic   where sdt='current' and dic_type='CONCILIATIONNFLAG' ) b on a.reconciliation_code	=b.dic_key 
 left join 
 (select dic_type,dic_key,dic_value from csx_dim.csx_dim_csx_basic_data_md_dic   where sdt='current' and dic_type='VENDERAGROUP' ) c on a.account_group=c.dic_key 
  where sdt='current'
  ) c on a.supplier_code=c.supplier_code
  where a.company_code='2115'
  and a.classify_small_name like '%冻%'
  and a.classify_large_name ='肉禽水产'
and receive_amt>0
;



--  如果付款日期为空，则按当前日期进行计算（搜索下载日或T-1日）；如果被减日期也为空，则计算结果为空
drop table if exists csx_analyse_tmp.temp_pss_02;
create temporary table csx_analyse_tmp.temp_pss_02 as 
select 
    coalesce(order_code ,source_bill_no) as purchase_no,
    entry_order_no,					--  入库单号
    link_in_out_order_code as batch_co,          --  批次单号
    company_code,
    company_name,
    a.purchase_org_code,
    a.purchase_org_name,
    happen_dc_code as receive_dc_id,
    d.shop_name as receive_dc_name ,
    settle_location_code as settle_dc_id,
    f.shop_name as settle_dc_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    order_create_date,
    a.supplier_code,
    supplier_name,
    reconciliation_tag,
    reconciliation_tag_name,
    account_group,                                      -- 帐户组
    account_group_name,                                 -- 公司帐户组名称
    pay_condition_code,                                      -- 付款条件
    pay_condition_name,                                 -- 付款条件名称
   coalesce(receive_date,'') as receive_date	,       -- 收货日期
   coalesce(receive_close_date,'') as receive_close_date,     -- 关单日期
   coalesce(post_date,      '') as post_date,              -- 过帐日期
   coalesce(check_ticket_order_code,'') as check_ticket_order_code,       -- 勾票单号
   coalesce(bill_code,  '')as bill_code,      --  对帐单号
   coalesce(payment_order_code,    '')as payment_order_code,        --  实际付款单号
   coalesce(statement_date,'')as statement_date,        --  对帐日期
   coalesce(finance_statement_date,'') as finance_statement_date,     --  财务对帐日期
   coalesce(pay_create_date,'')as pay_create_date,   -- - 付款生成日期
   coalesce(payment_date,'')as payment_date,         --  付款日期
   coalesce(sign_date,'')as sign_date,                  --  供应商签单日期
   coalesce(audit_date,       '')as audit_date,        --  票核日期
   coalesce(invoice_sub_date, '')as invoice_sub_date,  --  发票录入日期
   coalesce(review_date,      '')as review_date,        --  付款审核日期
   case when coalesce(finance_statement_date,'')='' then '' 
        when coalesce(payment_date,'')='' then datediff(date_sub(current_date() ,1),coalesce(finance_statement_date,''))
        else coalesce(datediff(coalesce(payment_date,''),coalesce(finance_statement_date,'')),'') end
	as finance_days,  -- 财务对帐天数
    case when coalesce(invoice_sub_date,'')='' then '' 
        when coalesce(payment_date,'')='' then datediff(date_sub(current_date() ,1),coalesce(invoice_sub_date,''))
        else coalesce(datediff(coalesce(payment_date,''),coalesce(invoice_sub_date,'')),'') end as invoice_sub_days,     -- 发票录入天数
    case when coalesce(audit_date,'')='' then '' 
        when coalesce(payment_date,'')='' then datediff(date_sub(current_date() ,1),coalesce(audit_date,''))
        else coalesce(datediff(coalesce(payment_date,''),coalesce(audit_date,'')),'') end as audit_days,                 -- 票核天数
    case when coalesce(pay_create_date,'')='' then '' 
        when coalesce(payment_date,'')='' then datediff(date_sub(current_date() ,1),coalesce(pay_create_date,''))
        else coalesce(datediff(coalesce(payment_date,''),coalesce(pay_create_date,'')),'') end as pay_create_days,       -- 付款生成天数
    case when coalesce(review_date,'')='' then '' 
        when coalesce(payment_date,'')='' then datediff(date_sub(current_date() ,1),coalesce(review_date,''))
        else coalesce(datediff(coalesce(payment_date,''),coalesce(review_date,'')),'')end as review_days,               -- 付款审核天数
    payment_status_code,          		--   单据状态
    case when payment_status_code='0' then  '单据未生成'
        when   payment_status_code='1' then '单据已生成未审核' 
        when  payment_status_code='2' then  '单据已生成已审核'
        when  payment_status_code='3' then  '单据已发起付款'
        when  payment_status_code='4' then  '单据已付款成功'
        else '' end   payment_status_name ,  --  付款状态 0-未生成 1 已生成未审核 2 已生成已审核  3已发起付款 4 已付款成功
        check_ticket_status_code,
        business_date
from csx_analyse_tmp.temp_pss_00  a 
left join 
csx_analyse_tmp.csx_analyse_tmp_temp_pss_01 b on order_code=purchase_order_code and a.link_in_out_order_code=b.business_order_code
left join
(select a.supplier_code supplier_code,
    a.supplier_name supplier_name,
    reconciliation_code	 as reconciliation_tag,
    b.dic_value  as reconciliation_tag_name,
    account_group as account_group,
    c.dic_value as account_group_name
 from csx_dim.csx_dim_basic_supplier a 
 left join
 (select dic_type,dic_key,dic_value from csx_dim.csx_dim_csx_basic_data_md_dic   where sdt='current' and dic_type='CONCILIATIONNFLAG' ) b on a.reconciliation_code	=b.dic_key 
 left join 
 (select dic_type,dic_key,dic_value from csx_dim.csx_dim_csx_basic_data_md_dic   where sdt='current' and dic_type='VENDERAGROUP' ) c on a.account_group=c.dic_key 
  where sdt='current'
  ) c on a.supplier_code=c.supplier_code
  left join 
  (select distinct purchase_org_code, supplier_code , pay_condition_code,dic_value as pay_condition_name  
  from csx_dim.csx_dim_basic_supplier_purchase a 
  left join 
  (select dic_type,dic_key,dic_value from csx_dim.csx_dim_csx_basic_data_md_dic   where sdt='current'
    and dic_type='ACCOUNTCYCLE' ) b on a.pay_condition_code = b.dic_key
    where sdt='current' ) m ON a.purchase_org_code=m.purchase_org_code and a.supplier_code=m.supplier_code
 left join 
 (select shop_code,shop_name from csx_dim.csx_dim_shop where sdt='current') d on a.happen_dc_code=d.shop_code
  left join 
 (select shop_code,shop_name from csx_dim.csx_dim_shop where sdt='current') f on a.settle_location_code=f.shop_code
 
;
