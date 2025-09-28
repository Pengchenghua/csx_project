-- 查找品类入库的供应商
/*
 蛋、水产、家禽、猪肉、干货、调味品
 B01	干货加工	B0101	干货
 B01	干货加工	B0103	蛋
 B03	肉禽水产	B0301	家禽
 B03	肉禽水产	B0302	猪肉
 B03	肉禽水产	B0303	水产
 B06	调味杂货	B0601	面类/米粉类  剔除
 B06	调味杂货	B0602	调味品类
 */
select 
  supplier_code,
  -- 供应商编码
  supplier_name,
  -- 供应商名称
  b.classify_large_code,
  b.classify_large_name,
  b.classify_middle_code,
  b.classify_middle_name,
  receive_dc_code as location_code,
  --regexp_replace(to_date(receive_time), '-', '') as receive_date,
  sum(receive_qty) as receive_qty,
  sum(receive_amt) / sum(receive_qty) as receive_price,
  sum(receive_amt) as receive_amt
from csx_dws.csx_dws_wms_entry_detail_di a 
 join 
(select goods_code,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name
 from csx_dim.csx_dim_basic_goods 
  where sdt='current'
    and classify_middle_code in ('B0101',
'B0103',
'B0301',
'B0302',
'B0303',
'B0601',
'B0602'))b on a.goods_code=b.goods_code
join 
(select shop_code,company_code FROM csx_dim.csx_dim_shop 
where sdt='current'
  and company_code='2115') c on sett=c.shop_code
-- from csx_analyse.csx_analyse_scm_purchase_order_flow_di
where sdt >= '20230101'
  and sdt <= '20240630' -- and return_flag <> 'Y'   -- 不含退货
  and receive_status <> 0 -- 收货状态 0-待收货 1-收货中 2-已关单
  and entry_type like 'P%' -- 订单类型
  and receive_qty > 0
  and purpose <> '09' -- 不含城市服务商
GROUP BY supplier_code,
  -- 供应商编码
  supplier_name,
  b.classify_large_code,
  b.classify_large_name,
  b.classify_middle_code,
  b.classify_middle_name,
  receive_dc_code

  ;

-- 根据结算DC入库 

select 
  supplier_code,
  -- 供应商编码
  supplier_name,
  -- 供应商名称
  b.classify_large_code,
  b.classify_large_name,
  b.classify_middle_code,
  b.classify_middle_name,
  settlement_dc_code as location_code,
  shop_name,
  --regexp_replace(to_date(receive_time), '-', '') as receive_date,
  sum(receive_qty) as receive_qty,
  sum(receive_amt) / sum(receive_qty) as receive_price,
  sum(receive_amt) as receive_amt
from   csx_dws.csx_dws_wms_entry_detail_di a 
 join 
(select goods_code,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name
 from csx_dim.csx_dim_basic_goods 
  where sdt='current'
    and classify_middle_code in (
'B0301',
'B0302',
'B0303'))b on a.goods_code=b.goods_code
join 
(select shop_code,shop_name,company_code FROM csx_dim.csx_dim_shop 
where sdt='current'
  and company_code='2304') c on settlement_dc_code=c.shop_code
-- from csx_analyse.csx_analyse_scm_purchase_order_flow_di
where sdt >= '20240101'
  and sdt <= '20240630' -- and return_flag <> 'Y'   -- 不含退货
  and receive_status <> 0 -- 收货状态 0-待收货 1-收货中 2-已关单
  and entry_type like 'P%' -- 订单类型
  and receive_qty > 0
  and purpose <> '09' -- 不含城市服务商
GROUP BY supplier_code,
  -- 供应商编码
  supplier_name,
  b.classify_large_code,
  b.classify_large_name,
  b.classify_middle_code,
  b.classify_middle_name,
  settlement_dc_code,
  shop_name

  ;


  select months,
    check_ticket_order_code,
    a.supplier_code,
    b.supplier_name,
    invoice_amount,
    set_middle_name
from 
(select  check_ticket_order_code,    -- 勾票单号
    supplier_code,
    company_code,
    to_date(check_ticket_date) as check_date,         -- 勾票日期
    substr(regexp_replace(to_date(check_ticket_date),'-',''),1,6) months,
    sum(invoice_amount) invoice_amount
from    csx_dwd.csx_dwd_pss_statement_check_ticket_di
where sdt>= '20230101'
    and sdt<='20240630'
    and company_code='2115'
    
group by      -- 勾票单号
    supplier_code,
    company_code,
    check_ticket_order_code,
    to_date(check_ticket_date) ,         -- 勾票日期
    substr(regexp_replace(to_date(check_ticket_date),'-',''),1,6)
    having sum(invoice_amount)>=50000
)a 
join 
(
select 
supplier_code, 	 -- 供应商编号
supplier_name, 	 -- 供应商名称
concat_ws('|', collect_set(classify_middle_name)) as set_middle_name
from csx_analyse_tmp.csx_analyse_tmp_entry_01
group by supplier_code, 	 -- 供应商编号
supplier_name
) b on a.supplier_code=b.supplier_code



  
-- 发票表
select
  invoice_no,
  check_ticket_order_code,
  supplier_code,
  company_code,
  price,
  tax_amt,
  value_tax_total as invoice_amt,
  row_number() over (partition by invoice_no order by audit_status_code desc) as rn
--  count(check_ticket_order_code) as cn
from
  csx_dwd.csx_dwd_pss_statement_invoice
where
  sdt>='20240601'
  and invoice_no='24117000000251677577'
 ;




-- 发票表
with pss as (
select * from 
(
select
  invoice_no,
  check_ticket_order_code,
  supplier_code,
  company_code,
  price,
  tax_amt,
  value_tax_total as invoice_amt,
  row_number() over (partition by invoice_no order by update_time desc) as rn
--  count(check_ticket_order_code) as cn
from
  csx_dwd.csx_dwd_pss_statement_invoice
where
  sdt>='20230101'
  and sdt<='20240630'
  and company_code='2115'
  and value_tax_total>=50000
  ) a where rn>1
  ) 
  select invoice_no,a.supplier_code,b.supplier_name,invoice_amt,set_middle_name
  from pss a
  join 
  (
select 
supplier_code, 	 -- 供应商编号
supplier_name, 	 -- 供应商名称
concat_ws('|', collect_set(classify_middle_name)) as set_middle_name
from csx_analyse_tmp.csx_analyse_tmp_entry_01
group by supplier_code, 	 -- 供应商编号
supplier_name
) b on a.supplier_code=b.supplier_code

--

select
  invoice_no,
  check_ticket_order_code,
  a.supplier_code,
  b.supplier_name,
  company_code,
  price,
  tax_amt,
  value_tax_total as invoice_amt,
  row_number() over (partition by invoice_no order by update_time desc) as rn
--  count(check_ticket_order_code) as cn
from
    csx_dwd.csx_dwd_pss_statement_invoice a 
  join 
  (select supplier_code,supplier_name
  from csx_dim.csx_dim_basic_supplier 
    where sdt='current'
    and  supplier_name in ('大庄园肉业集团股份有限公司',
                        '南京雨润生鲜食品有限公司',
                        '北京二商大红门五肉联食品有限公司',
                        '北京二商穆香源清真肉类食品有限公司')
    )b on a.supplier_code=b.supplier_code
where
  sdt>='20240101'
  and sdt<='20240630'
 -- and company_code='2304'
 
  ;


-- 采购订单
  select source_bill_no,  -- 采购订单
    link_in_out_order_code,          -- 关联单号
    company_code,
    company_name,
    a.purchase_org_code,
    a.purchase_org_name,
    happen_dc_code,
    settle_location_code,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    to_date(business_date) business_date
from csx_dwd.csx_dwd_pss_settle_inout_detail_di a  -- 采购订单
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
    to_date(business_date)
    ;

-- 勾票
    select purchase_order_code,  -- 采购单号
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
from csx_dwd.csx_dwd_pss_statement_source_bill_di 
where sdt>= '${sdate}'
    and sdt<='${enddate}'