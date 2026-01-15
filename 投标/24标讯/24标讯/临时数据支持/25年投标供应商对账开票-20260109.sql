-- 投标供应商对账开票
with tmp_puchar_order as (
  select
    source_bill_no,
    -- 采购订单
    company_code,
    company_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_name,
    sum(total_amount) goods_amount
  from
    csx_dwd.csx_dwd_pss_settle_inout_detail_di a -- 采购订单
    left join (
      SELECT
        goods_code,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name
      FROM
        csx_dim.csx_dim_basic_goods -- 商品资料表
      WHERE
        sdt = 'current'
    ) b on a.product_code = b.goods_code
  where
    a.source_order_type_code = '1'
  group by
    source_bill_no,
    -- 采购订单
    company_code,
    company_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_name
),
tmp_statement_source_bill as ( 
  select
    company_code,
    purchase_order_code,
    -- 采购单号
    bill_code,
    -- 对帐单号
    supplier_code,
    supplier_name,
    regexp_replace(substr(to_date(bill_time), 1, 7), '-', '') as smonth,
    -- 对帐日期
    sum(bill_amt) bill_amt
  from
    csx_dwd.csx_dwd_pss_statement_source_bill_di
  where
    bill_type = 1
    and smt >= '202501'
    and smt <= '202510'
    and to_date(bill_time)>='2025-01-14' and to_date(bill_time)<='2025-10-14'
    and company_code = '2115'
  group by
    company_code,
    purchase_order_code,
    -- 采购单号
    bill_code,
    supplier_code,
    supplier_name,
    regexp_replace(substr(to_date(bill_time), 1, 7), '-', '')
) -- 对账票
select
  a.company_code,
  a.bill_code,
  a.supplier_code,
  a.supplier_name,
  a.smonth,
  a.classify_large_name,
  a.classify_middle_name,
  totail_bill_amt,
  sum(goods_amount) goods_amount
from
  (
    select
      a.company_code,
      a.purchase_order_code,
      -- 采购单号
      a.bill_code,
      -- 对帐单号
      a.supplier_code,
      a.supplier_name,
      a.smonth,
      b.classify_large_code,
      b.classify_large_name,
      b.classify_middle_code,
      b.classify_middle_name,
      max(a.bill_amt) bill_amt,
      sum(b.goods_amount) goods_amount
    from
      tmp_statement_source_bill a  -- 修正：使用新的CTE名称
      left join tmp_puchar_order b on a.purchase_order_code = b.source_bill_no
      where
       ((classify_large_name ='肉禽水产' and classify_middle_name !='水产') or classify_small_name='冷冻食品')
    group by
      a.company_code,
      a.purchase_order_code,
      -- 采购单号
      a.bill_code,
      -- 对帐单号
      a.supplier_code,
      a.supplier_name,
      a.smonth,
      b.classify_large_code,
      b.classify_large_name,
      b.classify_middle_code,
      b.classify_middle_name
  ) a
  left join 
  (select bill_code,sum(bill_amt) totail_bill_amt from  tmp_statement_source_bill
  group by bill_code) b on a.bill_code=b.bill_code
 group by  a.company_code,
  a.bill_code,
  a.supplier_code,
  a.supplier_name,
  a.smonth,
  a.classify_large_name,
  a.classify_middle_name,
  totail_bill_amt
 