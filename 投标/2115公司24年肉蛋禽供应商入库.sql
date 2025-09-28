
-- 2313 供应商入库
  with entry_2024 as (select substr(sdt,1,4)year,
    supplier_code,
    supplier_name,
    settle_company_code,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  SUM(receive_qty) receive_qty,
  SUM(receive_amt) receive_amt
from
     csx_analyse.csx_analyse_scm_purchase_order_flow_di
where
  sdt >= '20240101'
  and sdt <= '20241231'
  and settle_company_code='2115'
  and classify_middle_name in('蛋','家禽','猪肉')
  group by supplier_code,
supplier_name,  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,settle_company_code,
  substr(sdt,1,4)
  ),
  entry_2023 as 
  (select substr(sdt,1,4)year,
    supplier_code,
    supplier_name,
    settle_company_code,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  SUM(receive_qty) receive_qty,
  SUM(receive_amt) receive_amt
from
     csx_analyse.csx_analyse_scm_purchase_order_flow_di
where
  sdt >= '20230101'
  and sdt <= '20231231'
  and settle_company_code='2115'
  and classify_middle_name in('蛋','家禽','猪肉')
  group by supplier_code,
supplier_name,  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,settle_company_code,
  substr(sdt,1,4)
  )
  
  select a.supplier_code,
  a.supplier_name,
  a.settle_company_code,
  collect_set(concat_ws('、', a.classify_middle_name)) classify_middle_name,
  SUM(a.receive_qty) receive_qty,
  SUM(a.receive_amt) receive_amt
  from entry_2023 a 
  left join 
    entry_2024 b  on a.supplier_code=b.supplier_code 
    where b.supplier_code is null 
  group by  
  a.supplier_code,
  a.supplier_name,
  a.settle_company_code