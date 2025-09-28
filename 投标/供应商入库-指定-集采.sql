select substr(sdt,1,4)year,
    supplier_code,
    supplier_name,
    settle_company_code,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  SUM(receive_qty),
  SUM(receive_amt)
from
   csx_analyse.csx_analyse_scm_purchase_order_flow_di
where
  sdt >= '20210101'
  and sdt <= '20231224'
--   and supplier_code in( '20066343',
-- '20066027',
-- '20065908',
-- '20065831',
-- '20065807',
-- '20065391',
-- '20016828',
-- '20064645',
-- '20064359',
-- '20064199',
-- '20063885',
-- '20063860',
-- '20063761',
-- '20063660',
-- '20063430',
-- '20063323',
-- '20063232',
-- '20063113',
-- '20063172',
-- '20063048')

  AND is_central_tag='1'
  and classify_middle_name='蛋'
  group by supplier_code,
supplier_name,  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,settle_company_code,
  substr(sdt,1,4)

-- 2313 供应商入库
  with entry as (select substr(sdt,1,4)year,
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
  and sdt <= '20240815'
  and settle_company_code='2313'
  group by supplier_code,
supplier_name,  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,settle_company_code,
  substr(sdt,1,4)
  )
  
  select supplier_code,
  supplier_name,
  settle_company_code,
  collect_set(concat_ws('、', classify_middle_name)) classify_middle_name,
  SUM(receive_qty) receive_qty,
  SUM(receive_amt) receive_amt
  from entry
  group by  supplier_code,
  supplier_name,
  settle_company_code