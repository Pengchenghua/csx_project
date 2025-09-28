-- 货源供应商关系
select a.location_code,
location_name,
product_code,
product_name,
a.supplier_code,
a.supplier_name,
unit_name,
classify_large_code,
classify_large_name,
classify_middle_code,
classify_middle_name,
classify_small_code,
classify_small_name,
b.receive_amt,
receive_qty from 
(SELECT location_code,
location_name,
product_code,
product_name,
supplier_code,
supplier_name,
daily_purchase_flag , -- 是否日采 
daily_purchase_master_flag,   -- 是否日采补货主供应商（
direct_master_flag, -- 是否直送主供应商 
status
FROM    csx_dwd.csx_dwd_scm_product_source_supplier_di 
WHERE 
-- SDT>='20240308' 
 location_code='WC09'
  and status=1
   group by  location_code,
location_name,
product_code,
product_name,
supplier_code,
supplier_name,
daily_purchase_flag , -- 是否日采 
daily_purchase_master_flag,   -- 是否日采补货主供应商（
direct_master_flag, -- 是否直送主供应商 
status 
    ) a 
left join 
(select dc_code,goods_code,supplier_code,sum(receive_amt) receive_amt,sum(receive_qty) receive_qty  from  csx_analyse.csx_analyse_scm_purchase_order_flow_di where sdt>='20230101' 
group by dc_code,goods_code,supplier_code ) b on a.location_code=b.dc_code and a.product_code=b.goods_code and a.supplier_code=b.supplier_code
left join 
(select goods_code,	goods_name,
unit_name,
business_division_code,
business_division_name,
classify_large_code,
classify_large_name,
classify_middle_code,
classify_middle_name,
classify_small_code,
classify_small_name
from csx_dim.csx_dim_basic_goods where sdt='current'
 and goods_code='1735141') c on a.product_code=c.goods_code




 -- 商品池
 select * from desc  csx_analyse.csx_analyse_report_yszx_dc_product_pool_df  where sdt='20240308' and inventory_dc_code='WC09'