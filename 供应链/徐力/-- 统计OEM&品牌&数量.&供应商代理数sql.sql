-- 统计OEM数量
SELECT 
       
    --   classify_large_code,
    --   classify_large_name,
    --   classify_middle_code,
    --   classify_middle_name,
    --   classify_small_code,
    --   classify_small_name,
	   count(goods_code )sku,
	   count(distinct brand_name) brand_qty,
	   count(distinct category_small_code) category_qty
FROM    csx_dim.csx_dim_basic_goods
WHERE sdt='current'
and  csx_purchase_level_code='03'
 
       
       
       ; 
       
           -- 统计OEM数量
SELECT 
       
      classify_large_code,
      classify_large_name,
      classify_middle_code,
      classify_middle_name,
      classify_small_code,
      classify_small_name,
      category_small_code,
      category_small_name,
      goods_code,
      goods_name,
      brand_name
	   csx_purchase_level_code,
	   csx_purchase_level_name
FROM    csx_dim.csx_dim_basic_goods
WHERE sdt='current'
and  csx_purchase_level_code='03'
 
       
       -- 经销供应商数量
select supplier_type_name,count(distinct category_small_code) from 
(select supplier_type_name,category_small_code from   csx_analyse.csx_analyse_scm_purchase_order_flow_di a 
join 
(select
  supplier_code,
  supplier_name,
  case
    when supplier_type = '0' then ''
    when supplier_type = '1' then '代理商'
    when supplier_type = '2' then '生产厂商'
    when supplier_type = '3' then '经销商(资产)'
    when supplier_type = '4' then '集成商(资产)' end as supplier_type_name
    from
      csx_dim.csx_dim_basic_supplier
    where
      sdt = 'current'
      and supplier_type='1'
      ) b on a.supplier_code=b.supplier_code
     where sdt>='20230101' and sdt<='20231231'
     group by supplier_type_name,category_small_code
    )a 
    group by supplier_type_name