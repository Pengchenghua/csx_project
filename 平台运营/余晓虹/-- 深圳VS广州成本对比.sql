-- 深圳VS广州成本对比
with tmp_receive as (
select  
  a.performance_province_name,
  a.dc_code,
  a.classify_large_code ,
  a.classify_large_name ,
  a.classify_middle_code, 
  a.classify_middle_name, 
  a.classify_small_code ,
  a.classify_small_name ,
  a.goods_code ,
  a.goods_name ,
  a.unit_name ,
  goods_status_name ,
  a.sdt,
  agreement_order_no,
  link_order_code,
  sum(receive_qty) as receive_qty,
  sum(receive_amt) as receive_amt
from      csx_report.csx_report_wms_order_flow_di a 
left  join (select goods_code,goods_status_name,dc_code from csx_dim.csx_dim_basic_dc_goods 
 where sdt='current'
 ) b on a.goods_code=b.goods_code and a.dc_code=b.dc_code
 LEFT JOIN 
(select order_code,goods_code from   csx_dws.csx_dws_scm_order_detail_di
where sdt>='20250101'
and price_type=2    -- 剔除售价下浮
group by order_code,goods_code) c on a.goods_code=c.goods_code and a.purchase_order_code=c.ORDER_CODE
where sdt>='20250801' and sdt<='20250914'
and c.goods_code is null        -- 剔除售价下浮
and performance_province_name in ('广东深圳','广东广州')
and purpose_name in ('大客户物流','工厂')
and super_class_code in (1,3)
and source_type_code in (1,8,9,10,23,19) 
and remedy_flag!='1'
group by a.performance_province_name,
  a.dc_code,
  a.classify_large_code ,
  a.classify_large_name ,
  a.classify_middle_code, 
  a.classify_middle_name, 
  a.classify_small_code ,
  a.classify_small_name ,
  a.goods_code ,
  a.goods_name ,
  a.unit_name ,
  goods_status_name ,
  sdt,
  agreement_order_no,
  link_order_code
  ),
  tmp_receive_01 as (select   a.classify_large_code ,
  a.classify_large_name ,
  a.classify_middle_code, 
  a.classify_middle_name, 
  a.classify_small_code ,
  a.classify_small_name ,
  a.goods_code ,
  a.goods_name ,
  a.unit_name ,
  goods_status_name ,
  a.sdt,
  sum(if(performance_province_name='广东深圳' ,receive_qty,0)) as sz_receive_qty,
  sum(if(performance_province_name='广东深圳' ,receive_amt,0))/sum(if(performance_province_name='广东深圳' ,receive_qty,0)) as sz_avg_cost,
  sum(if(performance_province_name='广东深圳' ,receive_amt,0)) as sz_receive_amt,
  sum(if(performance_province_name='广东广州' ,receive_qty,0)) as gz_receive_qty,
  sum(if(performance_province_name='广东广州' ,receive_amt,0))/sum(if(performance_province_name='广东广州' ,receive_qty,0)) as gz_avg_cost,
  sum(if(performance_province_name='广东广州' ,receive_amt,0)) as gz_receive_amt
 from tmp_receive a 
  group by   a.classify_large_code ,
  a.classify_large_name ,
  a.classify_middle_code, 
  a.classify_middle_name, 
  a.classify_small_code ,
  a.classify_small_name ,
  a.goods_code ,
  a.goods_name ,
  a.unit_name ,
  goods_status_name ,
  a.sdt
  ) select   a.classify_large_code ,
  a.classify_large_name ,
  a.classify_middle_code, 
  a.classify_middle_name, 
  a.classify_small_code ,
  a.classify_small_name ,
  a.goods_code ,
  a.goods_name ,
  a.unit_name ,
  goods_status_name ,
  a.sdt,
  sz_receive_qty,
  sz_avg_cost,
  sz_receive_amt,
  gz_receive_qty,
  gz_avg_cost,
  gz_receive_amt,
  case
    when gz_avg_cost > 0 and sz_avg_cost > 0 
    then least(coalesce(gz_avg_cost,0) ,coalesce(sz_avg_cost,0) )  -- 都大于0时取最小值
    else greatest(coalesce(gz_avg_cost,0), coalesce(sz_avg_cost,0))  -- 任一≤0时取最大值
  end as  min_avg_cost,  -- 新增列：取两个平均值中的最小值
  case when  least(coalesce(gz_avg_cost,0) ,coalesce(sz_avg_cost,0) )=0 then ''
        when  least(coalesce(gz_avg_cost,0) ,coalesce(sz_avg_cost,0) )=coalesce(gz_avg_cost,0) then '广东广州'
        else '广东深圳' end min_avg_flag
 from tmp_receive_01 a 
  


  ;

-- 明细


select  
 a.purchase_order_code as `采购订单号`,
 a.order_code as `入库/出库单号`,
 original_purchase_order_code		`原采购单号`,
 super_class_name as `单据类型名称`, 
  source_type_name as `来源采购订单名称`, 
  a.goods_code as `商品编码`, 
  bar_code as `商品条码`,
  goods_name as `商品名称`, 
  unit_name as `单位`, 
  brand_name as `品牌`, 
  spec as `规格`,
  purchase_group_code as `课组编码`, 
  purchase_group_name as `课组名称`, 
  classify_large_code as `管理一级编码`, 
  classify_large_name as `管理一级名称`, 
  classify_middle_code as `管理二级编码`, 
  classify_middle_name as `管理二级名称`, 
  classify_small_code as `管理三级编码`, 
  classify_small_name as `管理三级名称`, 
  category_large_code as `大类编码`, 
  category_large_name as `大类名称`,
  category_middle_code	as	`中类编码`,
  category_middle_name	as	`中类名称`,
  category_small_code	as	`小类编码`,
  category_small_name	as	`小类名称`,
  order_qty as `订单数量`, 
  order_price1 as `单价1`, 
  order_price1*order_qty as`订单金额`, 
  receive_qty as `入库数量`, 
  receive_amt as `入库金额`, 
  no_tax_receive_amt as `入库不含税金额`, 
  shipped_qty as `出库数量`, 
  shipped_amt as `出库金额`, 
  a.supplier_code as `供应商编码`, 
  supplier_name as `供应商名称`, 
  if(c.goods_code is not null ,'是','否') `是否委外`,
  a.dc_code as `DC编码`,  
  dc_name as `DC名称`, 
  local_purchase_flag as `是否地采`,
  items_close_time as `商品关单日期`,
  order_create_date as `订单日期`,
  receive_sdt as `收货关单日期`
from  csx_report.csx_report_wms_order_flow_di  a 
 join 
 (select goods_code,goods_status_name,dc_code,spec from   csx_dim.csx_dim_basic_dc_goods 
 where sdt='current'
 ) b on a.goods_code=b.goods_code and a.dc_code=b.dc_code
 LEFT JOIN 
(select order_code,goods_code from   csx_dws.csx_dws_scm_order_detail_di
where sdt>='20250101'
and price_type=2
group by order_code,goods_code) c on a.goods_code=c.goods_code and a.purchase_order_code=c.ORDER_CODE
where sdt>='20250801' and sdt<='20250911'
and performance_province_name in ('广东深圳','广东广州')
and purpose_name in ('大客户物流','工厂')
and super_class_code in (1,3)
and source_type_code in (1,8,9,10,23,19) 
and remedy_flag!='1'
