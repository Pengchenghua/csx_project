-- 1、根据销售凭证查找转码批次号-销售批次号，
-- 2、根据凭证信息查找原料批次单号 in_or_out=1 表示原料入库
-- 3、根据原料WMS入库批次号查找采购单号 关联 采购入库单
-- 4、未找到入库单，需要转调拨查找
-- 销售凭证
SELECT *
FROM csx_dwd.csx_dwd_cas_accounting_stock_log_item_di
WHERE sdt>='20240801'
--   AND in_out_type='CODE_TRANS'
--   and in_or_out=1                   -- 找出库的原品
--   and batch_no='CB20241122009060'
 and goods_code='447'
--  and link_wms_batch_no='ZM240830000107'
and credential_no='PZ20241106423251';


SELECT *
FROM csx_dwd.csx_dwd_cas_accounting_stock_log_item_di
WHERE sdt>='20240801'
--   AND in_out_type='CODE_TRANS'
--   and in_or_out=1
  and batch_no='CB20241116135013'
--  and goods_code='1493574'
--  and link_wms_batch_no='ZM240830000107'
-- and credential_no='PZ20241122024457';

-- 销售关联批次
select *
from csx_analyse_tmp.csx_analyse_tmp_source_batch_sale 
where batch_no='CB20241116135013'
and goods_code='1493574'

-- 查询转码单

SELECT *
FROM csx_dwd.csx_dwd_cas_accounting_stock_log_item_di
WHERE sdt>='20240801'
--   AND in_out_type='CODE_TRANS'
--   and in_or_out=1                   -- 找出库的原品
  and batch_no='CB20241104009804'
 and goods_code='620'
--  and link_wms_batch_no='ZM240830000107'
-- and credential_no='PZ20241106423251'
;


-- 查询销售单关联转码单
select * from csx_analyse_tmp.csx_analyse_tmp_wms_change_order_detail   where order_code='ZM241104000030'


-- 蔬菜基地明细导出
with tmp_order_sale as 
(select
   sale_sdt as sdt,
   receive_dc_code,
   receive_dc_name,
   region_code,
   region_name,
   province_code,
   province_name,
   city_group_code,
   city_group_name,
   order_code,
   goods_code,
   goods_name,
   sale_amt,
   sale_qty,
   profit,
   classify_middle_name,
   meta_batch_no, -- 原料批次成本单号
   product_code, -- 原料商品编码
   product_name, -- 原料商品名称
   short_name,
   product_tax_rate,
   product_classify_large_code,
   product_classify_large_name,
   product_classify_middle_code,
   product_classify_middle_name,
   product_classify_small_code,
   product_classify_small_name,
   meta_qty, -- 原料消耗数量
   meta_amt, -- 原料消耗金额
   meta_amt_no_tax, -- 原料消耗金额(未税)
   use_ratio, -- 原料使用占比
   product_ratio, -- 原料工单占比
   purchase_order_no,
   order_qty,
   order_amt,
   batch_no,
   transfer_crdential_no,
   supplier_code,
   supplier_name,
    purchase_order_type, -- 采购订单类型1 集采采购 2 基地采购 3 其他采购
    goods_shipped_type, -- 商品出库类型1 A进A出 2工厂加工 3其他
   channel_type_name,
   channel_type_code,
   supplier_type_code,
   supplier_type_name,
   purchase_crdential_flag,
   sale_correlation_flag,
   batch_qty, -- 销售批次数量
   batch_cost, -- 销售批次成本
   batch_cost_no_tax, -- 销售批次未税成本
   batch_sale_amt, -- 批次销售额
   batch_sale_amt_no_tax, -- 批次未税销售额
   batch_profit, -- 批次毛利额
   batch_profit_no_tax, -- 批次未税毛利额   
   product_profit_rate,
   product_no_tax_profit_rate,
   product_cost_amt,    -- 原料销售成本根据占比计算product_ratio
   product_cost_amt_no_tax,    -- 原料未税销售成本根据占比计算product_ratio
   product_profit,    -- 原料毛利额根据占比计算product_ratio
   product_profit_no_tax,    -- 原料未税毛利额根据占比计算product_ratio
   product_sale_amt,    -- 原料销售额根据占比计算product_ratio
   product_sale_amt_no_tax     -- 原料未税销售额根据占比计算product_ratio
from csx_analyse.csx_analyse_fr_fina_goods_sale_trace_po_di a 
 where sale_sdt >= '20241201' and sale_sdt <= '20241208' 
-- where sale_sdt between regexp_replace(date_sub(current_date,dayofweek(date_sub(current_date,1))-1),'-','') 
-- 				and regexp_replace(date_sub(current_date,1),'-','')
   and province_name='重庆'
  and classify_middle_code='B0202'	-- 蔬菜	
and (purchase_order_no is not null or sale_correlation_flag='调拨采购商品未关联')
group by 
   sale_sdt,
   receive_dc_code,
   receive_dc_name,
   region_code,
   region_name,
   province_code,
   province_name,
   city_group_code,
   city_group_name,
   order_code,
   goods_code,
   goods_name,
   sale_amt,
   sale_qty,
   profit,
   classify_middle_name,
   meta_batch_no, -- 原料批次成本单号
   product_code, -- 原料商品编码
   product_name, -- 原料商品名称
   short_name,
   product_tax_rate,
   product_classify_large_code,
   product_classify_large_name,
   product_classify_middle_code,
   product_classify_middle_name,
   product_classify_small_code,
   product_classify_small_name,
   meta_qty, -- 原料消耗数量
   meta_amt, -- 原料消耗金额
   meta_amt_no_tax, -- 原料消耗金额(未税)
   use_ratio, -- 原料使用占比
   product_ratio, -- 原料工单占比
   purchase_order_no,
   order_qty,
   order_amt,
   batch_no,
   transfer_crdential_no,
   supplier_code,
   supplier_name,
   
    purchase_order_type, -- 采购订单类型1 集采采购 2 基地采购 3 其他采购
    goods_shipped_type, -- 商品出库类型1 A进A出 2工厂加工 3其他
   channel_type_name,
   channel_type_code,
   supplier_type_code,
   supplier_type_name,
   purchase_crdential_flag,
   sale_correlation_flag,
   batch_qty, -- 销售批次数量
   batch_cost, -- 销售批次成本
   batch_cost_no_tax, -- 销售批次未税成本
   batch_sale_amt, -- 批次销售额
   batch_sale_amt_no_tax, -- 批次未税销售额
   batch_profit, -- 批次毛利额
   batch_profit_no_tax, -- 批次未税毛利额    
   product_profit_rate,
   product_no_tax_profit_rate,
   product_cost_amt,    -- 原料销售成本根据占比计算product_ratio
   product_cost_amt_no_tax,    -- 原料未税销售成本根据占比计算product_ratio
   product_profit,    -- 原料毛利额根据占比计算product_ratio
   product_profit_no_tax,    -- 原料未税毛利额根据占比计算product_ratio
   product_sale_amt,    -- 原料销售额根据占比计算product_ratio
   product_sale_amt_no_tax     -- 原料未税销售额根据占比计算product_ratio
  ) 
  select sdt
,receive_dc_code
,receive_dc_name
,region_name
,province_name
,city_group_name
,order_code
,goods_code
,goods_name
,sale_amt
,sale_qty
,profit
,classify_middle_name
,product_code
,product_name
,short_name
,product_tax_rate
,product_classify_middle_name
,product_classify_small_name
,purchase_order_no
,order_qty
,order_amt
,batch_no
,supplier_code
,supplier_name
,channel_type_name
,supplier_type_name
,batch_qty
,product_cost_amt
,product_cost_amt_no_tax
,product_profit
,product_profit_no_tax
,product_sale_amt
,product_sale_amt_no_tax
from tmp_order_sale
