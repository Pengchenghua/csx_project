
-- 当出现供应商成本为负的，可能出现价格补救情况

-- 商品销售追溯采购入库
-- 大区处理 增加低毛利DC 标识,关联供应链仓信息
drop table csx_analyse_tmp.r_csx_analyse_tmp_dc_new_01 ;
create    TABLE csx_analyse_tmp.r_csx_analyse_tmp_dc_new_01 as 
select 
	case when performance_region_code!='10' then '大区'else '平台' end dept_name,
    purchase_org,
    purchase_org_name,
    belong_region_code  region_code,
    belong_region_name  region_name,
    shop_code ,
    shop_name ,
    company_code ,
    company_name ,
    purpose,
    purpose_name,
    basic_performance_city_code as performance_city_code,
    basic_performance_city_name as performance_city_name,
    basic_performance_province_code as performance_province_code,
    basic_performance_province_name as performance_province_name,
    case when c.dc_code is not null then '1' else '0' end as is_purchase_dc ,
    enable_date,
    shop_low_profit_flag
from csx_dim.csx_dim_shop a 
 left join 
 (select distinct belong_region_code,
        belong_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name
  from csx_dim.csx_dim_basic_performance_attribution) b on a.basic_performance_city_code= b.performance_city_code
 left join 
 (select dc_code,regexp_replace(to_date(enable_time),'-','') enable_date 
 from csx_dim.csx_dim_csx_data_market_conf_supplychain_location 
 where sdt='current') c on a.shop_code=c.dc_code
 where sdt='current'    
    ;

-- 销售指定供应链仓
drop table   csx_analyse_tmp.r_csx_analyse_tmp_sale_group_detail_01 ;
create   table csx_analyse_tmp.r_csx_analyse_tmp_sale_group_detail_01 as
select
  sdt,
  split(id, '&') [ 0 ] as credential_no,
  order_code,
  region_code,
  region_name,
  b.performance_province_code province_code,
  b.performance_province_name province_name,
  b.performance_city_code city_group_code,
  b.performance_city_name city_group_name,
  business_type_name,
  inventory_dc_code as dc_code,
  shop_name as dc_name,
  customer_code,
  customer_name,
  a.goods_code,
  c.goods_name,
  a.order_channel_code,
  a.refund_order_flag,
  sale_qty,
  sale_amt,
  sale_amt_no_tax,
  sale_cost_no_tax,
  profit_no_tax,
  sale_cost,
  profit,
  cost_price,
  sale_price,
  c.business_division_code,
  c.business_division_name,
  c.division_code,
  c.division_name,
  c.classify_large_code,
  c.classify_large_name,
  c.classify_middle_code,
  c.classify_middle_name,
  c.classify_small_code,
  c.classify_small_name,
  short_name
from
  csx_dws.csx_dws_sale_detail_di a
  left join csx_analyse_tmp.r_csx_analyse_tmp_dc_new_01 b on a.inventory_dc_code = b.shop_code
  left join
  (select goods_code,goods_name,
          tax_rate/100 product_tax_rate,
          business_division_code,
          business_division_name,
          division_code,
          division_name,
          classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name,
          a.classify_small_code,
          classify_small_name,
          coalesce(short_name,'')short_name
   from  csx_dim.csx_dim_basic_goods a 
   left join 
 (select short_name,
        classify_small_code,
        start_date,
        end_date,
        1 as group_tag
     from csx_ods.csx_ods_data_analysis_prd_source_scm_w_a_group_purchase_classily_df
    ) c on a.classify_small_code=c.classify_small_code
  
   where sdt='current') c on a.goods_code = c.goods_code
   where sdt >= '${sdt_bf31d}' and sdt <= '${sdt_yes}' 
   and a.shipper_code='YHCSX'
   -- where sdt between regexp_replace(date_sub(current_date,dayofweek(date_sub(current_date,1))-1),'-','') 
	-- 			and regexp_replace(date_sub(current_date,1),'-','')
  -- and is_purchase_dc=1
  -- and channel_code not in ('2', '4', '6', '5')
  and business_type_code = '1' -- 日配业务
 
  and b.shop_low_profit_flag = 0
 
  and c.classify_middle_code='B0202'	-- 蔬菜
  
  ;
  
  
  
  
-- 采购入库商品的成本金额对应销售额毛利率-订单商品明细
drop table csx_analyse_tmp.r_csx_analyse_tmp_sale_group_detail_02;
create   table csx_analyse_tmp.r_csx_analyse_tmp_sale_group_detail_02 as  
select
  sdt,
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  a.city_group_code,
  a.city_group_name,
   a.order_code,
   a.goods_code,
   a.sale_amt,
   a.sale_qty,
   a.profit,
   a.meta_batch_no, -- 原料批次成本单号
   a.product_code, -- 原料商品编码
   a.product_name, -- 原料商品名称
   a.product_tax_rate,
   a.product_classify_large_code,
   a.product_classify_large_name,
   a.product_classify_middle_code,
   a.product_classify_middle_name,
   a.product_classify_small_code,
   a.product_classify_small_name,
   a.meta_amt, -- 原料消耗金额
   a.meta_amt_no_tax, -- 原料消耗金额(未税)
   a.use_ratio, -- 原料使用占比
   a.product_ratio, -- 原料工单占比
--   a.purchase_order_no,
   a.receive_dc_code, -- 入库DC
   a.receive_dc_name,
--   a.order_qty,
--   a.order_amt,
   a.batch_no,
   a.transfer_crdential_no,
   a.supplier_code,
   a.supplier_name,
   
    a.purchase_order_type, -- 采购订单类型1 集采采购 2 基地采购 3 其他采购
    a.goods_shipped_type, -- 商品出库类型1 A进A出 2工厂加工 3其他
   a.channel_type_name,
   a.channel_type_code,
   a.supplier_type_code,
   a.supplier_type_name,
   a.purchase_crdential_flag,
   a.sale_correlation_flag,
   a.product_profit_rate,
   a.product_no_tax_profit_rate,
  c.goods_name,
  c.business_division_code,
  c.business_division_name,
  c.division_code,
  c.division_name,
  c.classify_large_code,
  c.classify_large_name,
  c.classify_middle_code,
  c.classify_middle_name,
  c.classify_small_code,
  c.classify_small_name,
  c.short_name,
  c.start_date,
  c.end_date,  
  case when enable_date<=sdt  and is_purchase_dc=1 then 1 else 0 end is_purchase_dc,
  case
    when c.classify_small_code is not null
    and start_date <= sdt
    and end_date >= sdt then 1
    else 0 end as central_pursh_class_tag,
  a.batch_qty, -- 销售批次数量
  a.batch_cost, -- 销售批次成本
  a.batch_cost_no_tax, -- 销售批次未税成本
  a.batch_sale_amt, -- 批次销售额
  a.batch_sale_amt_no_tax, -- 批次未税销售额
  a.batch_profit, -- 批次毛利额
  a.batch_profit_no_tax, -- 批次未税毛利额	
  a.meta_qty,      -- 原料消耗数量
  a.product_cost_amt,    -- 原料销售成本根据占比计算product_ratio
  a.product_cost_amt_no_tax,    -- 原料未税销售成本根据占比计算product_ratio
  a.product_profit,    -- 原料毛利额根据占比计算product_ratio
  a.product_profit_no_tax,    -- 原料未税毛利额根据占比计算product_ratio
  a.product_sale_amt,    -- 原料销售额根据占比计算product_ratio
  a.product_sale_amt_no_tax     -- 原料未税销售额根据占比计算product_ratio
 from 
(
 select
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
--   purchase_order_no,
--   order_qty,
--   order_amt,
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
 where sale_sdt >= '${sdt_bf31d}' and sale_sdt <= '${sdt_yes}' 
-- where sale_sdt between regexp_replace(date_sub(current_date,dayofweek(date_sub(current_date,1))-1),'-','') 
-- 				and regexp_replace(date_sub(current_date,1),'-','')
  -- and province_name='北京'
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
--   purchase_order_no,
--   order_qty,
--   order_amt,
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
  ) a
 left join csx_analyse_tmp.r_csx_analyse_tmp_dc_new_01 b on a.receive_dc_code = b.shop_code
  left join
  (SELECT goods_code,
		  goods_name,
          tax_rate/100 product_tax_rate,
          business_division_code,
          business_division_name,
          division_code,
          division_name,
          classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name,
          a.classify_small_code,
          classify_small_name,
          short_name,
          start_date,
          end_date
   FROM  csx_dim.csx_dim_basic_goods a 
   left join 
   (
    select
      short_name,
      classify_small_code,
      start_date,
      end_date
    from
      csx_ods.csx_ods_data_analysis_prd_source_scm_w_a_group_purchase_classily_df
  ) b on a.classify_small_code=b.classify_small_code 
   WHERE sdt='current') c on a.goods_code = c.goods_code
;
 
-- 采购入库商品的成本金额对应销售额毛利率-日商品汇总 
drop table csx_analyse_tmp.r_csx_analyse_tmp_sale_group_detail_03;
create table csx_analyse_tmp.r_csx_analyse_tmp_sale_group_detail_03 as   
select
  sdt,
  region_code,
  -- region_name,
  province_code,
  -- province_name,
  city_group_code,
  -- city_group_name,
  classify_middle_code,
  -- classify_middle_name,
  classify_small_code,
  -- classify_small_name,
   goods_code,
   product_code,
  -- goods_name,
  count(distinct case when supplier_type_code in('4','10','11','12','13') then goods_code end) as cash_pursh_count_sku,
  coalesce(sum(case when supplier_type_code in('4','10','11','12','13') then batch_qty        end ),0)  cash_pursh_meta_qty,      -- 原料消耗数量
  coalesce(sum(case when supplier_type_code in('4','10','11','12','13') then product_cost_amt        end ),0)  cash_pursh_cost_amt,
  coalesce(sum(case when supplier_type_code in('4','10','11','12','13') then product_cost_amt_no_tax end ),0)  cash_pursh_cost_amt_no_tax,
  coalesce(sum(case when supplier_type_code in('4','10','11','12','13') then product_profit          end ),0)  cash_pursh_profit,
  coalesce(sum(case when supplier_type_code in('4','10','11','12','13') then product_profit_no_tax   end ),0)  cash_pursh_profit_no_tax,
  coalesce(sum(case when supplier_type_code in('4','10','11','12','13') then product_sale_amt        end ),0)  cash_pursh_sale_amt,
  coalesce(sum(case when supplier_type_code in('4','10','11','12','13') then product_sale_amt_no_tax end ),0)  cash_pursh_sale_amt_no_tax,
  
  count(distinct case when supplier_type_code in('5') or sale_correlation_flag='调拨采购商品未关联' then goods_code end) as base_count_sku,
  coalesce(sum(case when supplier_type_code in('5') then batch_qty when sale_correlation_flag='调拨采购商品未关联' then batch_qty end ),0)  base_pursh_meta_qty,    -- 原料消耗数量
  coalesce(sum(case when supplier_type_code in('5') then product_cost_amt when sale_correlation_flag='调拨采购商品未关联' then batch_cost end ),0)  base_pursh_cost_amt,
  coalesce(sum(case when supplier_type_code in('5') then product_cost_amt_no_tax when sale_correlation_flag='调拨采购商品未关联' then batch_cost_no_tax end ),0)  base_pursh_cost_amt_no_tax,
  coalesce(sum(case when supplier_type_code in('5') then product_profit when sale_correlation_flag='调拨采购商品未关联' then batch_profit end ),0)  base_pursh_profit,
  coalesce(sum(case when supplier_type_code in('5') then product_profit_no_tax when sale_correlation_flag='调拨采购商品未关联' then batch_profit_no_tax end ),0)  base_pursh_profit_no_tax,
  coalesce(sum(case when supplier_type_code in('5') then product_sale_amt when sale_correlation_flag='调拨采购商品未关联' then batch_sale_amt end ),0)  base_pursh_sale_amt,
  coalesce(sum(case when supplier_type_code in('5') then product_sale_amt_no_tax when sale_correlation_flag='调拨采购商品未关联' then batch_sale_amt_no_tax end ),0)  base_pursh_sale_amt_no_tax
  
  -- count(distinct case when supplier_type_code in('5') then goods_code end) as base_count_sku,
  -- coalesce(sum(case when supplier_type_code in('5') then meta_qty        end ),0)  base_pursh_meta_qty,    -- 原料消耗数量
  -- coalesce(sum(case when supplier_type_code in('5') then product_cost_amt        end ),0)  base_pursh_cost_amt,
  -- coalesce(sum(case when supplier_type_code in('5') then product_cost_amt_no_tax end ),0)  base_pursh_cost_amt_no_tax,
  -- coalesce(sum(case when supplier_type_code in('5') then product_profit          end ),0)  base_pursh_profit,
  -- coalesce(sum(case when supplier_type_code in('5') then product_profit_no_tax   end ),0)  base_pursh_profit_no_tax,
  -- coalesce(sum(case when supplier_type_code in('5') then product_sale_amt        end ),0)  base_pursh_sale_amt,
  -- coalesce(sum(case when supplier_type_code in('5') then product_sale_amt_no_tax end ),0)  base_pursh_sale_amt_no_tax
from  csx_analyse_tmp.r_csx_analyse_tmp_sale_group_detail_02  a
group by 
  sdt,
  region_code,
  -- region_name,
  province_code,
  -- province_name,
  city_group_code,
  -- city_group_name,
  classify_middle_code,
  -- classify_middle_name,
  classify_small_code,
  -- classify_small_name
   goods_code,
   product_code
;


select
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  a.city_group_code,
  a.city_group_name,
  -- a.business_division_code,
  -- a.business_division_name,
  -- a.division_code,
  -- a.division_name,
  a.classify_large_code,
  a.classify_large_name,
  a.classify_middle_code,
  a.classify_middle_name,
  a.classify_small_code,
  a.classify_small_name,
  a.goods_code,
  -- coalesce(a.short_name,'') short_name, 
  -- coalesce(central_pursh_tag,'')central_pursh_tag,
  -- coalesce(base_pursh_tag,'') base_pursh_tag,
--   a.all_count_sku,
  a.sale_amt,
  a.sale_qty,
  a.profit,
  
  -- coalesce(central_pursh_cost_amt,0) central_pursh_cost_amt,
  -- coalesce(central_pursh_cost_amt_no_tax,0) central_pursh_cost_amt_no_tax,
  -- coalesce(central_pursh_profit,0) central_pursh_profit,
  -- coalesce(central_pursh_profit_no_tax,0) central_pursh_profit_no_tax,
  -- coalesce(central_pursh_sale_amt,0) central_pursh_sale_amt,
  -- coalesce(central_pursh_sale_amt_no_tax,0) central_pursh_sale_amt_no_tax,
  coalesce(product_code,a.goods_code)product_code,
--   coalesce(base_count_sku,0) base_count_sku,
-- 基地
  coalesce(base_pursh_meta_qty,0) base_pursh_meta_qty,
  coalesce(base_pursh_cost_amt,0) base_pursh_cost_amt,
  coalesce(base_pursh_profit,0) base_pursh_profit,
  coalesce(base_pursh_sale_amt,0) base_pursh_sale_amt,
  -- 市场档口/尾货
--   coalesce(cash_pursh_count_sku,0) cash_pursh_count_sku,
   coalesce(cash_pursh_meta_qty,0) cash_pursh_meta_qty,
   coalesce(cash_pursh_cost_amt,0) cash_pursh_cost_amt,
   coalesce(cash_pursh_profit,0) cash_pursh_profit,
   coalesce(cash_pursh_sale_amt,0) cash_pursh_sale_amt, 
   -- 供应商入库
   sale_qty-coalesce(base_pursh_meta_qty,0)-coalesce(cash_pursh_meta_qty,0)-coalesce(sale_qty_th,0) as supplier_qty,
   sale_cost-coalesce(base_pursh_cost_amt,0)-coalesce(cash_pursh_cost_amt,0)-coalesce(sale_cost_th,0) as supplier_cost,
   sale_amt-coalesce(base_pursh_sale_amt,0)- coalesce(cash_pursh_sale_amt,0)-coalesce(sale_amt_fl,0)-coalesce(sale_amt_tj,0)-coalesce(sale_amt_th,0) as supplier_sale_amt,
   profit-coalesce(base_pursh_profit,0)-coalesce(cash_pursh_profit,0)-coalesce(profit_fl,0)-coalesce(profit_tj,0)-coalesce(profit_th,0) as supplier_profit,
   
  current_timestamp() update_time,
  a.sdt as sale_sdt,
  
  a.sale_amt_fl,
  a.profit_fl,

  a.sale_amt_tj,
  a.profit_tj,
  
  a.sale_amt_bj,
  a.profit_bj,
  
  a.sale_amt_th,
  a.profit_th 
from
(select
    sdt,
  region_code,
  region_name,
  province_code,
  province_name,
  city_group_code,
  city_group_name,
  case when classify_large_code in ('B01','B02','B03') then '11' else '12' end business_division_code,
  case when classify_large_code in ('B01','B02','B03') then '生鲜' else '食百' end business_division_name,
  a.division_code,
  a.division_name,
  a.classify_large_code,
  a.classify_large_name,
  a.classify_middle_code,
  a.classify_middle_name,
  a.classify_small_code,
  a.classify_small_name,
  goods_code ,
  -- sum(sale_amt_no_tax) sale_amt_no_tax,
  sum(sale_amt) sale_amt,
  sum(sale_qty) sale_qty,
  sum(profit) profit,
  sum(sale_cost) sale_cost,

  sum(case when order_channel_code='4' then sale_amt end) sale_amt_fl,
  sum(case when order_channel_code='4' then profit end) profit_fl,

  sum(case when order_channel_code='6' then sale_amt end) sale_amt_tj,
  sum(case when order_channel_code='6' then profit end) profit_tj,

  sum(case when order_channel_code='5' then sale_amt end) sale_amt_bj,
  sum(case when order_channel_code='5' then profit end) profit_bj,
  
  sum(case when order_channel_code not in('4','5','6') and refund_order_flag='1' then sale_cost end) sale_cost_th,
  sum(case when order_channel_code not in('4','5','6') and refund_order_flag='1' then sale_qty end) sale_qty_th,
  sum(case when order_channel_code not in('4','5','6') and refund_order_flag='1' then sale_amt end) sale_amt_th,
  sum(case when order_channel_code not in('4','5','6') and refund_order_flag='1' then profit end) profit_th 
  -- sum(profit_no_tax) profit_no_tax
from csx_analyse_tmp.r_csx_analyse_tmp_sale_group_detail_01 a   -- 销售指定供应链仓
    where sdt>='20241212'
    and province_name='北京'
  group by 
  region_code,
  region_name,
  province_code,
  province_name,
  city_group_code,
  city_group_name,
  case when classify_large_code in ('B01','B02','B03') then '11' else '12' end ,
  case when classify_large_code in ('B01','B02','B03') then '生鲜' else '食百' end ,
  a.division_code,
  a.division_name,
  a.classify_large_code,
  a.classify_large_name,
  a.classify_middle_code,
  a.classify_middle_name,
  a.classify_small_code,
  a.classify_small_name,
  goods_code,
  sdt
  ) a 
  left join csx_analyse_tmp.r_csx_analyse_tmp_sale_group_detail_03 b on a.classify_small_code = b.classify_small_code
  and a.classify_middle_code = b.classify_middle_code
  and a.city_group_code = b.city_group_code
  and a.sdt = b.sdt
  and a.province_code = b.province_code
  and a.region_code = b.region_code	  
  and a.goods_code=b.goods_code
  ;



create table csx_analyse.csx_analyse_report_supplier_vegetable_channel_sale_di as 
(region_code	string	comment '大区编码'
,region_name	string	comment '大区名称'
,province_code	string	comment'省区编码'
,province_name	string	comment'省区名称'
,city_group_code	string	comment'城市编码'
,city_group_name	string	comment'城市名称'
,classify_large_code	string	comment'管理大类'
,classify_large_name	string	comment'管理大类名称'
,classify_middle_code	string	comment'管理中类编码'
,classify_middle_name	string	comment'管理中类名称'
,classify_small_code	string	comment'管理小类编码'
,classify_small_name	string	comment'管理小类名称'
,goods_code	string	comment'销售商品编码'
,goods_name	string	comment'销售商品名称'
,sale_amt	decimal(30,6)	comment'销售额'
,sale_qty	decimal(30,6)	comment'销售量'
,profit	decimal(30,6)	comment'销售毛利额'
,sale_cost	decimal(30,6)	comment'销售成本'
,product_code	string	comment'原料商品编码'
,product_name	string	comment'原料商品名称'
,base_pursh_meta_qty	decimal(30,6)comment	'基地销售量'
,base_pursh_cost_amt	decimal(30,6)	comment'基地成本'
,base_pursh_profit	decimal(30,6)	comment'基地毛利额'
,base_pursh_sale_amt	decimal(30,6)	comment'基地销售额'
,cash_pursh_meta_qty	decimal(30,6)	comment'市场档口\尾货销量'
,cash_pursh_cost_amt	decimal(30,6)comment	'市场档口\尾货成本'
,cash_pursh_profit	decimal(30,6)comment	'市场档口\尾货毛利额'
,cash_pursh_sale_amt	decimal(30,6)	comment'市场档口\尾货销售额'
,supplier_qty	decimal(33,6)	comment'供应商销量'
,supplier_cost	decimal(33,6)	comment'供应商成本'
,supplier_sale_amt	decimal(35,6)	comment'供应商销售额'
,supplier_profit	decimal(35,6)	comment'供应商毛利额'

,sale_amt_fl	decimal(30,6)	comment'商品返利'
,profit_fl	decimal(30,6)	comment'商品返利额'
,sale_amt_tj	decimal(30,6)	comment'商品调价'
,profit_tj	decimal(30,6)	comment'商品调价'
,sale_amt_bj	decimal(30,6)	comment'销售额补救'
,profit_bj	decimal(30,6)	comment'毛利额补救'
,sale_amt_th	decimal(30,6)	comment'销售退货额'
,profit_th	decimal(30,6)comment	'毛利退货额'
,update_time	timestamp	comment'更新时间'
,sale_sdt	string	comment'销售日期'
  )comment '供应链蔬菜采购渠道商品top'
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  partitioned by (sdt string)
  