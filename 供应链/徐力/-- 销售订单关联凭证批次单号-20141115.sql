-- 销售订单关联凭证批次单号
-- ******************************************************************** 
-- @功能描述：商品销售追溯采购入库
-- @创建者： 彭承华 
-- @创建者日期：2022-10-11 16:28:58 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 

-- 如何按照调拨的话， 需要根据销售凭证查找批次-- 入库批次号关联调拨入库（单号）根据入库单号查询调拨订单号，再根据订单号查找调拨出库的批次--根据调拨出库凭证查找入库库批次
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=5000;
set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.groupby.skewindata=false;
set hive.map.aggr = true;
-- 允许使用正则
set hive.support.quoted.identifiers=none;
-- 增加reduce过程
set hive.optimize.sort.dynamic.partition=true;
set hive.tez.container.size=8192;

-- 商品销售追溯采购入库
-- 大区处理 增加低毛利DC 标识,关联供应链仓信息
drop table csx_analyse_tmp.csx_analyse_tmp_dc_new ;
create TABLE csx_analyse_tmp.csx_analyse_tmp_dc_new as 
select case when performance_region_code!='10' then '大区'else '平台' end dept_name,
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
    

-- 商品信息处理，关联集采品类
drop table   csx_analyse_tmp.csx_analyse_tmp_goods_short;
create     table csx_analyse_tmp.csx_analyse_tmp_goods_short as    
select goods_code,
    goods_name,
    tax_rate,                       -- 商品税率
    division_code,
    division_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    a.classify_small_code,
    classify_small_name,
    category_large_code,
    category_large_name,
    category_middle_code,
    category_middle_name,
    category_small_code,
    category_small_name,
    category_small_code,
    coalesce(short_name,'') as short_name,
    start_date,
    end_date,
    group_tag
from csx_dim.csx_dim_basic_goods a 
left join 
 (select short_name,
        classify_small_code,
        start_date,
        end_date,
        1 as group_tag
     from csx_ods.csx_ods_data_analysis_prd_source_scm_w_a_group_purchase_classily_df
    ) c on a.classify_small_code=c.classify_small_code
    where sdt='current'
    ;
    
    

 
-- 采购订单关联凭证批次单号
drop table csx_analyse_tmp.csx_analyse_tmp_source_order ;
create   table csx_analyse_tmp.csx_analyse_tmp_source_order as 
SELECT b.credential_no,
       b.real_location_code as receive_dc_code,
       purchase_order_code as source_order_no,
       b.batch_no as purchase_batch_no,
       a.batch_code as wms_batch_no,
       a.order_code as wms_order_no,
       a.supplier_code,
       a.supplier_name,
       settle_dc_code,
       a.goods_code,
       b.qty as pur_qty,
       b.amt as pur_amt,
       b.price as pur_price,
       order_business_type business_type,                              -- 基地标识
       is_central_tag as central_purchase_tag ,  -- 集采标识 
       central_purchase_short_name short_name,
       move_type_code,
	   channel_type_name,
	   channel_type_code,
	   supplier_type_code,
	   supplier_type_name    
-- ; 	   CASE
-- ;     WHEN purpose = '06' THEN '前置仓'
-- ;     when supplier_type = '1' then '经销商/代理商'
-- ;     when supplier_type = '2' then '生产厂商'
-- ;     when supplier_type = '9' then ' 农户个人'
-- ;     when supplier_type = '4' then ' 市场采买供应商'
-- ;     when supplier_type = '5' then ' 基地供应商'
-- ;     when supplier_type = '6' then ' OEM供应商'
-- ;     when supplier_type = '7' then ' 农户专业合作社'
-- ;     when supplier_type = '8' then ' 其他供应商'
-- ;     when supplier_type = '10' then '扫尾货'
-- ;     when supplier_type = '11' then '档口供应商'
-- ;     when supplier_type = '12' then '市场采买扫尾货'
-- ;     when supplier_type = '13' then '市场采买基地供应商'
-- ;     else '无标签'
-- ;   END AS supplier_type_name,
FROM   csx_analyse.csx_analyse_scm_purchase_order_flow_di a 
LEFT JOIN 
(select credential_no,
    batch_no,
    goods_code,
    dc_code real_location_code,
    -- source_order_no,
    link_wms_batch_no,
    link_wms_order_no,
    move_type_code,
    qty,
    amt,
    price
 from   csx_dwd.csx_dwd_cas_accounting_stock_log_item_di a 
    where sdt>='${sdate}'
        and sdt<='${edate}'
       -- and move_type_code ='101A'
        -- and direction_flag ='+'
        and in_out_type='PURCHASE_IN'
) b on a.order_code=b.link_wms_order_no 
    and a.goods_code=b.goods_code
    and a.batch_code=b.link_wms_batch_no 
WHERE   sdt>='${sdate}'
    and sdt<='${edate}'
    and source_type_code not in ('4','15','18') -- 剔除 4项目合伙人、15联营直送、18城市服务商
  --  and super_class_code in (1,2)    -- 1供应商订单、2供应商退货单
;

 



-- 查找102A成本批次单号cb_batch_no
drop table csx_analyse_tmp.csx_analyse_tmp_source_order_02;
create   table csx_analyse_tmp.csx_analyse_tmp_source_order_02 as 
select a.credential_no,
       a.receive_dc_code,
       source_order_no,
       a.wms_batch_no,
       a.wms_order_no,
       a.supplier_code,
       a.supplier_name,
       settle_dc_code,
       a.goods_code,
       a.pur_qty,
       a.pur_amt,
       a.pur_price,
       a.business_type,                              -- 基地标识
       a.central_purchase_tag ,  -- 集采标识 
       a.short_name,
       a.move_type_code,
       b.batch_no as purchase_batch_no,
       channel_type_name,
	   channel_type_code,
	   supplier_type_code,
	   supplier_type_name 
from csx_analyse_tmp.csx_analyse_tmp_source_order a 
left join 
(SELECT credential_no,
       batch_no,
       goods_code,
       move_type_code,
       link_wms_batch_no
FROM csx_dwd.csx_dwd_cas_accounting_stock_log_item_di
WHERE sdt>='${sdate}'
    and sdt<='${edate}'
  and in_out_type='PURCHASE_IN'
  and in_or_out=0
  group by credential_no,
       batch_no,
       goods_code,
        move_type_code,
       link_wms_batch_no
) b on a.wms_batch_no=b.link_wms_batch_no and a.goods_code=b.goods_code and a.move_type_code=b.move_type_code;



-- 销售指定供应链仓
drop table   csx_analyse_tmp.csx_analyse_tmp_source_sale_detal ;
create     table csx_analyse_tmp.csx_analyse_tmp_source_sale_detal as 
    select 
      sdt,
	  split(id, '&')[0] as credential_no,
	  order_code ,
      region_code,
      region_name,
      b.performance_province_code province_code,
      b.performance_province_name province_name,
	  b.performance_city_code city_group_code,
	  b.performance_city_name city_group_name,
      channel_name,
      order_channel_code,
	  business_type_name,
	  inventory_dc_code as dc_code, 
      customer_code,
      customer_name,
      goods_code,
      goods_name,
      sale_qty,
      sale_amt,
      sale_amt_no_tax ,
      sale_cost_no_tax , 
      profit_no_tax ,
      sale_cost,
      profit,
      cost_price,
      sale_price,
      division_code,
      classify_large_code,
      classify_middle_code,
      a.classify_small_code
    from  csx_dws.csx_dws_sale_detail_di a
    left join  csx_analyse_tmp.csx_analyse_tmp_dc_new b on a.inventory_dc_code=b.shop_code
       where sdt>='${s_date}'
        and sdt<='${edate}'
    --    and is_purchase_dc=1
        and channel_code not in ('2','4', '6','5')
 	    and business_type_code ='1'   -- 日配业务 
        and b.shop_low_profit_flag =0   -- 剔除DC直送仓
        and refund_order_flag =0            -- 正向单
	    and order_channel_code not in ('4','6')  -- 不含调价返利
	    -- and division_code in('11','10','12','13')
;




-- 查找销售批次号
drop table csx_analyse_tmp.csx_analyse_tmp_source_batch_sale;
create     table csx_analyse_tmp.csx_analyse_tmp_source_batch_sale as 
select sdt,
    region_code,
    region_name,
    province_code,
    province_name,
	city_group_code,
	city_group_name,
    dc_code,
    a.credential_no,
    channel_name,
    order_channel_code,
    order_code,
    a.goods_code,
    a.goods_name,
    division_code,
    classify_large_code,
    classify_middle_code,
    a.classify_small_code,
    sale_qty,
    sale_amt,
    sale_amt_no_tax ,
    sale_cost_no_tax , 
    profit_no_tax ,
    sale_cost,
    profit,
    cost_price,
    sale_price,
    b.batch_no,
    batch_qty,
    batch_amt,
    batch_amt_no_tax,
    batch_price
FROM csx_analyse_tmp.csx_analyse_tmp_source_sale_detal a
left join
(SELECT credential_no,
       batch_no,
       goods_code,
       qty  batch_qty,
       amt  batch_amt,
       amt_no_tax batch_amt_no_tax,
       price batch_price
FROM csx_dwd.csx_dwd_cas_accounting_stock_log_item_di
WHERE sdt>='${sdate}'
    and sdt<='${edate}'
  AND in_out_type='SALE_OUT'
  and in_or_out=1	) b on a.credential_no=b.credential_no and a.goods_code=b.goods_code;
  
  
 -- 根据批次号查找采购入库凭证 
 drop table  csx_analyse_tmp.csx_analyse_tmp_source_batch_sale_01;
 create   table  csx_analyse_tmp.csx_analyse_tmp_source_batch_sale_01 as 
 select sdt,
    region_code,
    region_name,
    province_code,
    province_name,
	city_group_code,
	city_group_name,
    dc_code,           -- 销售出库DC
    receive_dc_code,   -- 入库DC
    a.credential_no,
    order_code,
    channel_name,
    order_channel_code,
    a.goods_code,
    a.goods_name,
    division_code,
    classify_large_code,
    classify_middle_code,
    classify_small_code,
    sale_price,
    a.sale_cost,
    a.sale_amt,
    a.sale_qty,
    sale_amt_no_tax ,
    sale_cost_no_tax , 
    profit_no_tax ,
    a.profit,
    batch_qty,
    batch_amt,
    batch_amt_no_tax,
    batch_price,
    purchase_crdential_no,
    a.batch_no,
    b.qty as pur_qty,
    b.amt as pur_amt,
    b.amt_no_tax as pur_amt_no_tax,
    b.price as pur_price,
    if(b.purchase_crdential_no is not null ,1,0) as purchase_crdential_flag     -- 关联采购凭证标识
from  csx_analyse_tmp.csx_analyse_tmp_source_batch_sale a 
 left join
(SELECT credential_no as purchase_crdential_no,
       batch_no as purchase_batch_no,
       dc_code as receive_dc_code,
       goods_code,
       (qty)qty,
       (amt) amt,
       (amt_no_tax) amt_no_tax,
        price
FROM  csx_dwd.csx_dwd_cas_accounting_stock_log_item_di
WHERE sdt>='${sdate}'
    and sdt<='${edate}'
    and in_out_type='PURCHASE_IN'
    and in_or_out=0	
    and move_type_code= '101A'	
    and link_wms_order_no like 'IN%'
    -- and frozen_flag=0
  ) b on a.batch_no=b.purchase_batch_no and a.goods_code=b.goods_code
  
  where purchase_crdential_no is not null 
  ;
  
 
  
  
  -- 根据成品批次号查找领料凭证号 
drop table   csx_analyse_tmp.csx_analyse_tmp_source_batch_sale_02;
create  table csx_analyse_tmp.csx_analyse_tmp_source_batch_sale_02 as 
select sdt,
    region_code,
    region_name,
    province_code,
    province_name,
	city_group_code,
	city_group_name,
    dc_code,
    a.credential_no,
    order_code,
    channel_name,
    order_channel_code,
    a.goods_code,
    a.goods_name,
    division_code,
    classify_large_code,
    classify_middle_code,
    classify_small_code,
    sale_price,
    a.sale_cost,
    a.sale_amt,
    a.sale_qty,
    sale_amt_no_tax ,
    sale_cost_no_tax , 
    profit_no_tax ,
    a.profit,
    batch_qty,
    batch_amt,
    batch_amt_no_tax,
    batch_price,
    a.batch_no,
    transfer_crdential_no,
    transfer_qty,
    transfer_amt,
    transfer_price,
    transfer_amt_no_tax ,
    if(b.transfer_crdential_no is not null ,1,0) as transfer_crdential_flag     -- 关联领料凭证标识
from csx_analyse_tmp.csx_analyse_tmp_source_batch_sale a 
 left join
(SELECT credential_no as transfer_crdential_no,
       batch_no as transfer_batch_no,
       goods_code,
       qty as   transfer_qty,
       amt as   transfer_amt,
       amt_no_tax as transfer_amt_no_tax,
       price as transfer_price
FROM  csx_dwd.csx_dwd_cas_accounting_stock_log_item_di
WHERE sdt>='${sdate}'
    and sdt<='${edate}'
  AND in_out_type='FINISHED'
  and in_or_out=0
  ) b on a.batch_no=b.transfer_batch_no and a.goods_code=b.goods_code
where  b.transfer_crdential_no is not null 
;



-- select * from csx_tmp.temp_batch_sale_02 where sales_qty>0 and transfer_batch_no is not null;
 -- select distinct province_name from csx_analyse_tmp.csx_analyse_tmp_jd_batch_sale_03

-- 根据领料凭证号查找原料批次号
drop table    csx_analyse_tmp.csx_analyse_tmp_source_batch_sale_03;
create      table  csx_analyse_tmp.csx_analyse_tmp_source_batch_sale_03 as 
select  a.transfer_crdential_no,
    a.goods_code,
    transfer_qty,
    transfer_amt,
    transfer_price,
    meta_batch_no,
    product_code,
    meta_qty,
    meta_amt,
    meta_amt_no_tax,
    meta_amt/sum(meta_amt)over(partition by transfer_crdential_no ) as ratio
from
    (select transfer_crdential_no,
        goods_code,
        transfer_qty,
        transfer_amt,
        transfer_price
    from csx_analyse_tmp.csx_analyse_tmp_source_batch_sale_02
        where transfer_crdential_flag=1
    group by transfer_crdential_no,
        goods_code,
        transfer_qty,
        transfer_amt,
        transfer_price
    ) a 
 left join
(SELECT credential_no as meta_crdential_no,
       batch_no as meta_batch_no,
       goods_code product_code,
       sum(qty) as meta_qty,
       sum(amt) as meta_amt,
       sum(amt_no_tax) meta_amt_no_tax
FROM csx_dwd.csx_dwd_cas_accounting_stock_log_item_di
WHERE sdt>='${sdate}'
  and sdt<='${edate}'
  AND in_out_type='FINISHED'
  and in_or_out=1
  group by credential_no ,
       batch_no ,
       goods_code
  ) b on a.transfer_crdential_no=b.meta_crdential_no 
;

-- 计算占比，根据销售凭证号计算占比
drop table csx_analyse_tmp.csx_analyse_tmp_source_batch_sale_04 ;
create   table csx_analyse_tmp.csx_analyse_tmp_source_batch_sale_04 as 
select sdt,
    region_code,
    region_name,
    province_code,
    province_name,
	city_group_code,
	city_group_name,
    dc_code,
    a.credential_no,
    order_code,
    channel_name,
    order_channel_code,
    a.goods_code,
    a.goods_name,
    division_code,
    classify_large_code,
    classify_middle_code,
    classify_small_code,
    sale_price,
    a.sale_cost,
    a.sale_amt,
    a.sale_qty,
    sale_amt_no_tax ,
    sale_cost_no_tax , 
    profit_no_tax ,
    a.profit,
    batch_qty,
    batch_amt,
    batch_amt_no_tax,
    batch_price,
    a.batch_no,
    a.transfer_crdential_no,
    a.transfer_qty,
    a.transfer_amt,
    a.transfer_price,
    a.transfer_amt_no_tax , 
    meta_batch_no,
    product_code,
    meta_qty,
    meta_amt,
    meta_amt/sum(meta_amt)over(partition by a.credential_no,a.goods_code ) as ratio,
    transfer_crdential_flag
from csx_analyse_tmp.csx_analyse_tmp_source_batch_sale_02 a 
left join 
(select a.transfer_crdential_no,
    goods_code,
    transfer_qty,
    transfer_amt,
    transfer_price,
    meta_batch_no,
    product_code,
    meta_qty,
    meta_amt,
    ratio
from csx_analyse_tmp.csx_analyse_tmp_source_batch_sale_03 a 
  ) b on b.transfer_crdential_no =a.transfer_crdential_no and a.goods_code=b.goods_code
 ;

-- 工厂商品
drop table csx_analyse_tmp.csx_analyse_tmp_source_puracse_product;
create table csx_analyse_tmp.csx_analyse_tmp_source_puracse_product as
select sdt,
    region_code,
    region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    dc_code,
    a.credential_no,
    order_code,
    channel_name,
    order_channel_code,
    a.goods_code,
    a.goods_name,
    division_code,
    sale_price,
    a.sale_cost,
    a.sale_amt,
    a.sale_qty,
    sale_amt_no_tax,
    sale_cost_no_tax,
    profit_no_tax,
    a.profit,
    batch_qty,
    batch_amt,
    batch_amt_no_tax,
    batch_price,
    a.batch_no,
    a.transfer_crdential_no,
    a.transfer_qty,
    a.transfer_amt,
    a.transfer_price,
    a.transfer_amt_no_tax,
    b.meta_batch_no,
    b.source_order_no,
    b.receive_dc_code,
    b.product_code,
    b.product_name,
    b.short_name,
    b.product_tax_rate,
    -- 原料商品税率
    b.classify_large_code as product_classify_large_code,
    b.classify_large_name as product_classify_large_name,
    b.classify_middle_code as product_classify_middle_code,
    b.classify_middle_name as product_classify_middle_name,
    b.classify_small_code as product_classify_small_code,
    b.classify_small_name as product_classify_small_name,
    meta_qty,
    meta_amt,
    meta_amt_no_tax,
    meta_amt / sum(meta_amt) over(partition by a.credential_no, a.goods_code) as use_ratio,
    -- 原料使用占比
    product_ratio,
    order_qty,
    order_amt,
    business_type,
    central_purchase_tag,
    supplier_code,
    b.supplier_name,
    channel_type_name,
    channel_type_code,
    supplier_type_code,
    supplier_type_name,
    transfer_crdential_flag,
    if(b.transfer_crdential_no is not null ,1,0 ) is_meta_order_flag          -- 原料订单标识是否关联上
from csx_analyse_tmp.csx_analyse_tmp_source_batch_sale_02 a
    left join (
        SELECT a.transfer_crdential_no,
            a.goods_code,
            transfer_qty,
            transfer_amt,
            transfer_price,
            meta_batch_no,
            source_order_no,
            receive_dc_code,
            product_code,
            goods_name as product_name,
            short_name,
            product_tax_rate,
            classify_large_code,
            classify_large_name,
            classify_middle_code,
            classify_middle_name,
            classify_small_code,
            classify_small_name,
            meta_qty,
            meta_amt,
            meta_amt_no_tax,
            order_qty,
            order_amt,
            ratio as product_ratio,
            business_type,
            central_purchase_tag,
            supplier_code,
            supplier_name,
            channel_type_name,
            channel_type_code,
            supplier_type_code,
            supplier_type_name
        FROM csx_analyse_tmp.csx_analyse_tmp_source_batch_sale_03 a
            left JOIN (
                SELECT meta_batch_no batch_no,
                    business_type,
                    central_purchase_tag,
                    source_order_no,
                    order_qty,
                    order_amt,
                    receive_dc_code,
                    supplier_code,
                    supplier_name,
                    channel_type_name,
                    channel_type_code,
                    supplier_type_code,
                    supplier_type_name
                FROM csx_analyse_tmp.csx_analyse_tmp_source_batch_sale_04 a
                    LEFT JOIN (
                        SELECT purchase_batch_no,
                            a.business_type,
                            central_purchase_tag,
                            source_order_no,
                            receive_dc_code,
                            supplier_code,
                            supplier_name,
                            move_type_code,
                            channel_type_name,
                            channel_type_code,
                            supplier_type_code,
                            supplier_type_name,
                            sum(a.pur_qty) order_qty,
                            sum(a.pur_amt) order_amt
                        FROM csx_analyse_tmp.csx_analyse_tmp_source_order_02 a
                           where move_type_code='101A'
                        group by purchase_batch_no,
                            a.business_type,
                            source_order_no,
                            receive_dc_code,
                            supplier_code,
                            central_purchase_tag,
                            move_type_code,
                            supplier_name,
                            channel_type_name,
                            channel_type_code,
                            supplier_type_code,
                            supplier_type_name
                    ) b ON a.meta_batch_no = b.purchase_batch_no
                group by meta_batch_no,
                    business_type,
                    central_purchase_tag,
                    source_order_no,
                    order_qty,
                    order_amt,
                    supplier_code,
                    supplier_name,
                    receive_dc_code,
                    channel_type_name,
                    channel_type_code,
                    supplier_type_code,
                    supplier_type_name
            ) b ON a.meta_batch_no = b.batch_no
            left JOIN (
                SELECT goods_code,
                    goods_name,
                    short_name,
                    tax_rate / 100 product_tax_rate,
                    classify_large_code,
                    classify_large_name,
                    classify_middle_code,
                    classify_middle_name,
                    classify_small_code,
                    classify_small_name
                FROM csx_analyse_tmp.csx_analyse_tmp_goods_short
            ) g ON a.product_code = g.goods_code
        WHERE b.batch_no IS NOT NULL
    ) b on b.transfer_crdential_no = a.transfer_crdential_no
    and a.goods_code = b.goods_code
-- where b.transfer_crdential_no is not null
;

-- 工厂端成品销售
drop table  csx_analyse_tmp.csx_analyse_tmp_source_puracse_product_01;
create  table csx_analyse_tmp.csx_analyse_tmp_source_puracse_product_01 as 
select 
     substr(sdt,1,6) sale_month,
    sdt as sale_sdt,
    a.region_code,
    a.region_name,
    a.province_code,
    a.province_name,
	a.city_group_code,
	a.city_group_name,
    dc_code,
    d.shop_name as  dc_name,
    a.credential_no,
    order_code,
    a.goods_code,
    a.goods_name,
    tax_rate,                       -- 商品税率
    division_code,
    division_name,
    c.classify_large_code,
    c.classify_large_name,
    c.classify_middle_code,
    c.classify_middle_name,
    c.classify_small_code,
    c.classify_small_name,
    sale_price,
    a.sale_cost,
    a.sale_amt,
    a.sale_qty,
    a.profit,
    sale_amt_no_tax ,
    sale_cost_no_tax , 
    profit_no_tax ,   
    a.batch_no, 
    if(transfer_crdential_flag=1,batch_price,0) batch_price,
    if(transfer_crdential_flag=1,batch_qty   ,0)batch_qty,
    if(transfer_crdential_flag=1,batch_amt   ,0)batch_amt,
    if(transfer_crdential_flag=1,batch_amt_no_tax,0)batch_amt_no_tax,
    if(transfer_crdential_flag=1,(a.sale_price* batch_qty ),0) as batch_sale_amt,
    if(transfer_crdential_flag=1,(a.sale_price/(1+tax_rate)*batch_qty ),0) as batch_sale_amt_no_tax,  
    if(transfer_crdential_flag=1,(a.sale_price* batch_qty )- batch_amt,0) as batch_profit,
    if(transfer_crdential_flag=1,(a.sale_price/(1+tax_rate)*batch_qty )-batch_amt_no_tax,0) as batch_profit_no_tax,
    if(transfer_crdential_flag=1,(a.sale_price* batch_qty - batch_amt)/(a.sale_price* batch_qty ),0) as batch_profit_rate,
    if(transfer_crdential_flag=1,(a.sale_price/(1+tax_rate)*batch_qty -batch_amt_no_tax)/(a.sale_price/(1+tax_rate)*batch_qty ),0) as batch_profit_rate_no_tax,
    a.transfer_crdential_no,    -- 成品凭证单号
    a.transfer_price,
    a.transfer_qty,
    a.transfer_amt,
    a.transfer_amt_no_tax ,  
    meta_batch_no,              -- 原料批次成本单号           
    product_code,               -- 原料商品编码
    product_name,               -- 原料商品名称
    short_name,
    product_tax_rate,           -- 原料商品税率
    product_classify_large_code	    ,
    product_classify_large_name	    ,
    product_classify_middle_code    ,
    product_classify_middle_name    ,
    product_classify_small_code	    ,
    product_classify_small_name	    ,
    meta_qty,                   -- 原料消耗数量
    meta_amt,                   -- 原料消耗金额
    meta_amt_no_tax,            -- 原料消耗金额(未税)
    use_ratio,                  -- 原料使用占比
    product_ratio,              -- 原料工单占比
    source_order_no, 
    receive_dc_code,            -- 入库DC
    f.shop_name as receive_dc_name,
    order_qty,
    order_amt,
    supplier_code,
    supplier_name,
    if(transfer_crdential_flag=1,(a.sale_price* batch_qty ) * a.product_ratio,0) as product_sale_amt,
    if(transfer_crdential_flag=1,(a.sale_price/(1+tax_rate) * batch_qty ) * a.product_ratio,0) as product_sale_amt_no_tax,
    if(transfer_crdential_flag=1,batch_amt * product_ratio,0) as product_cost_amt,
    if(transfer_crdential_flag=1,a.batch_amt_no_tax * product_ratio,0) as product_cost_amt_no_tax,
    if(transfer_crdential_flag=1,(sale_price*batch_qty*product_ratio-a.batch_amt * a.product_ratio),0) product_profit,
    if(transfer_crdential_flag=1,(a.sale_price/(1+tax_rate)*batch_qty*product_ratio-a.batch_amt_no_tax * a.product_ratio),0) product_profit_no_tax,
    if(transfer_crdential_flag=1,(sale_price*batch_qty*product_ratio-a.batch_amt * a.product_ratio)/((a.sale_price* batch_qty ) * a.product_ratio) ,0) as product_profit_rate,
    if(transfer_crdential_flag=1,(a.sale_price/(1+tax_rate)*batch_qty*product_ratio-a.batch_amt_no_tax * a.product_ratio)/(a.sale_price/(1+tax_rate) * batch_qty),0) product_no_tax_profit_rate,
    case
    when supplier_type_code = '5'
    or business_type = 1 then '2'
    when central_purchase_tag = '1' then '1'
    else '3'
    end purchase_order_type,         -- 采购订单类型1 集采采购 2 基地采购 3 其他采购
    '2' as goods_shipped_type,           -- 商品出库类型1 A进A出 2工厂加工 3其他
    current_timestamp as update_time,
    channel_type_name,
    channel_type_code,
    supplier_type_code,
    supplier_type_name,
    transfer_crdential_flag as purchase_crdential_flag , --关联到的工厂采购入库凭证 1 关联 0 未关联
    is_meta_order_flag,
    substr(sdt,1,6) smt
from csx_analyse_tmp.csx_analyse_tmp_source_puracse_product a
left join
(select goods_code,
    tax_rate/100 tax_rate,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    division_name
 from csx_analyse_tmp.csx_analyse_tmp_goods_short ) c on a.goods_code=c.goods_code
left join 
 csx_analyse_tmp.csx_analyse_tmp_dc_new d on a.dc_code=d.shop_code
left join csx_analyse_tmp.csx_analyse_tmp_dc_new f on a.receive_dc_code=f.shop_code
-- where business_type=1
-- and classify_large_code='B02'
;


-- 采购端入库销售
drop table csx_analyse_tmp.csx_analyse_tmp_source_puracse_product_02;
create table csx_analyse_tmp.csx_analyse_tmp_source_puracse_product_02 as
select
  substr(sdt, 1, 6) sale_month,
  sdt as sale_sdt,
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  a.city_group_code,
  a.city_group_name,
  dc_code,
  d.shop_name as dc_name,
  a.credential_no,
  order_code,
  a.goods_code,
  a.goods_name,
  tax_rate,
  division_code,
  c.division_name,
  c.classify_large_code,
  c.classify_large_name,
  c.classify_middle_code,
  c.classify_middle_name,
  c.classify_small_code,
  c.classify_small_name,
  sale_price,
  a.sale_cost,
  a.sale_amt,
  a.sale_qty,
  a.profit,
  sale_amt_no_tax,
  sale_cost_no_tax,
  profit_no_tax,
  a.batch_no,
  batch_price,
  batch_qty,
  batch_amt,
  batch_amt_no_tax,
  if(purchase_crdential_flag=1,(a.sale_price * batch_qty),0) as batch_sale_amt,
  if(purchase_crdential_flag=1,(a.sale_price / (1 + tax_rate) * batch_qty),0) as batch_sale_amt_no_tax,
  if(purchase_crdential_flag=1,(a.sale_price * batch_qty) - batch_amt,0) as batch_profit,
  if(purchase_crdential_flag=1,(a.sale_price / (1 + tax_rate) * batch_qty) - batch_amt_no_tax,0) as batch_profit_no_tax,
  if(purchase_crdential_flag=1,(a.sale_price * batch_qty - batch_amt) / (a.sale_price * batch_qty),0) as batch_profit_rate,
  if(purchase_crdential_flag=1,(
    a.sale_price / (1 + tax_rate) * batch_qty - batch_amt_no_tax
  ) / (a.sale_price / (1 + tax_rate) * batch_qty) ,0) as batch_profit_rate_no_tax,
  '' as transfer_crdential_no,
  -- 成品凭证单号
  0 as transfer_price,
  0 as transfer_qty,
  0 as transfer_amt,
  0 as transfer_amt_no_tax,
  batch_no as meta_batch_no,
  -- 原料批次成本单号
  a.goods_code as product_code,
  -- 原料商品编码
  a.goods_name as product_name,
  -- 原料商品名称
  short_name,
  tax_rate as product_tax_rate,
  c.classify_large_code as product_classify_large_code,
  c.classify_large_name as product_classify_large_name,
  c.classify_middle_code as product_classify_middle_code,
  c.classify_middle_name as product_classify_middle_name,
  c.classify_small_code as product_classify_small_code,
  c.classify_small_name as product_classify_small_name,
  if(purchase_crdential_flag=1,batch_qty,0) as meta_qty,
  -- 原料消耗数量
  if(purchase_crdential_flag=1,batch_amt,0) as meta_amt,
  -- 原料消耗金额
  if(purchase_crdential_flag=1,batch_amt_no_tax,0) as meta_amt_no_tax,
  -- 原料消耗金额(未税)
  1 as use_ratio,
  -- 原料使用占比
  1 as product_ratio,
  -- 原料工单占比
  source_order_no,
  b.receive_dc_code,
  -- 入库DC
  f.shop_name receive_dc_name,
  order_qty,
  order_amt,
  supplier_code,
  supplier_name,
  if(purchase_crdential_flag=1, a.sale_price * batch_qty,0) as product_sale_amt,
  if(purchase_crdential_flag=1, (a.sale_price / (1 + tax_rate)) * a.batch_qty,0) as product_sale_amt_no_tax,
  if(purchase_crdential_flag=1,a.batch_amt,0) as product_cost_amt,
  if(purchase_crdential_flag=1,a.batch_amt_no_tax,0) as product_cost_amt_no_tax,
  if(purchase_crdential_flag=1,a.sale_price * batch_qty - batch_amt ,0) as product_profit,
  if(purchase_crdential_flag=1,(a.sale_price / (1 + tax_rate)) * a.batch_qty - batch_amt_no_tax ,0) as product_profit_no_tax,
  if(purchase_crdential_flag=1,(a.sale_price * batch_qty - batch_amt) / (a.sale_price * batch_qty),0) as product_profit_rate,
  if(purchase_crdential_flag=1,(
    (a.sale_price / (1 + tax_rate)) * a.batch_qty - batch_amt_no_tax
  ) / (a.sale_price / (1 + tax_rate) * a.batch_qty),0) as product_no_tax_profit_rate,
  case
    when supplier_type_code = '5'
    or business_type = 1 then '2'
    when central_purchase_tag = '1' then '1'
    
    else '3'
  end as purchase_order_type,
  -- 采购订单类型1 集采采购 2 基地采购 3 其他采购
  '1' as goods_shipped_type,
  -- 商品出库类型1 A进A出 2工厂加工 3其他
  current_timestamp() update_time,
  channel_type_name,
  channel_type_code,
  supplier_type_code,
  supplier_type_name,
  purchase_crdential_flag,      -- 关联到的采购单号
  1 is_meta_order_flag,            -- 原料是否关联到采购单  
  substr(sdt, 1, 6) smt
from
  csx_analyse_tmp.csx_analyse_tmp_source_batch_sale_01 a
  left join (
        SELECT purchase_batch_no,
            a.business_type,
            central_purchase_tag,
            source_order_no,
            goods_code,
            supplier_code,
            supplier_name,
            move_type_code,
            receive_dc_code,
            channel_type_name,
            channel_type_code,
            supplier_type_code,
            supplier_type_name,
            sum(a.pur_qty) order_qty,
            sum(a.pur_amt) order_amt
        FROM csx_analyse_tmp.csx_analyse_tmp_source_order_02 a 
        where move_type_code='101A'
        group by purchase_batch_no,
            a.business_type,
            source_order_no,
            supplier_code,
            supplier_name,
            a.goods_code,
            move_type_code,
            receive_dc_code,
            channel_type_name,
            channel_type_code,
            supplier_type_code,
            supplier_type_name,
            central_purchase_tag ) b on a.batch_no = b.purchase_batch_no
  and b.goods_code = a.goods_code
  left join (
    select
      goods_code,
      tax_rate / 100 tax_rate,
      short_name,
      classify_large_code,
      classify_large_name,
      classify_middle_code,
      classify_middle_name,
      classify_small_code,
      classify_small_name,
      division_name
    from
      csx_analyse_tmp.csx_analyse_tmp_goods_short
  ) c on a.goods_code = c.goods_code
  left join csx_analyse_tmp.csx_analyse_tmp_dc_new d on a.dc_code = d.shop_code
  left join csx_analyse_tmp.csx_analyse_tmp_dc_new f on b.receive_dc_code = f.shop_code -- where business_type=1
  -- and a.classify_large_code='B02'
;

 -- 根据批次号查找调拔入库凭证 
 drop table  csx_analyse_tmp.csx_analyse_tmp_source_batch_sale_trans_01;
 create       table  csx_analyse_tmp.csx_analyse_tmp_source_batch_sale_trans_01 as 
 select sdt,
    region_code,
    region_name,
    province_code,
    province_name,
	  city_group_code,
	  city_group_name,
    dc_code,           -- 销售出库DC
    receive_dc_code,   -- 入库DC
    a.credential_no,
    order_code,
    channel_name,
    order_channel_code,
    a.goods_code,
    a.goods_name,
    division_code,
    classify_large_code,
    classify_middle_code,
    classify_small_code,
    sale_price,
    a.sale_cost,
    a.sale_amt,
    a.sale_qty,
    sale_amt_no_tax ,
    sale_cost_no_tax , 
    profit_no_tax ,
    a.profit,
    batch_qty,
    batch_amt,
    batch_amt_no_tax,
    batch_price,
    purchase_crdential_no,
    a.batch_no,
    b.qty as pur_qty,
    b.amt as pur_amt,
    b.amt_no_tax as pur_amt_no_tax,
    b.price as pur_price,
    b.wms_batch_no,
    b.wms_order_no,
    if(b.purchase_crdential_no is not null ,1,0) as purchase_crdential_flag     -- 关联采购凭证标识
from  csx_analyse_tmp.csx_analyse_tmp_source_batch_sale a 
 left join
(
select credential_no purchase_crdential_no,
    batch_no purchase_batch_no,
    goods_code,
    dc_code receive_dc_code,
    link_wms_batch_no as wms_batch_no, 
    link_wms_order_no as wms_order_no,
    move_type_code,
    in_out_type,
    qty,
    amt,
    price,
    amt_no_tax
 from  csx_dwd.csx_dwd_cas_accounting_stock_log_item_di a 
    where sdt>='${sdate}'
  		and sdt<='${edate}'
        and move_type_code ='102A'
        and in_or_out=0
        and in_out_type='PURCHASE_IN'
        and link_wms_order_no like 'IN%'
  ) b on a.batch_no=b.purchase_batch_no and a.goods_code=b.goods_code
  ;




-- 调拔单关联入库采购单
drop table csx_analyse_tmp.csx_analyse_tmp_source_batch_sale_trans_02;
create table csx_analyse_tmp.csx_analyse_tmp_source_batch_sale_trans_02 as 
  select substr(sdt, 1, 6) sale_month,
  sdt as sale_sdt,
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  a.city_group_code,
  a.city_group_name,
  dc_code,
  d.shop_name as dc_name,
  a.credential_no,
  order_code,
  a.goods_code,
  a.goods_name,
  tax_rate,
  division_code,
  c.division_name,
  c.classify_large_code,
  c.classify_large_name,
  c.classify_middle_code,
  c.classify_middle_name,
  c.classify_small_code,
  c.classify_small_name,
  sale_price,
  a.sale_cost,
  a.sale_amt,
  a.sale_qty,
  a.profit,
  sale_amt_no_tax,
  sale_cost_no_tax,
  profit_no_tax,
  a.batch_no,
  batch_price,
  batch_qty,
  batch_amt,
  batch_amt_no_tax,
  if(b.purchase_batch_no is not null,(a.sale_price * batch_qty),0) as batch_sale_amt,
  if(b.purchase_batch_no is not null,(a.sale_price / (1 + tax_rate) * batch_qty),0) as batch_sale_amt_no_tax,
  if(b.purchase_batch_no is not null,(a.sale_price * batch_qty) - batch_amt,0) as batch_profit,
  if(b.purchase_batch_no is not null,(a.sale_price / (1 + tax_rate) * batch_qty) - batch_amt_no_tax,0) as batch_profit_no_tax,
  if(b.purchase_batch_no is not null,(a.sale_price * batch_qty - batch_amt) / (a.sale_price * batch_qty),0) as batch_profit_rate,
  if(b.purchase_batch_no is not null,( a.sale_price / (1 + tax_rate) * batch_qty - batch_amt_no_tax ) / (a.sale_price / (1 + tax_rate) * batch_qty) ,0) as batch_profit_rate_no_tax,  '' as transfer_crdential_no,
  -- 成品凭证单号
  0 as transfer_price,
  0 as transfer_qty,
  0 as transfer_amt,
  0 as transfer_amt_no_tax,
  batch_no as meta_batch_no,
  -- 原料批次成本单号
  a.goods_code as product_code,
  -- 原料商品编码
  a.goods_name as product_name,
  -- 原料商品名称
  short_name,
  tax_rate as product_tax_rate,
  c.classify_large_code as product_classify_large_code,
  c.classify_large_name as product_classify_large_name,
  c.classify_middle_code as product_classify_middle_code,
  c.classify_middle_name as product_classify_middle_name,
  c.classify_small_code as product_classify_small_code,
  c.classify_small_name as product_classify_small_name,
  if(purchase_crdential_flag=1,batch_qty,0) as meta_qty,
  -- 原料消耗数量
  if(purchase_crdential_flag=1,batch_amt,0) as meta_amt,
  -- 原料消耗金额
  if(purchase_crdential_flag=1,batch_amt_no_tax,0) as meta_amt_no_tax,
  -- 原料消耗金额(未税)
  1 as use_ratio,
  -- 原料使用占比
  1 as product_ratio,
  -- 原料工单占比
  b.source_order_no,
  b.receive_dc_code,
  -- 入库DC
  f.shop_name receive_dc_name,
  order_qty,
  order_amt,
  supplier_code,
  supplier_name,
  if(b.purchase_batch_no is not null, a.sale_price * batch_qty,0) as product_sale_amt,
  if(b.purchase_batch_no is not null, (a.sale_price / (1 + tax_rate)) * a.batch_qty,0) as product_sale_amt_no_tax,
  if(b.purchase_batch_no is not null,a.batch_amt,0) as product_cost_amt,
  if(b.purchase_batch_no is not null,a.batch_amt_no_tax,0) as product_cost_amt_no_tax,
  if(b.purchase_batch_no is not null,a.sale_price * batch_qty - batch_amt ,0) as product_profit,
  if(b.purchase_batch_no is not null,(a.sale_price / (1 + tax_rate)) * a.batch_qty - batch_amt_no_tax ,0) as product_profit_no_tax,
  if(b.purchase_batch_no is not null,(a.sale_price * batch_qty - batch_amt) / (a.sale_price * batch_qty),0) as product_profit_rate,
  if(b.purchase_batch_no is not null,(
    (a.sale_price / (1 + tax_rate)) * a.batch_qty - batch_amt_no_tax
  ) / (a.sale_price / (1 + tax_rate) * a.batch_qty),0) as product_no_tax_profit_rate,
  case
    when supplier_type_code = '5' or business_type = 1 then '2'
    when central_purchase_tag = '1' then '1'    
    else '3'
  end as purchase_order_type,
  -- 采购订单类型1 集采采购 2 基地采购 3 其他采购
  '1' as goods_shipped_type,
  -- 商品出库类型1 A进A出 2工厂加工 3其他
  current_timestamp() update_time,
  channel_type_name,
  channel_type_code,
  supplier_type_code,
  supplier_type_name,
  if(b.purchase_batch_no is not null ,1,0)  purchase_crdential_flag,      -- 关联到的采购单号
  1 is_meta_order_flag,            -- 原料是否关联到采购单  
  substr(sdt, 1, 6) smt
from  csx_analyse_tmp.csx_analyse_tmp_source_batch_sale_trans_01 a 
  left join 
(SELECT purchase_batch_no,
        wms_batch_no,
        wms_order_no,
         a.business_type,
         central_purchase_tag,
         source_order_no,
         receive_dc_code,
         supplier_code,
         supplier_name,
         move_type_code,
         goods_code,
         channel_type_name,
         channel_type_code,
         supplier_type_code,
         supplier_type_name,
         sum(a.pur_qty) order_qty,
         sum(a.pur_amt) order_amt
     FROM csx_analyse_tmp.csx_analyse_tmp_source_order_02 a
        where move_type_code='102A'
     group by purchase_batch_no,
         a.business_type,
         source_order_no,
         receive_dc_code,
         supplier_code,
         central_purchase_tag,
         move_type_code,
         supplier_name,
         channel_type_name,
         channel_type_code,
         supplier_type_code,
         supplier_type_name,
         goods_code,
         wms_batch_no,
         wms_order_no
     ) b ON a.wms_batch_no = b.wms_batch_no and a.wms_order_no=b.wms_order_no
         and a.goods_code=b.goods_code
    left join (
    select
      goods_code,
      tax_rate / 100 tax_rate,
      short_name,
      classify_large_code,
      classify_large_name,
      classify_middle_code,
      classify_middle_name,
      classify_small_code,
      classify_small_name,
      division_name
    from
      csx_analyse_tmp.csx_analyse_tmp_goods_short
  ) c on a.goods_code = c.goods_code
  left join csx_analyse_tmp.csx_analyse_tmp_dc_new d on a.dc_code = d.shop_code
  left join csx_analyse_tmp.csx_analyse_tmp_dc_new f on b.receive_dc_code = f.shop_code
--  where b.purchase_batch_no is not null
where purchase_crdential_flag=1

;  



 drop table csx_analyse_tmp.csx_analyse_tmp_wms_change_order_detail; 
 create table  csx_analyse_tmp.csx_analyse_tmp_wms_change_order_detail as 
    select id,
        order_code,
        a.dc_code,
        source_goods_code,
        source_goods_qty,
        -- b.source_order_no,
        b.batch_no as source_batch_no,
        b.credential_no as source_credential_no,
        -- b.move_type_code as ,
        b.qty as source_qty,
        b.amt as source_amt,
        b.price as source_price,
        target_goods_code,
        target_goods_qty,
        -- c.source_order_no as targer_source_order_code,
        c.batch_no as target_batch_no,
        c.credential_no as target_credential_no,
        c.move_type_code ,
        c.qty as target_qty,
        c.amt as target_amt,
        c.price as target_price
    from 
    
    (select id,order_code,
        dc_code,
        change_type,        -- 转换类型 1-子转母 2-母转子 3-等量转换
        source_goods_code,
        source_goods_qty,
        target_goods_code,
        target_goods_qty
    from    csx_dwd.csx_dwd_wms_product_change_order_detail_di  
    where sdt>='${sdate}'
        and sdt<='${edate}'
        and change_type=3  
    ) a 
    left join 
    (select credential_no,
    batch_no,
    goods_code,
    dc_code,
    -- source_order_no,
    link_wms_batch_no wms_batch_no,
    link_wms_order_no wms_order_no,
    move_type_code,
    qty,
    amt,
    price
 from   csx_dwd.csx_dwd_cas_accounting_stock_log_item_di a 
    where sdt>='${sdate}'
        and sdt<='${edate}'
        and move_type_code ='202A'
        -- and direction_flag ='+'
        and in_out_type='CODE_TRANS'
) b on  a.source_goods_code=b.goods_code
    and a.order_code=b.wms_order_no 
      left join 
    (select credential_no,
         batch_no,
         goods_code,
         dc_code,
         -- source_order_no,
         link_wms_batch_no wms_batch_no,
         link_wms_order_no wms_order_no,
         move_type_code,
         qty,
         amt,
         price
 from   csx_dwd.csx_dwd_cas_accounting_stock_log_item_di a 
    where sdt>='${sdate}'
        and sdt<='${edate}'
        and move_type_code ='202A'
        -- and direction_flag ='+'
        and in_out_type='CODE_TRANS'
) c on  a.target_goods_code=c.goods_code
    and a.order_code=c.wms_order_no 
where b.batch_no is not null or c.batch_no is not null 
  
  ;
  

  -- 根据成品批次号查找转码凭证号 

drop table   csx_analyse_tmp.csx_analyse_tmp_wms_change_order_detail_sale_01;
create  table csx_analyse_tmp.csx_analyse_tmp_wms_change_order_detail_sale_01 as 
with 
tmp_source_batch as 
(select sdt,
    region_code,
    region_name,
    province_code,
    province_name,
	city_group_code,
	city_group_name,
    dc_code,
    a.credential_no,
    a.order_code,
    channel_name,
    order_channel_code,
    a.goods_code,
    a.goods_name,
    division_code,
    classify_large_code,
    classify_middle_code,
    classify_small_code,
    sale_price,
    a.sale_cost,
    a.sale_amt,
    a.sale_qty,
    sale_amt_no_tax ,
    sale_cost_no_tax , 
    profit_no_tax ,
    a.profit,
    sum(batch_qty) as batch_qty,
    sum(batch_amt) as batch_amt,
    sum(batch_amt_no_tax) batch_amt_no_tax,
    sum(batch_price) batch_price,
    a.batch_no
     
from csx_analyse_tmp.csx_analyse_tmp_source_batch_sale a 
group by 
    sdt,
    region_code,
    region_name,
    province_code,
    province_name,
	city_group_code,
	city_group_name,
    dc_code,
    a.credential_no,
    a.order_code,
    channel_name,
    order_channel_code,
    a.goods_code,
    a.goods_name,
    division_code,
    classify_large_code,
    classify_middle_code,
    classify_small_code,
    sale_price,
    a.sale_cost,
    a.sale_amt,
    a.sale_qty,
    sale_amt_no_tax ,
    sale_cost_no_tax , 
    profit_no_tax ,
    a.profit,
    batch_no
)
select   sdt,
    region_code,
    region_name,
    province_code,
    province_name,
	city_group_code,
	city_group_name,
    dc_code,
    a.credential_no,
    a.order_code,
    channel_name,
    order_channel_code,
    a.goods_code,
    a.goods_name,
    division_code,
    classify_large_code,
    classify_middle_code,
    classify_small_code,
    sale_price,
    a.sale_cost,
    a.sale_amt,
    a.sale_qty,
    sale_amt_no_tax ,
    sale_cost_no_tax , 
    profit_no_tax ,
    a.profit,
    (batch_qty) as batch_qty,
    (batch_amt) as batch_amt,
    (batch_amt_no_tax) batch_amt_no_tax,
    (batch_price) batch_price,
    a.batch_no,
    transfer_crdential_no,
    transfer_qty,
    transfer_amt,
    transfer_price,
    transfer_amt_no_tax ,
    move_type_code,
    in_out_type,
    b.in_or_out,
    if(b.transfer_crdential_no is not null ,1,0) as transfer_crdential_flag     -- 关联领料凭证标识
 from tmp_source_batch as a 
 left join
(SELECT credential_no as transfer_crdential_no,
       batch_no as transfer_batch_no,
       move_type_code,
       in_out_type,
       goods_code,
       in_or_out,
       sum(qty) as   transfer_qty,
       sum(amt) as   transfer_amt,
       sum(amt_no_tax)  as transfer_amt_no_tax,
       sum(amt)/sum(qty)  as transfer_price
    --   row_number()over(partition by credential_no,batch_no,goods_code order by create_time asc ) rn 
FROM  csx_dwd.csx_dwd_cas_accounting_stock_log_item_di
WHERE sdt>='${sdate}'
      and sdt<='${edate}'
  AND in_out_type='CODE_TRANS'
    group by  credential_no,
       batch_no ,
       move_type_code,
       in_out_type,
       goods_code,
       in_or_out
) b on a.batch_no=b.transfer_batch_no and a.goods_code=b.goods_code
where  1=1
-- and rn =1 
 and b.transfer_batch_no is not null 
-- and  order_code='OM24121100004963'
 ;


-- 查找转码单采购单号
 
 drop table csx_analyse_tmp.csx_analyse_tmp_change_order_sale_detail;;

create table csx_analyse_tmp.csx_analyse_tmp_change_order_sale_detail as 
-- 转码原品
with tmp_change_order as 
(select a.*,b. source_goods_code,
        source_goods_qty,
        source_batch_no,
        source_credential_no,
        -- b.move_type_code as ,
        source_qty,
        source_amt,
        source_price,
        zm_order_code,
        zm_dc_code
        
from csx_analyse_tmp.csx_analyse_tmp_wms_change_order_detail_sale_01 a 
left join 
(select source_goods_code,
        source_goods_qty,
        source_batch_no,
        source_credential_no,
        -- b.move_type_code as ,
        sum(source_qty) as source_qty,
        sum(source_amt) as source_amt,
         sum(source_amt)/sum(source_qty ) as source_price,
        order_code as zm_order_code,
        dc_code as zm_dc_code
    from csx_analyse_tmp.csx_analyse_tmp_wms_change_order_detail 
    
    group by source_goods_code,
        source_goods_qty,
        source_batch_no,
        source_credential_no,
        -- b.move_type_code as ,
        
        order_code,
        dc_code
    )
    
    b on a.goods_code=b.source_goods_code and a.transfer_crdential_no=b.source_credential_no 
 where  in_or_out=1
 )
,
tmp_change_order_01 as 
(select substr(sdt, 1, 6) sale_month,
  sdt as sale_sdt,
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  a.city_group_code,
  a.city_group_name,
  dc_code,
  d.shop_name as dc_name,
  a.credential_no,
  order_code,
  a.goods_code,
  a.goods_name,
  tax_rate,
  division_code,
  c.division_name,
  c.classify_large_code,
  c.classify_large_name,
  c.classify_middle_code,
  c.classify_middle_name,
  c.classify_small_code,
  c.classify_small_name,
  sale_price,
  a.sale_cost,
  a.sale_amt,
  a.sale_qty,
  a.profit,
  sale_amt_no_tax,
  sale_cost_no_tax,
  profit_no_tax,
  a.batch_no,
  batch_price,
  batch_qty,
  batch_amt,
  batch_amt_no_tax,
  if(b.purchase_batch_no is not null,(a.sale_price * batch_qty),0) as batch_sale_amt,
  if(b.purchase_batch_no is not null,(a.sale_price / (1 + tax_rate) * batch_qty),0) as batch_sale_amt_no_tax,
  if(b.purchase_batch_no is not null,(a.sale_price * batch_qty) - batch_amt,0) as batch_profit,
  if(b.purchase_batch_no is not null,(a.sale_price / (1 + tax_rate) * batch_qty) - batch_amt_no_tax,0) as batch_profit_no_tax,
  if(b.purchase_batch_no is not null,(a.sale_price * batch_qty - batch_amt) / (a.sale_price * batch_qty),0) as batch_profit_rate,
  if(b.purchase_batch_no is not null,( a.sale_price / (1 + tax_rate) * batch_qty - batch_amt_no_tax ) / (a.sale_price / (1 + tax_rate) * batch_qty) ,0) as batch_profit_rate_no_tax,  '' as transfer_crdential_no,
  -- 成品凭证单号
  0 as transfer_price,
  0 as transfer_qty,
  0 as transfer_amt,
  0 as transfer_amt_no_tax,
  batch_no as meta_batch_no,
  -- 原料批次成本单号
  a.goods_code as product_code,
  -- 原料商品编码
  a.goods_name as product_name,
  -- 原料商品名称
  short_name,
  tax_rate as product_tax_rate,
  c.classify_large_code as product_classify_large_code,
  c.classify_large_name as product_classify_large_name,
  c.classify_middle_code as product_classify_middle_code,
  c.classify_middle_name as product_classify_middle_name,
  c.classify_small_code as product_classify_small_code,
  c.classify_small_name as product_classify_small_name,
  if(b.purchase_batch_no is not null,batch_qty,0) as meta_qty,
  -- 原料消耗数量
  if(b.purchase_batch_no is not null,batch_amt,0) as meta_amt,
  -- 原料消耗金额
  if(b.purchase_batch_no is not null,batch_amt_no_tax,0) as meta_amt_no_tax,
  -- 原料消耗金额(未税)
  1 as use_ratio,
  -- 原料使用占比
  1 as product_ratio,
  -- 原料工单占比
  b.source_order_no,
  b.receive_dc_code,
  -- 入库DC
  f.shop_name receive_dc_name,
  order_qty,
  order_amt,
  supplier_code,
  supplier_name,
  if(b.purchase_batch_no is not null, a.sale_price * batch_qty,0) as product_sale_amt,
  if(b.purchase_batch_no is not null, (a.sale_price / (1 + tax_rate)) * a.batch_qty,0) as product_sale_amt_no_tax,
  if(b.purchase_batch_no is not null,a.batch_amt,0) as product_cost_amt,
  if(b.purchase_batch_no is not null,a.batch_amt_no_tax,0) as product_cost_amt_no_tax,
  if(b.purchase_batch_no is not null,a.sale_price * batch_qty - batch_amt ,0) as product_profit,
  if(b.purchase_batch_no is not null,(a.sale_price / (1 + tax_rate)) * a.batch_qty - batch_amt_no_tax ,0) as product_profit_no_tax,
  if(b.purchase_batch_no is not null,(a.sale_price * batch_qty - batch_amt) / (a.sale_price * batch_qty),0) as product_profit_rate,
  if(b.purchase_batch_no is not null,(
    (a.sale_price / (1 + tax_rate)) * a.batch_qty - batch_amt_no_tax
  ) / (a.sale_price / (1 + tax_rate) * a.batch_qty),0) as product_no_tax_profit_rate,
  case
    when supplier_type_code = '5' or business_type = 1 then '2'
    when central_purchase_tag = '1' then '1'    
    else '3'
  end as purchase_order_type,
  -- 采购订单类型1 集采采购 2 基地采购 3 其他采购
  '1' as goods_shipped_type,
  -- 商品出库类型1 A进A出 2工厂加工 3其他
  current_timestamp() update_time,
  channel_type_name,
  channel_type_code,
  supplier_type_code,
  supplier_type_name,
  if(b.purchase_batch_no is not null ,1,0)  purchase_crdential_flag,      -- 关联到的采购单号
  1 is_meta_order_flag,            -- 原料是否关联到采购单  
  substr(sdt, 1, 6) smt
  from tmp_change_order a 
left join 
(SELECT purchase_batch_no,
        wms_batch_no,
        wms_order_no,
         a.business_type,
         central_purchase_tag,
         source_order_no,
         receive_dc_code,
         supplier_code,
         supplier_name,
         goods_code,
         channel_type_name,
         channel_type_code,
         supplier_type_code,
         supplier_type_name,
         sum(a.pur_qty) order_qty,
         sum(a.pur_amt) order_amt
     FROM csx_analyse_tmp.csx_analyse_tmp_source_order_02 a
        where move_type_code in ('101A','102A')
     group by purchase_batch_no,
         a.business_type,
         source_order_no,
         receive_dc_code,
         supplier_code,
         central_purchase_tag,
         supplier_name,
         channel_type_name,
         channel_type_code,
         supplier_type_code,
         supplier_type_name,
         goods_code,
         wms_batch_no,
         wms_order_no
     ) b ON a.source_batch_no = b.purchase_batch_no and a.goods_code=b.goods_code and a.batch_no=source_batch_no
    left join (
    select
      goods_code,
      tax_rate / 100 tax_rate,
      short_name,
      classify_large_code,
      classify_large_name,
      classify_middle_code,
      classify_middle_name,
      classify_small_code,
      classify_small_name,
      division_name
    from
      csx_analyse_tmp.csx_analyse_tmp_goods_short
  ) c on a.goods_code = c.goods_code
  left join csx_analyse_tmp.csx_analyse_tmp_dc_new d on a.dc_code = d.shop_code
  left join csx_analyse_tmp.csx_analyse_tmp_dc_new f on b.receive_dc_code = f.shop_code
where 1=1

-- and province_name='北京'
and source_credential_no is not null 
),
-- 转码原品

 tmp_change_order_target
  as 
(select a.*,b.target_goods_code,
        target_batch_no,
        target_credential_no,
        zm_order_code,
        zm_dc_code,
        b.source_batch_no,
        b.source_goods_code
from csx_analyse_tmp.csx_analyse_tmp_wms_change_order_detail_sale_01 a 
left join 
(select 
        target_batch_no,
        source_batch_no,
        source_goods_code,
        target_goods_code,
        target_credential_no,
        order_code as zm_order_code,
        dc_code as zm_dc_code
 from csx_analyse_tmp.csx_analyse_tmp_wms_change_order_detail 
    group by target_batch_no,
        target_credential_no,
        source_batch_no,
        order_code,
        dc_code,
        target_goods_code,
        source_goods_code
    )
    
    b on a.goods_code=b.target_goods_code and a.transfer_crdential_no=b.target_credential_no  and a.batch_no=target_batch_no
    where in_or_out=0
-- left join 
-- (select order_code,goods_code,batch_no from csx_analyse_tmp.csx_analyse_tmp_source_puracse_product_02 )
) ,
-- select * from tmp_change_order a where source_batch_no='CB20241114176592'
-- and source_goods_code='537'
-- and source_batch_no='CB20241114176592'
-- and a.goods_code='537'
-- and a.order_code='OM24111600002107'
tmp_change_order_target_01 as 
(select substr(sdt, 1, 6) sale_month,
  sdt as sale_sdt,
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  a.city_group_code,
  a.city_group_name,
  dc_code,
  d.shop_name as dc_name,
  a.credential_no,
  order_code,
  a.goods_code,
  a.goods_name,
  tax_rate,
  division_code,
  c.division_name,
  c.classify_large_code,
  c.classify_large_name,
  c.classify_middle_code,
  c.classify_middle_name,
  c.classify_small_code,
  c.classify_small_name,
  sale_price,
  a.sale_cost,
  a.sale_amt,
  a.sale_qty,
  a.profit,
  sale_amt_no_tax,
  sale_cost_no_tax,
  profit_no_tax,
  a.batch_no,
  batch_price,
  batch_qty,
  batch_amt,
  batch_amt_no_tax,
  if(b.purchase_batch_no is not null,(a.sale_price * batch_qty),0) as batch_sale_amt,
  if(b.purchase_batch_no is not null,(a.sale_price / (1 + tax_rate) * batch_qty),0) as batch_sale_amt_no_tax,
  if(b.purchase_batch_no is not null,(a.sale_price * batch_qty) - batch_amt,0) as batch_profit,
  if(b.purchase_batch_no is not null,(a.sale_price / (1 + tax_rate) * batch_qty) - batch_amt_no_tax,0) as batch_profit_no_tax,
  if(b.purchase_batch_no is not null,(a.sale_price * batch_qty - batch_amt) / (a.sale_price * batch_qty),0) as batch_profit_rate,
  if(b.purchase_batch_no is not null,( a.sale_price / (1 + tax_rate) * batch_qty - batch_amt_no_tax ) / (a.sale_price / (1 + tax_rate) * batch_qty) ,0) as batch_profit_rate_no_tax,  '' as transfer_crdential_no,
  -- 成品凭证单号
  0 as transfer_price,
  0 as transfer_qty,
  0 as transfer_amt,
  0 as transfer_amt_no_tax,
  batch_no as meta_batch_no,
  -- 原料批次成本单号
  a.source_goods_code as product_code,
  -- 原料商品编码
  c.goods_name as product_name,
  -- 原料商品名称
  short_name,
  tax_rate as product_tax_rate,
  c.classify_large_code as product_classify_large_code,
  c.classify_large_name as product_classify_large_name,
  c.classify_middle_code as product_classify_middle_code,
  c.classify_middle_name as product_classify_middle_name,
  c.classify_small_code as product_classify_small_code,
  c.classify_small_name as product_classify_small_name,
  if(b.purchase_batch_no is not null,batch_qty,0) as meta_qty,
  -- 原料消耗数量
  if(b.purchase_batch_no is not null,batch_amt,0) as meta_amt,
  -- 原料消耗金额
  if(b.purchase_batch_no is not null,batch_amt_no_tax,0) as meta_amt_no_tax,
  -- 原料消耗金额(未税)
  1 as use_ratio,
  -- 原料使用占比
  1 as product_ratio,
  -- 原料工单占比
  b.source_order_no,
  b.receive_dc_code,
  -- 入库DC
  f.shop_name receive_dc_name,
  order_qty,
  order_amt,
  supplier_code,
  supplier_name,
  if(b.purchase_batch_no is not null, a.sale_price * batch_qty,0) as product_sale_amt,
  if(b.purchase_batch_no is not null, (a.sale_price / (1 + tax_rate)) * a.batch_qty,0) as product_sale_amt_no_tax,
  if(b.purchase_batch_no is not null,a.batch_amt,0) as product_cost_amt,
  if(b.purchase_batch_no is not null,a.batch_amt_no_tax,0) as product_cost_amt_no_tax,
  if(b.purchase_batch_no is not null,a.sale_price * batch_qty - batch_amt ,0) as product_profit,
  if(b.purchase_batch_no is not null,(a.sale_price / (1 + tax_rate)) * a.batch_qty - batch_amt_no_tax ,0) as product_profit_no_tax,
  if(b.purchase_batch_no is not null,(a.sale_price * batch_qty - batch_amt) / (a.sale_price * batch_qty),0) as product_profit_rate,
  if(b.purchase_batch_no is not null,(
    (a.sale_price / (1 + tax_rate)) * a.batch_qty - batch_amt_no_tax
  ) / (a.sale_price / (1 + tax_rate) * a.batch_qty),0) as product_no_tax_profit_rate,
  case
    when supplier_type_code = '5' or business_type = 1 then '2'
    when central_purchase_tag = '1' then '1'    
    else '3'
  end as purchase_order_type,
  -- 采购订单类型1 集采采购 2 基地采购 3 其他采购
  '1' as goods_shipped_type,
  -- 商品出库类型1 A进A出 2工厂加工 3其他
  current_timestamp() update_time,
  channel_type_name,
  channel_type_code,
  supplier_type_code,
  supplier_type_name,
  if(b.purchase_batch_no is not null ,1,0)  purchase_crdential_flag,      -- 关联到的采购单号
  1 is_meta_order_flag,            -- 原料是否关联到采购单  
  substr(sdt, 1, 6) smt
  from tmp_change_order_target a 
left join 
(SELECT purchase_batch_no,
        wms_batch_no,
        wms_order_no,
        a.business_type,
        central_purchase_tag,
        source_order_no,
        receive_dc_code,
        supplier_code,
        supplier_name,
        goods_code,
        channel_type_name,
        channel_type_code,
        supplier_type_code,
        supplier_type_name,
        sum(a.pur_qty) order_qty,
        sum(a.pur_amt) order_amt
     FROM csx_analyse_tmp.csx_analyse_tmp_source_order_02 a
        where move_type_code in ('101A','102A')
     group by purchase_batch_no,
         a.business_type,
         source_order_no,
         receive_dc_code,
         supplier_code,
         central_purchase_tag,
         supplier_name,
         channel_type_name,
         channel_type_code,
         supplier_type_code,
         supplier_type_name,
         goods_code,
         wms_batch_no,
         wms_order_no
     ) b ON a.source_batch_no = b.purchase_batch_no and a.source_goods_code=b.goods_code
    left join (
    select
      goods_code,
      goods_name,
      tax_rate / 100 tax_rate,
      short_name,
      classify_large_code,
      classify_large_name,
      classify_middle_code,
      classify_middle_name,
      classify_small_code,
      classify_small_name,
      division_name
    from
      csx_analyse_tmp.csx_analyse_tmp_goods_short
  ) c on a.target_goods_code = c.goods_code
  left join csx_analyse_tmp.csx_analyse_tmp_dc_new d on a.dc_code = d.shop_code
  left join csx_analyse_tmp.csx_analyse_tmp_dc_new f on b.receive_dc_code = f.shop_code
where 1=1 
-- where province_name='北京'
and target_credential_no is not null 
)
select a.* 
from  
(select * from tmp_change_order_01
union all 
select * from tmp_change_order_target_01) a 
left join 
(select order_code,goods_code,batch_no,source_order_no
from
(
select order_code,goods_code,batch_no,source_order_no
from csx_analyse_tmp.csx_analyse_tmp_source_puracse_product_02
union all 
select  order_code,goods_code,batch_no,source_order_no
from csx_analyse_tmp.csx_analyse_tmp_source_batch_sale_trans_02
) a 
group by  order_code,goods_code,batch_no,source_order_no
) b on a.order_code=b.order_code and a.batch_no=b.batch_no and a.goods_code=b.goods_code and a.source_order_no=b.source_order_no
where b.order_code is null 
;


insert overwrite table csx_analyse.csx_analyse_fr_fina_goods_sale_trace_po_di partition(month)

-- drop table csx_analyse_tmp.csx_analyse_tmp_fr_fina_goods_sale_trace_po_di;
-- create table csx_analyse_tmp.csx_analyse_tmp_fr_fina_goods_sale_trace_po_di as
select sale_month
        ,sale_sdt
        ,region_code
        ,region_name
        ,province_code
        ,province_name
        ,city_group_code
        ,city_group_name
        ,dc_code
        ,dc_name
        ,credential_no
        ,order_code
        ,goods_code
        ,goods_name
        ,tax_rate
        ,division_code
        ,division_name
        ,classify_large_code
        ,classify_large_name
        ,classify_middle_code
        ,classify_middle_name
        ,classify_small_code
        ,classify_small_name
        ,sale_price
        ,sale_cost
        ,sale_amt
        ,sale_qty
        ,profit
        ,sale_amt_no_tax
        ,sale_cost_no_tax
        ,profit_no_tax
        ,batch_no
        ,batch_price
        ,batch_qty
        ,batch_amt
        ,batch_amt_no_tax
        ,batch_sale_amt
        ,batch_sale_amt_no_tax
        ,batch_profit
        ,batch_profit_no_tax
        ,batch_profit_rate
        ,batch_profit_rate_no_tax
        ,transfer_crdential_no
        ,transfer_price
        ,transfer_qty
        ,transfer_amt
        ,transfer_amt_no_tax
        ,meta_batch_no
        ,product_code
        ,product_name
        ,short_name
        ,product_tax_rate
        ,product_classify_large_code
        ,product_classify_large_name
        ,product_classify_middle_code
        ,product_classify_middle_name
        ,product_classify_small_code
        ,product_classify_small_name
        ,meta_qty
        ,meta_amt
        ,meta_amt_no_tax
        ,use_ratio
        ,product_ratio
        ,source_order_no
        ,receive_dc_code
        ,receive_dc_name
        ,order_qty
        ,order_amt
        ,supplier_code
        ,supplier_name
        ,product_sale_amt
        ,product_sale_amt_no_tax
        ,product_cost_amt
        ,product_cost_amt_no_tax
        ,product_profit
        ,product_profit_no_tax
        ,product_profit_rate
        ,product_no_tax_profit_rate
        ,purchase_order_type
        ,goods_shipped_type
        ,update_time
        ,channel_type_name
        ,channel_type_code
        ,supplier_type_code
        ,supplier_type_name
        ,purchase_crdential_flag
        ,is_meta_order_flag
      

        ,case
          when source_order_no is null
          and goods_shipped_type = '2' then '采购订单加工商品未关联'
          when source_order_no is null
          and goods_shipped_type = '1' then '采购订单采购商品未关联'
          else '已关联'
        end type_flag,
        smt
 from csx_analyse_tmp.csx_analyse_tmp_source_puracse_product_01
union all 
select  sale_month
        ,sale_sdt
        ,region_code
        ,region_name
        ,province_code
        ,province_name
        ,city_group_code
        ,city_group_name
        ,dc_code
        ,dc_name
        ,credential_no
        ,order_code
        ,goods_code
        ,goods_name
        ,tax_rate
        ,division_code
        ,division_name
        ,classify_large_code
        ,classify_large_name
        ,classify_middle_code
        ,classify_middle_name
        ,classify_small_code
        ,classify_small_name
        ,sale_price
        ,sale_cost
        ,sale_amt
        ,sale_qty
        ,profit
        ,sale_amt_no_tax
        ,sale_cost_no_tax
        ,profit_no_tax
        ,batch_no
        ,batch_price
        ,batch_qty
        ,batch_amt
        ,batch_amt_no_tax
        ,batch_sale_amt
        ,batch_sale_amt_no_tax
        ,batch_profit
        ,batch_profit_no_tax
        ,batch_profit_rate
        ,batch_profit_rate_no_tax
        ,transfer_crdential_no
        ,transfer_price
        ,transfer_qty
        ,transfer_amt
        ,transfer_amt_no_tax
        ,meta_batch_no
        ,product_code
        ,product_name
        ,short_name
        ,product_tax_rate
        ,product_classify_large_code
        ,product_classify_large_name
        ,product_classify_middle_code
        ,product_classify_middle_name
        ,product_classify_small_code
        ,product_classify_small_name
        ,meta_qty
        ,meta_amt
        ,meta_amt_no_tax
        ,use_ratio
        ,product_ratio
        ,source_order_no
        ,receive_dc_code
        ,receive_dc_name
        ,order_qty
        ,order_amt
        ,supplier_code
        ,supplier_name
        ,product_sale_amt
        ,product_sale_amt_no_tax
        ,product_cost_amt
        ,product_cost_amt_no_tax
        ,product_profit
        ,product_profit_no_tax
        ,product_profit_rate
        ,product_no_tax_profit_rate
        ,purchase_order_type
        ,goods_shipped_type
        ,update_time
        ,channel_type_name
        ,channel_type_code
        ,supplier_type_code
        ,supplier_type_name
        ,purchase_crdential_flag
        ,is_meta_order_flag

        ,case
          when source_order_no is null
          and goods_shipped_type = '2' then '采购加工商品未关联'
          when source_order_no is null
          and goods_shipped_type = '1' then '采购商品未关联'
          else '已关联'
        end type_flag,
        smt
 from csx_analyse_tmp.csx_analyse_tmp_source_puracse_product_02
 union all 
 select  sale_month
        ,sale_sdt
        ,region_code
        ,region_name
        ,province_code
        ,province_name
        ,city_group_code
        ,city_group_name
        ,dc_code
        ,dc_name
        ,credential_no
        ,order_code
        ,goods_code
        ,goods_name
        ,tax_rate
        ,division_code
        ,division_name
        ,classify_large_code
        ,classify_large_name
        ,classify_middle_code
        ,classify_middle_name
        ,classify_small_code
        ,classify_small_name
        ,sale_price
        ,sale_cost
        ,sale_amt
        ,sale_qty
        ,profit
        ,sale_amt_no_tax
        ,sale_cost_no_tax
        ,profit_no_tax
        ,batch_no
        ,batch_price
        ,batch_qty
        ,batch_amt
        ,batch_amt_no_tax
        ,batch_sale_amt
        ,batch_sale_amt_no_tax
        ,batch_profit
        ,batch_profit_no_tax
        ,batch_profit_rate
        ,batch_profit_rate_no_tax
        ,transfer_crdential_no
        ,transfer_price
        ,transfer_qty
        ,transfer_amt
        ,transfer_amt_no_tax
        ,meta_batch_no
        ,product_code
        ,product_name
        ,short_name
        ,product_tax_rate
        ,product_classify_large_code
        ,product_classify_large_name
        ,product_classify_middle_code
        ,product_classify_middle_name
        ,product_classify_small_code
        ,product_classify_small_name
        ,meta_qty
        ,meta_amt
        ,meta_amt_no_tax
        ,use_ratio
        ,product_ratio
        ,source_order_no
        ,receive_dc_code
        ,receive_dc_name
        ,order_qty
        ,order_amt
        ,supplier_code
        ,supplier_name
        ,product_sale_amt
        ,product_sale_amt_no_tax
        ,product_cost_amt
        ,product_cost_amt_no_tax
        ,product_profit
        ,product_profit_no_tax
        ,product_profit_rate
        ,product_no_tax_profit_rate
        ,purchase_order_type
        ,goods_shipped_type
        ,update_time
        ,channel_type_name
        ,channel_type_code
        ,supplier_type_code
        ,supplier_type_name
        ,purchase_crdential_flag
        ,is_meta_order_flag

        ,case
          when source_order_no is null
          and goods_shipped_type = '2' then '调拨加工商品未关联'
          when source_order_no is null
          and goods_shipped_type = '1' then '调拨采购商品未关联'
          else '已关联'
        end type_flag,
        smt
 from  csx_analyse_tmp.csx_analyse_tmp_source_batch_sale_trans_02
 union all 
  select  sale_month
        ,sale_sdt
        ,region_code
        ,region_name
        ,province_code
        ,province_name
        ,city_group_code
        ,city_group_name
        ,dc_code
        ,dc_name
        ,credential_no
        ,order_code
        ,goods_code
        ,goods_name
        ,tax_rate
        ,division_code
        ,division_name
        ,classify_large_code
        ,classify_large_name
        ,classify_middle_code
        ,classify_middle_name
        ,classify_small_code
        ,classify_small_name
        ,sale_price
        ,sale_cost
        ,sale_amt
        ,sale_qty
        ,profit
        ,sale_amt_no_tax
        ,sale_cost_no_tax
        ,profit_no_tax
        ,batch_no
        ,batch_price
        ,batch_qty
        ,batch_amt
        ,batch_amt_no_tax
        ,batch_sale_amt
        ,batch_sale_amt_no_tax
        ,batch_profit
        ,batch_profit_no_tax
        ,batch_profit_rate
        ,batch_profit_rate_no_tax
        ,transfer_crdential_no
        ,transfer_price
        ,transfer_qty
        ,transfer_amt
        ,transfer_amt_no_tax
        ,meta_batch_no
        ,product_code
        ,product_name
        ,short_name
        ,product_tax_rate
        ,product_classify_large_code
        ,product_classify_large_name
        ,product_classify_middle_code
        ,product_classify_middle_name
        ,product_classify_small_code
        ,product_classify_small_name
        ,meta_qty
        ,meta_amt
        ,meta_amt_no_tax
        ,use_ratio
        ,product_ratio
        ,source_order_no
        ,receive_dc_code
        ,receive_dc_name
        ,order_qty
        ,order_amt
        ,supplier_code
        ,supplier_name
        ,product_sale_amt
        ,product_sale_amt_no_tax
        ,product_cost_amt
        ,product_cost_amt_no_tax
        ,product_profit
        ,product_profit_no_tax
        ,product_profit_rate
        ,product_no_tax_profit_rate
        ,purchase_order_type
        ,goods_shipped_type
        ,update_time
        ,channel_type_name
        ,channel_type_code
        ,supplier_type_code
        ,supplier_type_name
        ,purchase_crdential_flag
        ,is_meta_order_flag

        ,case
          when source_order_no is null
          and goods_shipped_type = '2' then '转码加工商品未关联'
          when source_order_no is null
          and goods_shipped_type = '1' then '转码采购商品未关联'
          else '已关联'
        end type_flag,
        smt
    from csx_analyse_tmp.csx_analyse_tmp_change_order_sale_detail 
 ;