-- 同一供应商进价对比
/*
同一供应商同一商品在同一天入库单数取值规则：
1、剔除补救单含正向单
2、指定供应链仓
3、super_class_code = '1'  -- 供应商订单
4、source_type_code in ('1', '10', '19', '23') -- 1-采购导入、10-智能补货（实际上就是委外）、19-日采补货、23-手工创建
5、剔除市场采购及扣点订单 scm_order_items;取deduction_flag为“1”的过滤 
*/
with sku as (
-- 计算同一供应商同一商品的订单数
select  
        dc_code,
        supplier_code,
        goods_code,
        count(distinct purchase_order_code) order_cn
from csx_analyse.csx_analyse_scm_purchase_order_flow_di a
where  sdt='${yes_date}'        -- 关单日期
and remedy_flag <> '1'          -- 剔除补救单
and is_supply_stock_tag = '1'   -- 指定供应链仓
and super_class_code = '1'      -- 供应商订单
and source_type_code in ('1', '10', '19', '23') -- 1-采购导入、10-智能补货（实际上就是委外）、19-日采补货、23-手工创建
-- and goods_code in ('846778', '620')  
and substr(link_order_code,1,2)<>'RO' -- 剔除补救的采购单
group by 
        dc_code,
        goods_code,
        supplier_code
),
rn as (select *,row_number()over(partition by dc_code order by receive_amount desc ) rn from (
select  
        a.sdt,
 --       a.performance_region_code,
        a.performance_region_name,
 --       a.performance_province_code,
        a.performance_province_name,
 --       a.performance_city_code,
        a.performance_city_name,
        a.dc_code,
        a.dc_name,
        -- a.receive_dc_code,
        -- a.settle_dc_code,
        a.supplier_code,
        a.supplier_name,
     --   supplier_classify_name,
        a.goods_code,
        a.goods_name,
        a.classify_middle_name,
        a.div_name,
        a.receive_amt/a.receive_qty avg_cost,
        a.receive_qty,
        a.receive_amt,
        a.max_cost,
        case when max_cost=b.cost then b.purchase_order_code end   max_purch_order,
        case when max_cost=b.cost then b.po_receive_qty end  max_po_receive_qty,
        a.min_cost,
        case when min_cost=c.cost then c.purchase_order_code end min_purch_order,
        case when min_cost=c.cost then c.po_receive_qty end   min_po_receive_qty,
        (a.max_cost- a.min_cost)*(case when max_cost=b.cost then b.po_receive_qty end) receive_amount
from
(select  
        a.sdt,
        a.performance_region_code,
        a.performance_region_name,
        a.performance_province_code,
        a.performance_province_name,
        a.performance_city_code,
        a.performance_city_name,
        a.dc_code,
        a.dc_name,
        a.receive_dc_code,
        a.settle_dc_code,
        a.supplier_code,
        supplier_name,
        a.goods_code,
        goods_name,
        classify_middle_name,
        div_name,
        supplier_classify_name,
        max(cost)max_cost,
        min(cost)min_cost,
        sum(receive_qty)receive_qty,
        sum(receive_amt)receive_amt
from

(select  
        a.sdt,
        a.performance_region_code,
        a.performance_region_name,
        a.performance_province_code,
        a.performance_province_name,
        a.performance_city_code,
        a.performance_city_name,
        a.dc_code,
        a.dc_name,
        a.receive_dc_code,
        a.settle_dc_code,
        purchase_order_code,
        order_code,
        a.supplier_code,
        supplier_name,
        supplier_classify_name,
        a.goods_code,
        goods_name,
        classify_middle_name,
        case when classify_large_code in ('B01','B02','B03') then '生鲜'  else '食百' end div_name,
        if (order_price1 = 0,order_price2,order_price1) as cost,
        receive_qty,
        receive_amt  
from csx_analyse.csx_analyse_scm_purchase_order_flow_di a
join sku on a.goods_code=sku.goods_code and a.dc_code=sku.dc_code and a.supplier_code=sku.supplier_code
where  sdt='${yes_date}'
and remedy_flag <> '1'
and is_supply_stock_tag = '1'
and super_class_code = '1'
and source_type_code in ('1', '10', '19', '23') -- 1-采购导入、10-智能补货（实际上就是委外）、19-日采补货、23-手工创建
-- and goods_code in ('846778', '620')  
and order_cn>1
and supplier_classify_code <>'2'        -- 过滤市场采购
and  if (order_price1 = 0,order_price2,order_price1)<>0
and performance_city_name in('${city}')
and substr(link_order_code,1,2)<>'RO' -- 剔除补救单

)a 
-- where   a.goods_code='1284650'
-- and a.dc_code='W0A3'
group by a.sdt,
        a.performance_region_code,
        a.performance_region_name,
        a.performance_province_code,
        a.performance_province_name,
        a.performance_city_code,
        a.performance_city_name,
        a.dc_code,
        a.dc_name,
        a.receive_dc_code,
        a.settle_dc_code,
        a.supplier_code,
        supplier_name,
        a.goods_code,
        goods_name,
        div_name,
        supplier_classify_name,
        classify_middle_name
having max(cost)<>min(cost)  -- 过滤最高进价与最低进价相同的情况
) a
left join 
(select  
        a.sdt,
        a.dc_code,
        purchase_order_code,
        a.supplier_code,
        a.goods_code,
        if (order_price1 = 0,order_price2,order_price1) as cost,
        sum(receive_qty) po_receive_qty,
        sum(receive_amt) po_receive_amt
from csx_analyse.csx_analyse_scm_purchase_order_flow_di a
where  sdt='${yes_date}'
and remedy_flag <> '1'
and is_supply_stock_tag = '1'
and super_class_code = '1'
and source_type_code in ('1', '10', '19', '23') -- 1-采购导入、10-智能补货（实际上就是委外）、19-日采补货、23-手工创建
-- and a.goods_code='1284650'
-- and a.dc_code='W0A3'
and substr(link_order_code,1,2)<>'RO' -- 剔除补救单
group by  a.sdt,
        a.dc_code,
        purchase_order_code,
        a.supplier_code,
        a.goods_code,
         if (order_price1 = 0,order_price2,order_price1)
)b on a.goods_code=b.goods_code and a.dc_code=b.dc_code and a.supplier_code=b.supplier_code and a.max_cost=b.cost
left join 
(select  
        a.sdt,
        a.dc_code,
        purchase_order_code,
        a.supplier_code,
        a.goods_code,
         if (order_price1 = 0,order_price2,order_price1) as cost,
        sum(receive_qty) po_receive_qty,
        sum(receive_amt) po_receive_amt
from csx_analyse.csx_analyse_scm_purchase_order_flow_di a
where  sdt='${yes_date}'
and remedy_flag <> '1'
and is_supply_stock_tag = '1'
and super_class_code = '1'
and source_type_code in ('1', '10', '19', '23') -- 1-采购导入、10-智能补货（实际上就是委外）、19-日采补货、23-手工创建
-- and a.goods_code='1284650'
-- and a.dc_code='W0A3'
and substr(link_order_code,1,2)<>'RO' -- 剔除补救单

group by  a.sdt,
        a.dc_code,
        purchase_order_code,
        a.supplier_code,
        a.goods_code,
        if (order_price1 = 0,order_price2,order_price1)
)c on a.goods_code=c.goods_code and a.dc_code=c.dc_code and a.supplier_code=c.supplier_code and a.min_cost=c.cost
) a
)
select * from rn where rn<=10
order by rn
-- where row_number()over(partition by dc_code order by receive_amount desc )<=10
; 

