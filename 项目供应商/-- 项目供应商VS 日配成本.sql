-- 项目供应商VS 日配成本
with city as (select  a.performance_province_name,
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
        receive_amt,
        source_type_code,
        original_order_code,
        link_order_code,
        agreement_order_no
from csx_analyse.csx_analyse_scm_purchase_order_flow_di a
where sdt >= '20240501'
      and sdt <= '20240516'
      and remedy_flag <> '1'                           -- 剔除补救单 
     -- and is_supply_stock_tag = '1'                    -- 指定供应链仓 
      and super_class_code = '1'                       -- 供应商订单 
      and purpose_name='合伙人物流'
    --  and source_type_code in ('1', '19', '23')  -- 1-采购导入、10-智能补货（实际上就是委外）、19-日采补货、23-手工创建
      and source_type_code in ('18','4')
    -- and goods_code in ('846778', '620') 
    --  and substr(link_order_code, 1, 2) <> 'RO'        -- 剔除补救的采购单
      and performance_province_name='福建'
      ),
rp as (select  a.performance_province_name,
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
        receive_amt,
        source_type_code,
        original_order_code,
        link_order_code,
        agreement_order_no
from csx_analyse.csx_analyse_scm_purchase_order_flow_di a
where sdt >= '20240401'
      and sdt <= '20240501'
      and remedy_flag <> '1'                           -- 剔除补救单 
      and is_supply_stock_tag = '1'                    -- 指定供应链仓 
      and super_class_code = '1'                       -- 供应商订单 
    --  and purpose_name='合伙人物流'
      and source_type_code in ('1', '19', '23')  -- 1-采购导入、10-智能补货（实际上就是委外）、19-日采补货、23-手工创建
    --  and source_type_code in ('18','4')
    -- and goods_code in ('846778', '620') 
    --  and substr(link_order_code, 1, 2) <> 'RO'        -- 剔除补救的采购单
      and performance_province_name='福建'
      ),

jg as (select a.*,
    b.rp_price,
    b.rp_qty,
    rp_price-city_price as diff_price,
    if(rp_price/city_price-1>0.05,1,0)note_type,
    rp_price/city_price-1 as ratio
from 
(select performance_city_name,
    goods_code,
    goods_name,
    div_name,
    classify_middle_name,
    sum(receive_amt)/sum(receive_qty) as city_price,
    sum(receive_qty) as city_qty
from city a 
    group by 
       performance_city_name,
       goods_code,
       goods_name,
       div_name,
       classify_middle_name
) a 
join 
(select performance_city_name,
    goods_code,
    sum(receive_amt)/sum(receive_qty) as rp_price,
    sum(receive_qty) as rp_qty
from rp a 
    group by 
       performance_city_name,
    goods_code
) b on a.performance_city_name=b.performance_city_name and a.goods_code=b.goods_code
)
select * from jg where ratio<0.5 and note_type=1     
      

-- 项目供应商
with city as (select  a.performance_province_name,
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
        receive_amt,
        source_type_code,
        original_order_code,
        link_order_code,
        agreement_order_no
from csx_analyse.csx_analyse_scm_purchase_order_flow_di a
where sdt >= '20240501'
      and sdt <= '20240516'
      and remedy_flag <> '1'                           -- 剔除补救单 
     -- and is_supply_stock_tag = '1'                    -- 指定供应链仓 
      and super_class_code = '1'                       -- 供应商订单 
      and purpose_name='合伙人物流'
    --  and source_type_code in ('1', '19', '23')  -- 1-采购导入、10-智能补货（实际上就是委外）、19-日采补货、23-手工创建
      and source_type_code in ('18','4')
    -- and goods_code in ('846778', '620') 
      and substr(link_order_code, 1, 2) <> 'RO'        -- 剔除补救的采购单
      and performance_province_name='福建'
      ),
rp as (select  a.performance_province_name,
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
        receive_amt,
        source_type_code,
        original_order_code,
        link_order_code,
        agreement_order_no
from csx_analyse.csx_analyse_scm_purchase_order_flow_di a
where sdt >= '20240401'
      and sdt <= '20240501'
      and remedy_flag <> '1'                           -- 剔除补救单 
      and is_supply_stock_tag = '1'                    -- 指定供应链仓 
      and super_class_code = '1'                       -- 供应商订单 
    --  and purpose_name='合伙人物流'
      and source_type_code in ('1', '19', '23')  -- 1-采购导入、10-智能补货（实际上就是委外）、19-日采补货、23-手工创建
    --  and source_type_code in ('18','4')
    -- and goods_code in ('846778', '620') 
      and substr(link_order_code, 1, 2) <> 'RO'        -- 剔除补救的采购单
      and performance_province_name='福建'
      )
select a.*,
    b.rp_price,
    b.rp_qty,
    rp_price-city_price as diff_price,
    if(rp_price/city_price-1>0.05,1,0)note_type,
    rp_price/city_price-1 as ratio
from 
(select performance_city_name,
    goods_code,
    goods_name,
    div_name,
    classify_middle_name,
    sum(receive_amt)/sum(receive_qty) as city_price,
    receive_qty as city_qty
from city a 
    group by 
       performance_city_name,
       goods_code,
       goods_name,
       div_name,
       classify_middle_name
) a 
join 
(select performance_city_name,
    goods_code,
    sum(receive_amt)/sum(receive_qty) as rp_price,
    receive_qty as rp_qty
from rp a 
    group by 
       performance_city_name,
    goods_code
) b on a.performance_city_name=b.performance_city_name and a.goods_code=b.goods_code
      
      

-- 项目供应商商品
WITH sale as (
  select
    performance_city_name,
    goods_code,
    goods_name,
    case
      when classify_large_code in ('B01', 'B02', 'B03') then '生鲜'
      else '食百'
    end div_name,
    classify_middle_name,
    sum(sale_cost) as sale_cost,
    sum(sale_qty) as sale_qty,
    sum(sale_amt) as sale_amt,
    sum(profit) as profit
  from
    csx_dws.csx_dws_sale_detail_di a
    left JOIN (
      select
        shop_code
      from
        csx_dim.csx_dim_shop
      where
        sdt = 'current'
        and shop_low_profit_flag = 0
    ) b on a.inventory_dc_code = b.shop_code
  where
    sdt >= '20240501'
    and sdt <= '20240521'
    and business_type_code = 4 -- and inventory_dc_code not in ('W0J2', 'W0AJ', 'W0G6', 'WB71')
  group by
    performance_city_name,
    goods_code,
    goods_name,
    case
      when classify_large_code in ('B01', 'B02', 'B03') then '生鲜'
      else '食百'
    end,
    classify_middle_name
)
select
  *
from
  (
    select
      *,
      row_number() over(
        partition by performance_city_name,
        div_name,
        classify_middle_name
        order by
          sale_amt desc
      ) as rank
    from
      sale
  ) a
where
  performance_city_name = '福州市'

  -- 日配供应商
  WITH sale as (
  select
    performance_city_name,
    goods_code,
    goods_bar_code,
    goods_name,
    case
      when classify_large_code in ('B01', 'B02', 'B03') then '生鲜'
      else '食百'
    end div_name,
    classify_middle_name,
    sum(sale_cost) as sale_cost,
    sum(sale_qty) as sale_qty,
    sum(sale_amt) as sale_amt,
    sum(profit) as profit
  from
    csx_dws.csx_dws_sale_detail_di a
    left JOIN (
      select
        shop_code,
        shop_low_profit_flag
      from
        csx_dim.csx_dim_shop
      where
        sdt = 'current' --   and shop_low_profit_flag = 0
    ) b on a.inventory_dc_code = b.shop_code
  where
    sdt >= '20240401'
    and sdt <= '20240430'
    and business_type_code = 1
    and shop_low_profit_flag = 0
    and delivery_type_code <> 2
    and inventory_dc_code not in ('W0J2', 'W0AJ', 'W0G6', 'WB71')
  group by
    performance_city_name,
    goods_code,
    goods_bar_code,
    goods_name,
    case
      when classify_large_code in ('B01', 'B02', 'B03') then '生鲜'
      else '食百'
    end,
    classify_middle_name
)
select
  performance_city_name,
  goods_code,
  goods_bar_code,
  goods_name,
  div_name,
  classify_middle_name,
  sale_cost / sale_qty as avg_cost,
  sale_cost,
  sale_qty,
  sale_amt,
  profit,
  rank,
  rank_div
from
  (
    select
      *,
      row_number() over(
        partition by performance_city_name,
        div_name,
        classify_middle_name
        order by
          sale_amt desc
      ) as rank,
      row_number() over(
        partition by performance_city_name,
        div_name
        order by
          sale_amt desc
      ) as rank_div
    from
      sale
  ) a
where
  performance_city_name = '福州市'


  
-- 项目供应商销售商品TOP
WITH sale as (
  select
    performance_city_name,
    goods_code,
    goods_name,
    case
      when classify_large_code in ('B01', 'B02', 'B03') then '生鲜'
      else '食百'
    end div_name,
    classify_middle_name,
    sum(sale_cost) as sale_cost,
    sum(sale_qty) as sale_qty,
    sum(sale_amt) as sale_amt,
    sum(profit) as profit
  from
    csx_dws.csx_dws_sale_detail_di a
    left JOIN (
      select
        shop_code
      from
        csx_dim.csx_dim_shop
      where
        sdt = 'current'
        and shop_low_profit_flag = 0
    ) b on a.inventory_dc_code = b.shop_code
  where
    sdt >= '20240401'
    and sdt <= '20240430'
    and customer_code='129941'
  --  and business_type_code = 4 -- and inventory_dc_code not in ('W0J2', 'W0AJ', 'W0G6', 'WB71')
  group by
    performance_city_name,
    goods_code,
    goods_name,
    case
      when classify_large_code in ('B01', 'B02', 'B03') then '生鲜'
      else '食百'
    end,
    classify_middle_name
)
select
  *
from
  (
    select
      *,
      row_number() over(
        partition by performance_city_name,
        div_name,
        classify_middle_name
        order by
          sale_amt desc
      ) as rank
    from
      sale
  ) a
--where
--  performance_city_name = '福州市'
