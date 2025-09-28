-- 预估毛利率根据报价
-- create table csx_analyse_tmp.csx_analyse_tmp_sale_detail as 
with tmp_sale_detail as 
(select
  inventory_dc_code,
  customer_code,
  customer_name,
  sub_customer_code,
  sub_customer_name,
  goods_code,
  a.classify_large_code ,
  a.classify_large_name ,
  a.classify_middle_code, 
  a.classify_middle_name, 
  a.classify_small_code ,
  a.classify_small_name ,
  a.unit_name ,
  delivery_type_name,
  direct_delivery_type,
  order_channel_code,
  refund_order_flag,
  delivery_type_code,
  second_category_name,
  performance_region_name,
  performance_province_name,
  performance_city_name,
  sdt,
  business_type_code,
  business_type_name,
  sale_qty,
  sale_amt,
  sale_cost,
  profit,
  extra as rp_manager_type
from
          csx_dws.csx_dws_sale_detail_di a 
 left  join (
  select
    `code`,
    name,
    extra
  from
    csx_dim.csx_dim_basic_topic_dict_df
  where
    parent_code = 'direct_delivery_type'
    and extra = '采购参与'
) a2 on cast(a.direct_delivery_type as string) = a2.`code`
where
  business_type_code in('1')
  and shipper_code = 'YHCSX'
  and extra='采购参与'
  and performance_province_name in ('广东深圳','广东广州')
  and sdt between '20250901'  and '20250911'
--   and price_type<>2
  and delivery_type_code<>2 
  and order_channel_code not in (4,5,6)
  and refund_order_flag<>1
) 
,
tmp_sale_customer_goods as 
(select performance_region_name,
    performance_province_name,
    performance_city_name,
    inventory_dc_code,
    customer_code,
    customer_name ,
    sub_customer_code,
    sub_customer_name,
    goods_code,
    b.goods_name,
    a.classify_large_code ,
    a.classify_large_name ,
    a.classify_middle_code, 
    a.classify_middle_name, 
    a.classify_small_code ,
    a.classify_small_name ,
    goods_status_name,
    unit_name,
    sum(sale_cost)sale_cost,
    sum(sale_amt) sale_amt,
    sum(sale_qty) sale_qty,
    sum(profit) profit
from tmp_sale_detail   a 
left join 
(select dc_code,
    goods_code,
    goods_name,
    goods_status_name 
  from csx_dim.csx_dim_basic_dc_goods a
  where sdt='current'
  )b on a.goods_code=b.goods_code and a.inventory_dc_code=b.dc_code
  group by performance_region_name,
    performance_province_name,
    performance_city_name,
    inventory_dc_code,
    customer_code,
    customer_name ,
    sub_customer_code,
    sub_customer_name,
    a.goods_code,
    b.goods_name,
    a.classify_large_code ,
    a.classify_large_name ,
    a.classify_middle_code, 
    a.classify_middle_name, 
    a.classify_small_code ,
    a.classify_small_name ,
    goods_status_name,
    unit_name
)

,
csx_analyse_tmp_sale_detail_ky_target_month as 
(select inventory_dc_code,
    customer_code,
    sub_customer_code,
    goods_code,
    cost_price_type,
    sale_price_for,
    cost_price_for,
   case when cost_price_type='下单策略-委外配置' and str_cost_price_type=1 then '售价下浮' else '' end price_type_flag 
from csx_analyse_tmp.csx_analyse_tmp_sale_detail_ky_target_month
) 
-- select * from csx_analyse_tmp_sale_detail_ky_target_month where customer_code='246424' and goods_code='1056283'
,
tmp_csx_analyse_tmp_last_rk_price as 
(select sdt,
    performance_city_name,
    dc_code,
    classify_middle_code,
    classify_middle_name,
    goods_code,
    received_price1,
    pm 
from csx_analyse_tmp.csx_analyse_tmp_last_rk_price
where pm=1 
),
-- --------------------------------------------------------------------------------------------------
-- -------------------------------------
-- ------客户商品目前生效价格
-- drop table if exists csx_analyse_tmp.csx_analyse_tmp_month_now_customer_price_month;
-- create table if not exists csx_analyse_tmp.csx_analyse_tmp_month_now_customer_price_month as 
csx_analyse_tmp_month_now_customer_price_month as 
(select 
    a.* 
from 
    (select 
        warehouse_code as dc_code,
        customer_code,
        product_code as goods_code,
        customer_price,
        (case when price_type=1 then '建议售价' 
             when price_type=2 then '对标对象' 
             when price_type=3 then '销售成本价' 
             when price_type=4 then '上一周价格' 
             when price_type=5 then '售价' 
             when price_type=6 then '采购/库存成本价' 
             when price_type=7 then '上期价格' 
        else price_type end) as price_type,
        row_number()over(partition by warehouse_code,customer_code,product_code order by create_time desc) as pm  
    from csx_dwd.csx_dwd_price_customer_price_guide_di 
    where effective='1' 
    and length(sub_customer_code)=0 
    -- and warehouse_code in ('W0A3','W0R9') 
    ) a 
where a.pm=1 
),
-- --------------------------------------------------------------------------------------------------
-- -------------------------------------
-- ------子客户商品目前生效价格
-- drop table if exists csx_analyse_tmp.csx_analyse_tmp_month_now_customer_price_month_sub;
-- create table if not exists csx_analyse_tmp.csx_analyse_tmp_month_now_customer_price_month_sub as
tmp_csx_analyse_tmp_month_now_customer_price_month_sub as (
select 
    a.* 
from 
    (select 
        warehouse_code as dc_code,
        customer_code,
        sub_customer_code,
        product_code as goods_code,
        customer_price,
        (case when price_type=1 then '建议售价' 
             when price_type=2 then '对标对象' 
             when price_type=3 then '销售成本价' 
             when price_type=4 then '上一周价格' 
             when price_type=5 then '售价' 
             when price_type=6 then '采购/库存成本价' 
             when price_type=7 then '上期价格' 
        else price_type end) as price_type,
        row_number()over(partition by warehouse_code,customer_code,sub_customer_code,product_code order by create_time desc) as pm  
    from   csx_dwd.csx_dwd_price_customer_price_guide_di 
    where effective='1' 
    and length(sub_customer_code)>0 
    ) a 
where a.pm=1 
)
-- and warehouse_code in ('W0A3','W0R9') 
,
tmp_sale_customer_goods_01 as 
(select performance_region_name,
    performance_province_name,
    performance_city_name,
    a.inventory_dc_code,
    a.customer_code,
    a.customer_name ,
    a.sub_customer_code,
    a.sub_customer_name,
    a.goods_code,
    a.goods_name,
    a.classify_large_code ,
    a.classify_large_name ,
    a.classify_middle_code, 
    a.classify_middle_name, 
    a.classify_small_code ,
    a.classify_small_name ,
    goods_status_name,
    unit_name,
    sale_cost,
   (sale_amt) sale_amt,
   (sale_qty) sale_qty,
   (profit) profit,
   (sale_qty)*3 as forecast_sale_qty,   -- 按照3倍预测 
   cost_price_type,
   sale_price_for,
   cost_price_for,
   sale_price_for*(sale_qty)*3 as forecast_sale_amt,
   cost_price_for*(sale_qty)*3 as forecast_sale_cost,
   price_type_flag,
   coalesce(c.price_type,d.price_type) as price_type
from tmp_sale_customer_goods a
left join tmp_csx_analyse_tmp_month_now_customer_price_month_sub c 
on a.customer_code=c.customer_code and a.sub_customer_code=c.sub_customer_code and a.inventory_dc_code=c.dc_code and a.goods_code=c.goods_code
left join csx_analyse_tmp_month_now_customer_price_month d on a.customer_code=d.customer_code and a.inventory_dc_code=d.dc_code and a.goods_code=d.goods_code
left join csx_analyse_tmp_sale_detail_ky_target_month b 
on a.customer_code=b.customer_code and a.goods_code=b.goods_code and a.sub_customer_code=b.sub_customer_code and a.inventory_dc_code=b.inventory_dc_code
)
select a.performance_region_name,
    a.performance_province_name,
    a.performance_city_name,
    a.customer_code,
    a.customer_name ,
    
    -- a.classify_large_code ,
    a.classify_large_name ,
    -- a.classify_middle_code, 
    a.classify_middle_name, 
    -- a.classify_small_code ,
    a.classify_small_name ,
    a.goods_code,
    a.goods_name,
    unit_name,
    goods_status_name,
    
    sum(sale_qty) sale_qty,
    sum(sale_amt) sale_amt,
    sum(profit) profit,
    sum(profit)/sum(sale_amt) as profit_rate,
    sum(forecast_sale_qty)forecast_sale_qty,
    sum(forecast_sale_amt)/sum(forecast_sale_qty) forecast_sale_price,
    concat_ws(',',collect_set(price_type)) as price_type,
    min(b.received_price1) received_price1,
    sum(forecast_sale_amt)forecast_sale_amt,
    sum(forecast_sale_cost) forecast_sale_cost,
    sum(forecast_sale_amt)-sum(forecast_sale_cost) forecast_profit 
from tmp_sale_customer_goods_01 a 
left join tmp_csx_analyse_tmp_last_rk_price b on a.inventory_dc_code=b.dc_code and a.goods_code=b.goods_code 
group by  a.performance_region_name,
    a.performance_province_name,
    a.performance_city_name,
    a.customer_code,
    a.customer_name ,
    a.goods_code,
    a.goods_name,
    -- a.classify_large_code ,
    a.classify_large_name ,
    -- a.classify_middle_code, 
    a.classify_middle_name, 
    -- a.classify_small_code ,
    a.classify_small_name ,
    goods_status_name,
    unit_name

-- 创建表
-- 创建临时表1：销售明细数据
drop table if exists temp_sale_detail;
create   table csx_analyse_tmp.temp_sale_detail as
select
  inventory_dc_code,
  customer_code,
  customer_name,
  sub_customer_code,
  sub_customer_name,
  goods_code,
  a.classify_large_code,
  a.classify_large_name,
  a.classify_middle_code, 
  a.classify_middle_name, 
  a.classify_small_code,
  a.classify_small_name,
  a.unit_name,
  delivery_type_name,
  direct_delivery_type,
  order_channel_code,
  refund_order_flag,
  delivery_type_code,
  second_category_name,
  performance_region_name,
  performance_province_name,
  performance_city_name,
  sdt,
  business_type_code,
  business_type_name,
  sale_qty,
  sale_amt,
  sale_cost,
  profit,
  extra as rp_manager_type
from
  csx_dws.csx_dws_sale_detail_di a 
 left join (
  select
    `code`,
    name,
    extra
  from
    csx_dim.csx_dim_basic_topic_dict_df
  where
    parent_code = 'direct_delivery_type'
    and extra = '采购参与'
) a2 on cast(a.direct_delivery_type as string) = a2.`code`
where
  business_type_code in('1')
  and shipper_code = 'YHCSX'
  and extra='采购参与'
  and performance_province_name in ('广东深圳','广东广州')
  and sdt between '20250901'  and '20250911'
  and delivery_type_code<>2 
  and order_channel_code not in (4,5,6)
  and refund_order_flag<>1;

-- 创建临时表2：客户商品销售汇总
drop table if exists temp_sale_customer_goods;
create   table csx_analyse_tmp.temp_sale_customer_goods as
select 
    performance_region_name,
    performance_province_name,
    performance_city_name,
    inventory_dc_code,
    customer_code,
    customer_name,
    sub_customer_code,
    sub_customer_name,
    a.goods_code,
    b.goods_name,
    a.classify_large_code,
    a.classify_large_name,
    a.classify_middle_code, 
    a.classify_middle_name, 
    a.classify_small_code,
    a.classify_small_name,
    b.goods_status_name,
    unit_name,
    sum(sale_cost) as sale_cost,
    sum(sale_amt) as sale_amt,
    sum(sale_qty) as sale_qty,
    sum(profit) as profit
from csx_analyse_tmp.temp_sale_detail a 
left join 
(
  select 
    dc_code,
    goods_code,
    goods_name,
    goods_status_name 
  from csx_dim.csx_dim_basic_dc_goods 
  where sdt='current'
) b on a.goods_code=b.goods_code and a.inventory_dc_code=b.dc_code
group by 
    performance_region_name,
    performance_province_name,
    performance_city_name,
    inventory_dc_code,
    customer_code,
    customer_name,
    sub_customer_code,
    sub_customer_name,
    a.goods_code,
    b.goods_name,
    a.classify_large_code,
    a.classify_large_name,
    a.classify_middle_code, 
    a.classify_middle_name, 
    a.classify_small_code,
    a.classify_small_name,
    b.goods_status_name,
    unit_name;



-- 创建临时表5：客户当前价格（无子客户）
drop table if exists temp_customer_price;
create   table csx_analyse_tmp.temp_customer_price as
select 
    dc_code,
    customer_code,
    goods_code,
    customer_price,
    price_type
from 
    (
    select 
        warehouse_code as dc_code,
        customer_code,
        product_code as goods_code,
        customer_price,
        (case when price_type=1 then '建议售价' 
             when price_type=2 then '对标对象' 
             when price_type=3 then '销售成本价' 
             when price_type=4 then '上一周价格' 
             when price_type=5 then '售价' 
             when price_type=6 then '采购/库存成本价' 
             when price_type=7 then '上期价格' 
        else price_type end) as price_type,
        row_number() over(partition by warehouse_code, customer_code, product_code order by create_time desc) as pm  
    from csx_dwd.csx_dwd_price_customer_price_guide_di 
    where effective='1' 
    and length(sub_customer_code)=0
) a 
where a.pm=1;

-- 创建临时表6：子客户当前价格
drop table if exists temp_sub_customer_price;
create   table csx_analyse_tmp.temp_sub_customer_price as
select 
    dc_code,
    customer_code,
    sub_customer_code,
    goods_code,
    customer_price,
    price_type
from 
    (
    select 
        warehouse_code as dc_code,
        customer_code,
        sub_customer_code,
        product_code as goods_code,
        customer_price,
        (case when price_type=1 then '建议售价' 
             when price_type=2 then '对标对象' 
             when price_type=3 then '销售成本价' 
             when price_type=4 then '上一周价格' 
             when price_type=5 then '售价' 
             when price_type=6 then '采购/库存成本价' 
             when price_type=7 then '上期价格' 
        else price_type end) as price_type,
        row_number() over(partition by warehouse_code, customer_code, sub_customer_code, product_code order by create_time desc) as pm  
    from csx_dwd.csx_dwd_price_customer_price_guide_di 
    where effective='1' 
    and length(sub_customer_code)>0 
) a 
where a.pm=1;

-- 创建临时表7：客户商品销售数据01
drop table if exists temp_sale_customer_goods_01;
create  table csx_analyse_tmp.temp_sale_customer_goods_01 as
with 
-- -- 创建临时表3：目标月份销售数据
-- drop table if exists temp_ky_target_month;
-- create   table 
temp_ky_target_month as
(select 
    inventory_dc_code,
    customer_code,
    sub_customer_code,
    goods_code,
    cost_price_type,
    sale_price_for,
    cost_price_for,
    case when cost_price_type='下单策略-委外配置' and str_cost_price_type=1 then '售价下浮' else '' end price_type_flag 
from csx_analyse_tmp.csx_analyse_tmp_sale_detail_ky_target_month
-- where customer_code='249799' AND goods_code='1165853'
)
-- SELECT * FROM temp_ky_target_month WHERE goods_code='1483649' AND inventory_dc_code='WD57' AND customer_code='246424'

select 
    performance_region_name,
    performance_province_name,
    performance_city_name,
    a.inventory_dc_code,
    a.customer_code,
    a.customer_name,
    a.sub_customer_code,
    a.sub_customer_name,
    a.goods_code,
    a.goods_name,
    a.classify_large_code,
    a.classify_large_name,
    a.classify_middle_code, 
    a.classify_middle_name, 
    a.classify_small_code,
    a.classify_small_name,
    goods_status_name,
    unit_name,
    sale_cost,
    sale_amt,
    sale_qty,
    profit,
    (sale_qty)*3 as forecast_sale_qty,
    cost_price_type,
    sale_price_for,
    cost_price_for,
    coalesce(sale_price_for,0)*(sale_qty)*3 as forecast_sale_amt,
    coalesce(cost_price_for,0)*(sale_qty)*3 as forecast_sale_cost,
    price_type_flag,
    coalesce(c.price_type, d.price_type) as price_type
from csx_analyse_tmp.temp_sale_customer_goods a
left join csx_analyse_tmp.temp_sub_customer_price c 
    on a.customer_code=c.customer_code 
    and a.sub_customer_code=c.sub_customer_code 
    and a.inventory_dc_code=c.dc_code
    and a.goods_code=c.goods_code
left join csx_analyse_tmp.temp_customer_price d 
    on a.customer_code=d.customer_code 
    and a.inventory_dc_code=d.dc_code
    and a.goods_code=d.goods_code
left join temp_ky_target_month b 
    on a.customer_code=b.customer_code 
    and a.goods_code=b.goods_code 
    and a.sub_customer_code=b.sub_customer_code 
    and a.inventory_dc_code=b.inventory_dc_code
where a.customer_code='252035'
;

-- 最终查询结果
with temp_last_rk_price as
(select 
    sdt,
    performance_city_name,
    dc_code,
    classify_middle_code,
    classify_middle_name,
    goods_code,
    received_price1,
    pm 
from csx_analyse_tmp.csx_analyse_tmp_last_rk_price
where pm=1
) 
-- SELECT * FROM temp_last_rk_price WHERE dc_code='WD57' AND goods_code='1483649'
select 
    a.performance_region_name,
    a.performance_province_name,
    a.performance_city_name,
    a.customer_code,
    a.customer_name,
    
    -- a.classify_large_code,
    a.classify_large_name,
    -- a.classify_middle_code, 
    a.classify_middle_name, 
    -- a.classify_small_code,
    a.classify_small_name,
    a.goods_code,
    a.goods_name,
    unit_name,
    goods_status_name,
    
    sum(sale_qty) sale_qty,
    sum(sale_amt) as sale_amt,
    sum(profit) as profit,
    sum(profit)/sum(sale_amt) as profit_rate,
    sum(forecast_sale_qty) as forecast_sale_qty,
    sum(forecast_sale_amt)/sum(forecast_sale_qty) as forecast_sale_price,
    concat_ws(',', collect_set(price_type)) as price_type,
    min(b.received_price1) as received_price1,
    sum(forecast_sale_amt) as forecast_sale_amt,
    sum(forecast_sale_cost) as forecast_sale_cost,
    sum(forecast_sale_amt)-sum(forecast_sale_cost) as forecast_profit 
from csx_analyse_tmp.temp_sale_customer_goods_01 a 
left join temp_last_rk_price b 
    on a.inventory_dc_code=b.dc_code 
    and a.goods_code=b.goods_code 
group by  
    a.performance_region_name,
    a.performance_province_name,
    a.performance_city_name,
    a.customer_code,
    a.customer_name,
    a.goods_code,
    a.goods_name,
    -- a.classify_large_code,
    a.classify_large_name,
    -- a.classify_middle_code, 
    a.classify_middle_name, 
    -- a.classify_small_code,
    a.classify_small_name,
    goods_status_name,
    unit_name;