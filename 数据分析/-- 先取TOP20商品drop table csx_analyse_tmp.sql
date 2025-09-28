-- 先取TOP20商品drop table csx_analyse_tmp.csx_analyse_tmp_sale_top20;
create table csx_analyse_tmp.csx_analyse_tmp_sale_top20 as
with aa  as (select
basic_performance_province_code,
  basic_performance_province_name,
--   basic_performance_city_code,
--   basic_performance_city_name,
--  dc_code
--  ,dc_name
 goods_code
 ,goods_name
 ,classify_large_code
 ,classify_large_name
 ,classify_middle_code
 ,classify_middle_name
 ,classify_small_code
 ,classify_small_name
 ,sum(receive_amt)receive_amt
 ,sum(shipped_amt)shipped_amt
 ,sum(receive_qty)receive_qty
 ,sum(shipped_qty)shipped_qty
 ,sum(receive_amt)-sum(shipped_amt) as net_amt
 ,sum(receive_qty)-sum(shipped_qty) as net_qty
from
  csx_analyse.csx_analyse_scm_purchase_order_flow_di a 
 left join 
 (select
  shop_code,
  shop_name,
  basic_performance_province_code,
  basic_performance_province_name,
  basic_performance_city_code,
  basic_performance_city_name
from
  csx_dim.csx_dim_shop
where
  sdt = 'current') b on a.dc_code=b.shop_code
where
  sdt >= '20231113' and sdt<='20231210'
--   and goods_code = '1483562'
--   and dc_code = 'WB38'
  and remedy_flag !='1'
 and direct_delivery_type not in (1,2)
  and source_type_name not in('临时地采','临时加单','直送','紧急采购','城市服务商','联营直送','项目合伙人' )
  and is_supply_stock_tag=1
  group by  
--   dc_code
--  ,dc_name
 goods_code
 ,goods_name
 ,classify_large_code
 ,classify_large_name
 ,classify_middle_code
 ,classify_middle_name
 ,classify_small_code
 ,classify_small_name
  ,basic_performance_province_code,
  basic_performance_province_name
--   basic_performance_city_code,
--   basic_performance_city_name
  )
  select basic_performance_province_code,
  basic_performance_province_name
--   basic_performance_city_code,
--   basic_performance_city_name,
 ,goods_code
 ,goods_name
 ,classify_large_code
 ,classify_large_name
 ,classify_middle_code
 ,classify_middle_name
 ,classify_small_code
 ,classify_small_name
 ,receive_amt
 ,shipped_amt
 ,receive_qty
 ,shipped_qty
 ,net_amt
 ,net_qty
 ,row_number()over(partition by basic_performance_province_name,classify_middle_name order by net_amt desc ) as rn
 from
  (select basic_performance_province_code,
  basic_performance_province_name
--   basic_performance_city_code,
--   basic_performance_city_name,
 ,goods_code
 ,goods_name
 ,classify_large_code
 ,classify_large_name
 ,classify_middle_code
 ,classify_middle_name
 ,classify_small_code
 ,classify_small_name
 ,receive_amt
 ,shipped_amt
 ,receive_qty
 ,shipped_qty
 ,net_amt
 ,net_qty
 ,row_number()over(partition by basic_performance_province_name,classify_middle_name order by net_amt desc ) as rn
 from aa 
 ) a where rn<21;
 -- 近4周的进价成本
 -- 同期未来4周的进价成本

;
with bb as 
(select csx_week,
basic_performance_province_code,
  basic_performance_province_name,
--   basic_performance_city_code,
--   basic_performance_city_name,
--  dc_code
--  ,dc_name
 goods_code
 ,goods_name
 ,classify_large_code
 ,classify_large_name
 ,classify_middle_code
 ,classify_middle_name
 ,classify_small_code
 ,classify_small_name
 ,sum(receive_amt)receive_amt
 ,sum(shipped_amt)shipped_amt
 ,sum(receive_qty)receive_qty
 ,sum(shipped_qty)shipped_qty
 ,sum(receive_amt)-sum(shipped_amt) as net_amt
 ,sum(receive_qty)-sum(shipped_qty) as net_qty
 ,(sum(receive_amt)-sum(shipped_amt))/sum(receive_qty)-sum(shipped_qty) as avg_price
from
  csx_analyse.csx_analyse_scm_purchase_order_flow_di a 
 left join 
 (select
  shop_code,
  shop_name,
  basic_performance_province_code,
  basic_performance_province_name,
  basic_performance_city_code,
  basic_performance_city_name
from
  csx_dim.csx_dim_shop
where
  sdt = 'current') b on a.dc_code=b.shop_code
  left join (select * from csx_dim.csx_dim_basic_date ) c on a.sdt=c.calday
where
  sdt >= '20231101' and sdt<='20231130'
--   and goods_code = '1483562'
--   and dc_code = 'WB38'
  and remedy_flag !='1'
 and direct_delivery_type not in (1,2)
  and source_type_name not in('临时地采','临时加单','直送','紧急采购','城市服务商','联营直送','项目合伙人' )
  and is_supply_stock_tag=1
  group by csx_week, 
--   dc_code
--  ,dc_name
 goods_code
 ,goods_name
 ,classify_large_code
 ,classify_large_name
 ,classify_middle_code
 ,classify_middle_name
 ,classify_small_code
 ,classify_small_name
  ,basic_performance_province_code,
  basic_performance_province_name
--   basic_performance_city_code,
--   basic_performance_city_name
 ) 
 select *,lag(avg_price,1,0)over(partition by basic_performance_province_name,goods_name order by csx_week) as ratio from bb where basic_performance_province_name='江西' and classify_middle_name='食用油类'
 ;


-- 本期环比
 ;
drop table csx_analyse_tmp.csx_analyse_tmp_sale_ring ;
create table csx_analyse_tmp.csx_analyse_tmp_sale_ring as
with bb as 
(select csx_week,
basic_performance_province_code,
  basic_performance_province_name,
--   basic_performance_city_code,
--   basic_performance_city_name,
--  dc_code
--  ,dc_name
 goods_code
 ,goods_name
 ,classify_large_code
 ,classify_large_name
 ,classify_middle_code
 ,classify_middle_name
 ,classify_small_code
 ,classify_small_name
 ,sum(receive_amt)receive_amt
 ,sum(shipped_amt)shipped_amt
 ,sum(receive_qty)receive_qty
 ,sum(shipped_qty)shipped_qty
 ,sum(receive_amt)-sum(shipped_amt) as net_amt
 ,sum(receive_qty)-sum(shipped_qty) as net_qty
 ,(sum(receive_amt)-sum(shipped_amt))/sum(receive_qty)-sum(shipped_qty) as avg_price
from
  csx_analyse.csx_analyse_scm_purchase_order_flow_di a 
 left join 
 (select
  shop_code,
  shop_name,
  basic_performance_province_code,
  basic_performance_province_name,
  basic_performance_city_code,
  basic_performance_city_name
from
  csx_dim.csx_dim_shop
where
  sdt = 'current') b on a.dc_code=b.shop_code
  left join (select * from csx_dim.csx_dim_basic_date ) c on a.sdt=c.calday
where
  sdt >= '20231113' and sdt<='20231210'
--   and goods_code = '1483562'
--   and dc_code = 'WB38'
  and remedy_flag !='1'
 and direct_delivery_type not in (1,2)
  and source_type_name not in('临时地采','临时加单','直送','紧急采购','城市服务商','联营直送','项目合伙人' )
  and is_supply_stock_tag=1
  group by csx_week, 
--   dc_code
--  ,dc_name
 goods_code
 ,goods_name
 ,classify_large_code
 ,classify_large_name
 ,classify_middle_code
 ,classify_middle_name
 ,classify_small_code
 ,classify_small_name
  ,basic_performance_province_code,
  basic_performance_province_name
--   basic_performance_city_code,
--   basic_performance_city_name
 ) 
  select csx_week,
basic_performance_province_code,
  basic_performance_province_name,
--   basic_performance_city_code,
--   basic_performance_city_name,
--  dc_code
--  ,dc_name
 goods_code
 ,goods_name
 ,classify_large_code
 ,classify_large_name
 ,classify_middle_code
 ,classify_middle_name
 ,classify_small_code
 ,classify_small_name
 ,net_amt
,net_qty
,avg_price
, ratio
 ,  note
 ,case when note>0 then row_number()over(partition by basic_performance_province_name,goods_name,note  order by note ) else 0 end as rn 
 from (
 select csx_week,
basic_performance_province_code,
  basic_performance_province_name,
--   basic_performance_city_code,
--   basic_performance_city_name,
--  dc_code
--  ,dc_name
 goods_code
 ,goods_name
 ,classify_large_code
 ,classify_large_name
 ,classify_middle_code
 ,classify_middle_name
 ,classify_small_code
 ,classify_small_name
 ,net_amt
,net_qty
,avg_price
,if(ratio=0,1,avg_price/ratio) ratio
 ,case when if(ratio=0,1,avg_price/ratio)>1 then 1 else 0 end note
 ,row_number()over(partition by basic_performance_province_name,goods_name order by if(ratio=0,1,avg_price/ratio)) as rn 
 from (
 select csx_week,
  basic_performance_province_code,
  basic_performance_province_name,
--   basic_performance_city_code,
--   basic_performance_city_name,
--  dc_code
--  ,dc_name
 goods_code
 ,goods_name
 ,classify_large_code
 ,classify_large_name
 ,classify_middle_code
 ,classify_middle_name
 ,classify_small_code
 ,classify_small_name
 ,net_amt
,net_qty
,avg_price
 ,lag(avg_price,1,0)over(partition by basic_performance_province_name,goods_name order by csx_week) as ratio 
 from bb
 where 1=1
 -- basic_performance_province_name='江西' 
-- and classify_middle_name='食用油类'
 ) a 
 ) a 
 ;

 -- 同期成本环比
drop table csx_analyse_tmp.csx_analyse_tmp_sale_tq ;
create table csx_analyse_tmp.csx_analyse_tmp_sale_tq as
with bb as 
(select csx_week,
basic_performance_province_code,
  basic_performance_province_name,
--   basic_performance_city_code,
--   basic_performance_city_name,
--  dc_code
--  ,dc_name
 goods_code
 ,goods_name
 ,classify_large_code
 ,classify_large_name
 ,classify_middle_code
 ,classify_middle_name
 ,classify_small_code
 ,classify_small_name
 ,sum(receive_amt)receive_amt
 ,sum(shipped_amt)shipped_amt
 ,sum(receive_qty)receive_qty
 ,sum(shipped_qty)shipped_qty
 ,sum(receive_amt)-sum(shipped_amt) as net_amt
 ,sum(receive_qty)-sum(shipped_qty) as net_qty
 ,(sum(receive_amt)-sum(shipped_amt))/sum(receive_qty)-sum(shipped_qty) as avg_price
from
  csx_analyse.csx_analyse_scm_purchase_order_flow_di a 
 left join 
 (select
  shop_code,
  shop_name,
  basic_performance_province_code,
  basic_performance_province_name,
  basic_performance_city_code,
  basic_performance_city_name
from
  csx_dim.csx_dim_shop
where
  sdt = 'current') b on a.dc_code=b.shop_code
  left join (select * from csx_dim.csx_dim_basic_date ) c on a.sdt=c.calday
where
  sdt >= '20221212' and sdt<='20230108'
--   and goods_code = '1483562'
--   and dc_code = 'WB38'
  and remedy_flag !='1'
 and direct_delivery_type not in (1,2)
  and source_type_name not in('临时地采','临时加单','直送','紧急采购','城市服务商','联营直送','项目合伙人' )
  and is_supply_stock_tag=1
  group by csx_week, 
--   dc_code
--  ,dc_name
 goods_code
 ,goods_name
 ,classify_large_code
 ,classify_large_name
 ,classify_middle_code
 ,classify_middle_name
 ,classify_small_code
 ,classify_small_name
  ,basic_performance_province_code,
  basic_performance_province_name
--   basic_performance_city_code,
--   basic_performance_city_name
 ) 
  select csx_week,
basic_performance_province_code,
  basic_performance_province_name,
--   basic_performance_city_code,
--   basic_performance_city_name,
--  dc_code
--  ,dc_name
 goods_code
 ,goods_name
 ,classify_large_code
 ,classify_large_name
 ,classify_middle_code
 ,classify_middle_name
 ,classify_small_code
 ,classify_small_name
 ,net_amt
,net_qty
,avg_price
, ratio
 ,  note
 ,case when note>0 then row_number()over(partition by basic_performance_province_name,goods_name,note  order by note ) else 0 end as rn 
 from (
 select csx_week,
basic_performance_province_code,
  basic_performance_province_name,
--   basic_performance_city_code,
--   basic_performance_city_name,
--  dc_code
--  ,dc_name
 goods_code
 ,goods_name
 ,classify_large_code
 ,classify_large_name
 ,classify_middle_code
 ,classify_middle_name
 ,classify_small_code
 ,classify_small_name
 ,net_amt
,net_qty
,avg_price
,if(ratio=0,1,avg_price/ratio) ratio
 ,case when if(ratio=0,1,avg_price/ratio)>1 then 1 else 0 end note
 ,row_number()over(partition by basic_performance_province_name,goods_name order by if(ratio=0,1,avg_price/ratio)) as rn 
 from (
 select csx_week,
  basic_performance_province_code,
  basic_performance_province_name,
--   basic_performance_city_code,
--   basic_performance_city_name,
--  dc_code
--  ,dc_name
 goods_code
 ,goods_name
 ,classify_large_code
 ,classify_large_name
 ,classify_middle_code
 ,classify_middle_name
 ,classify_small_code
 ,classify_small_name
 ,net_amt
,net_qty
,avg_price
 ,lag(avg_price,1,0)over(partition by basic_performance_province_name,goods_name order by csx_week) as ratio 
 from bb
 where 1=1
 -- basic_performance_province_name='江西' 
-- and classify_middle_name='食用油类'
 ) a 
 ) a 
  ;

    
  select a.basic_performance_province_code,
  a.basic_performance_province_name
--   basic_performance_city_code,
--   basic_performance_city_name,
 ,a.goods_code
 ,goods_name
 ,classify_large_code
 ,classify_large_name
 ,classify_middle_code
 ,classify_middle_name
 ,classify_small_code
 ,classify_small_name
 ,receive_amt
 ,shipped_amt
 ,receive_qty
 ,shipped_qty
 ,net_amt
 ,net_qty
 ,a.rn
 ,stock_qty
,stock_amt
 ,b.rn weeks
 ,c.rn tq_weeks
 ,hq_avg_price
 ,tq_avg_price
 from 
 csx_analyse_tmp.csx_analyse_tmp_sale_top20 a 
 left join 
 (select 
basic_performance_province_code,
  basic_performance_province_name,
--   basic_performance_city_code,
--   basic_performance_city_name,
--  dc_code
--  ,dc_name
 goods_code
 -- ,goods_name
 ,max(rn)  rn
 ,max(avg_price) as hq_avg_price
 from  csx_analyse_tmp.csx_analyse_tmp_sale_ring 
 group by basic_performance_province_code,
  basic_performance_province_name,
--   basic_performance_city_code,
--   basic_performance_city_name,
--  dc_code
--  ,dc_name
 goods_code ) b on a.goods_code=b.goods_code and a.basic_performance_province_name=b.basic_performance_province_name
 left join 
  (select 
basic_performance_province_code,
  basic_performance_province_name,
--   basic_performance_city_code,
--   basic_performance_city_name,
--  dc_code
--  ,dc_name
 goods_code
 -- ,goods_name
 ,max(rn)  rn
 ,max(avg_price) as tq_avg_price
 from  csx_analyse_tmp.csx_analyse_tmp_sale_tq 
 group by basic_performance_province_code,
  basic_performance_province_name,
--   basic_performance_city_code,
--   basic_performance_city_name,
--  dc_code
--  ,dc_nam
 goods_code ) c on a.goods_code=c.goods_code and a.basic_performance_province_name=c.basic_performance_province_name
 left join 
 (select
basic_performance_province_code,
  basic_performance_province_name,
  goods_code,
--  dc_code,
  sum(qty) stock_qty,
  sum(amt) stock_amt
from
  csx_dws.csx_dws_cas_accounting_stock_m_df a 
  join 
  (select shop_code,
  shop_name,
  basic_performance_province_code,
  basic_performance_province_name,
  basic_performance_city_code,
  basic_performance_city_name
  from csx_dim.csx_dim_shop a 
   join csx_dim.csx_dim_csx_data_market_conf_supplychain_location b on a.shop_code=b.dc_code
  where a.sdt='current') b on a.dc_code=b.shop_code
where
  sdt = '20231206'
  and reservoir_area_code not in ('PD01', 'PD02', 'TH01', 'TS01')
  AND IS_BZ_RESERVOIR = 1
GROUP BY
basic_performance_province_code,
  basic_performance_province_name,
  goods_code) d on a.basic_performance_province_name=d.basic_performance_province_name and a.goods_code=d.goods_code