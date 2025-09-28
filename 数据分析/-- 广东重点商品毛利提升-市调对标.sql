-- 广东重点商品毛利提升-市调对标
-- drop table  csx_analyse_tmp.csx_analyse_tmp_gd_sale30_top ;
create table csx_analyse_tmp.csx_analyse_tmp_gd_sale30_top as 
with sale as 
 (select
    --   substr(sdt,1,6) csx_week,
    --   performance_region_name,
    --   performance_province_code,
    --   performance_province_name,
    --   performance_city_code,
    --   performance_city_name,
      csx_week,
      sdt,
      basic_performance_province_code,
      basic_performance_province_name,
      basic_performance_city_code,
      basic_performance_city_name,
      a.goods_code,
      b.goods_name,
      b.div_name,
      b.classify_large_code,
      b.classify_large_name,
      b.classify_middle_code,
      b.classify_middle_name,
      b.classify_small_code,
      b.classify_small_name,
      b.unit_name,
      sum(sale_qty) sale_qty,
      sum(sale_amt) sale_amt,
      sum(profit) profit,
      sum(sale_cost) sale_cost
from
      csx_analyse.csx_analyse_bi_sale_detail_di a
      left join 
      (
    select
      shop_code,
      shop_name,
      basic_performance_province_code,
      basic_performance_province_name,
      basic_performance_city_code,
      basic_performance_city_name
    from
      csx_dim.csx_dim_shop
    where
      sdt = 'current'
    )c on a.inventory_dc_code=c.shop_code
      join 
      (select goods_code,goods_name,
            case when classify_large_code in ('B01','B02','B03') THEN'生鲜' else '食百' end div_name,
            classify_large_code,
            classify_large_name,
            classify_middle_code,
            classify_middle_name,
            classify_small_code,
            classify_small_name,
            unit_name
       from csx_dim.csx_dim_basic_goods where sdt='current') b on a.goods_code=b.goods_code
    where

     -- sdt >= '20230901'
     -- and sdt <= '20240411'
      sdt>=  regexp_replace(date_add(from_unixtime(unix_timestamp('20240415','yyyyMMdd'),'yyyy-MM-dd'),-28-dayofweek(from_unixtime(unix_timestamp('20240415','yyyyMMdd'),'yyyy-MM-dd'))),'-','')
      and sdt<='20240415'
    
      and business_type_code = 1
      and shop_low_profit_flag = 0
      and direct_delivery_type_code not in (1, 2, 11, 12) --  'R直送1'  2  'Z直送2'11  '临时加单'12  '紧急补货'
      and order_channel_code not in ('6', '4') -- 6 '调价''4''返利'
      and refund_order_flag = 0 -- 剔除退货
      and performance_region_name like '%大区%'
      and performance_province_name='广东深圳'
group by 
    -- substr(sdt,1,6) ,
    --   performance_region_name,
    --   performance_province_code,
    --   performance_province_name,
    --   performance_city_code,
    --   performance_city_name,
      a.goods_code, 
      b.goods_name,
      b.div_name,
      b.classify_large_code,
      b.classify_large_name,
      b.classify_middle_code,
      b.classify_middle_name,
      b.classify_small_code,
      b.classify_small_name,
      b.unit_name,
      basic_performance_province_code,
      basic_performance_province_name,
      basic_performance_city_code,
      basic_performance_city_name,
      csx_week,
      sdt
)
select * from (
select *
 ,lead(csx_week,1,'202404')over(partition by basic_performance_province_name,basic_performance_city_name,classify_middle_code,goods_code order by csx_week) lead_month
 ,lag(csx_week,1,'')over(partition by basic_performance_province_name,basic_performance_city_name,classify_middle_code,goods_code order by csx_week) lag_month

from (
select 
    --   csx_week,
    --   performance_region_name,
    --   performance_province_code,
    --   performance_province_name,
    --   performance_city_code,
    --   performance_city_name,
      csx_week,
      basic_performance_province_code,
      basic_performance_province_name,
      basic_performance_city_code,
      basic_performance_city_name,
      goods_code,
      goods_name,
      div_name,
      classify_large_code,
      classify_large_name,
      classify_middle_code,
      classify_middle_name,
      classify_small_code,
      classify_small_name,
      unit_name,
      sale_qty,
      sale_amt,
      profit,
      sale_cost,
    sum(sale_qty)over(partition by basic_performance_province_name,basic_performance_city_name,classify_middle_code,goods_code) all_sale_qty,
    count(csx_week)over(partition by basic_performance_province_name,basic_performance_city_name,goods_code) csx_week_num,
    dense_rank()over(partition by basic_performance_province_name,basic_performance_city_name,classify_middle_code,csx_week order by sale_amt desc ) as rn
from sale 
) a
) a 
-- where rn<21
;


-- ----------------------------------------------
-- 计算市调价中间表；
-- drop table if exists csx_analyse_tmp.last_4_week_avg_shop_price_tmp_tmp; 
-- 
create table if not exists csx_analyse_tmp.last_4_week_avg_shop_price_tmp_tmp as 
select 
    c2.location_code,
    c4.classify_large_code,
    c4.classify_large_name,
    c4.classify_middle_code,
    c4.classify_middle_name,
    c2.product_code,
    c1.shop_code,
    c1.shop_name,
    (case when c1.market_source_type_code=1 then '永辉' 
          when c1.market_source_type_code=4 then '一批' 
          when c1.market_source_type_code=5 then '二批' 
          when c1.market_source_type_code=6 then '终端'  
    end) as market_source_type_name,
    c1.market_research_price,
    regexp_replace(substr(c1.price_begin_time,1,10),'-','') as price_begin_date,
    regexp_replace(substr(c1.price_end_time_new,1,10),'-','') as price_end_date 
from 
    (-- 目前失效数据数据
    select 
        a1.product_id as market_goods_id,
        a1.source_type_code as market_source_type_code,
        a1.shop_code,
        a1.shop_name,
        cast(a1.price as decimal(20,6)) as market_research_price,
        a1.price_begin_time,
        a1.price_end_time,
        (case when a1.status=0 then a1.update_time else a1.price_end_time end) as price_end_time_new 
    from 
            (select * 
            from csx_dwd.csx_dwd_market_research_not_yh_price_di 
            where substr((case when status=0 and price_end_time>date_add(date(update_time),-1) then date_add(date(update_time),-1) else price_end_time end),1,10)>=
             date_add(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'),-365) 
            and product_status='0' 
            ) a1 
            -- 只取TOP商品的对标市调地点
            left join 
            (select distinct bmk_code 
            from csx_analyse_tmp.csx_analyse_tmp_st_dc 
            ) a2 
            on a1.shop_code=a2.bmk_code 
    where a2.bmk_code is not null 
    
    union all 
    -- 目前生效市调数据
    select 
        b1.product_id as market_goods_id,
        b1.source_type_code as market_source_type_code,
        b1.shop_code,
        b1.shop_name,
        cast(b1.price as decimal(20,6)) as market_research_price,
        b1.price_begin_time,
        b1.price_end_time,
        b1.price_end_time as price_end_time_new  
    from 
            (select * 
            from csx_ods.csx_ods_csx_price_prod_market_research_not_yh_price_effective_df 
            where substr(price_end_time,1,10)>=date_add(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'),-365) 
            and product_status=0 
            ) b1 
            -- 只取TOP商品的对标市调地点
            left join 
            (select distinct bmk_code 
            from csx_analyse_tmp.csx_analyse_tmp_st_dc 
            ) b2 
            on b1.shop_code=b2.bmk_code 
    where b2.bmk_code is not null 
    ) c1 
    left join 
    (select * 
    from csx_ods.csx_ods_csx_price_prod_market_research_product_df 
    where sdt='${yes_date}'
    ) c2 
    on c1.market_goods_id=c2.id 
    left join 
    (select * 
    from csx_dim.csx_dim_basic_goods 
    where sdt='current' 
    ) c4 
    on c2.product_code=c4.goods_code 
;

-- 取对标品类
drop table  csx_analyse_tmp.csx_analyse_tmp_st_dc;
create table csx_analyse_tmp.csx_analyse_tmp_st_dc as 
select a.warehouse_code dc_code, 
    a.classify_large_code,
    b.bmk_code  from (
             select *,
                    dimension_value_code as goods_code,
                    ''                   as classify_large_code,
                    ''                   as classify_middle_code,
                    ''                   as classify_small_code,
                    0                       priority
             from    csx_ods.csx_ods_csx_gpm_prod_reference_item_config_df
             where sdt = regexp_replace(date_sub(current_date,1),'-','')
               and responsible_department = 1
               and status = 1
--       `dimension_type`           INT COMMENT '商品 =0 小类 =1 中类 =2 大类 =3',
               and dimension_type = 0
             union all
             select *,
                    ''                   as goods_code,
                    ''                   as classify_large_code,
                    ''                   as classify_middle_code,
                    dimension_value_code as classify_small_code,
                    1                       priority
             from csx_ods.csx_ods_csx_gpm_prod_reference_item_config_df
             where sdt = regexp_replace(date_sub(current_date,1),'-','')
               and responsible_department = 1
               and status = 1
--       `dimension_type`           INT COMMENT '商品 =0 小类 =1 中类 =2 大类 =3',
               and dimension_type = 1
             union all
             select *,
                    ''                   as goods_code,
                    ''                   as classify_large_code,
                    dimension_value_code as classify_middle_code,
                    ''                   as classify_small_code,
                    2                       priority
             from csx_ods.csx_ods_csx_gpm_prod_reference_item_config_df
             where sdt = regexp_replace(date_sub(current_date,1),'-','')
               and responsible_department = 1
               and status = 1
--       `dimension_type`           INT COMMENT '商品 =0 小类 =1 中类 =2 大类 =3',
               and dimension_type = 2
             union all
             select *,
                    ''                   as goods_code,
                    dimension_value_code as classify_large_code,
                    ''                   as classify_middle_code,
                    ''                   as classify_small_code,
                    3                       priority
             from csx_ods.csx_ods_csx_gpm_prod_reference_item_config_df
             where sdt = regexp_replace(date_sub(current_date,1),'-','')
               and responsible_department = 1
               and status = 1
--       `dimension_type`           INT COMMENT '商品 =0 小类 =1 中类 =2 大类 =3',
               and dimension_type = 3
) a 
left join 
-- 关联对标地点
(
select *
from csx_ods.csx_ods_csx_gpm_prod_bmk_config_df
where sdt = regexp_replace(date_sub(current_date,1),'-','')
) b 
on a.id = b.reference_item_config_id 
group by a.warehouse_code, 
    a.classify_large_code,
    b.bmk_code 
; 




-- 计算市调平均价；
drop table if exists csx_analyse_tmp.csx_analyse_tmp_last_week_avg_shop_price_tmp; 
create table if not exists csx_analyse_tmp.csx_analyse_tmp_last_week_avg_shop_price_tmp as 
select 
  csx_week,
  csx_week_rn,
  c4.sdt,
  c3.location_code,
  c3.product_code,
  c3.shop_code,
  avg(c3.market_research_price) as avg_market_price 
from 
  (select 
    c1.* 
  from 
    csx_analyse_tmp.last_4_week_avg_shop_price_tmp_tmp c1 
    -- 选定市调地点数据
    left join 
    -- TOP商品数据
    (select distinct 
        dc_code,
        bmk_code 
    from csx_analyse_tmp.csx_analyse_tmp_st_dc 
    ) c3 
    on c1.shop_code=c3.bmk_code and c1.location_code=c3.dc_code 
  where c3.dc_code is not null 
  ) c3
  cross join 
  (select  calday as sdt,csx_week,dense_rank()over(order by csx_week desc ) csx_week_rn
  from csx_dim.csx_dim_basic_date 
  where calday>=regexp_replace(date_add(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'),-365),'-','') 
  and calday<='${yes_date}'  
  ) c4 
  where c3.price_begin_date<=c4.sdt and c3.price_end_date>=c4.sdt 
  and csx_week_rn<5
group by 
  c4.sdt,
  c3.location_code,
  c3.product_code,
  c3.shop_code ,
   csx_week,
  csx_week_rn
  ;


-- 计算品类环比下滑趋势
with sale as (select csx_week,
-- basic_performance_province_code	,
-- basic_performance_province_name,
-- basic_performance_city_code,
-- basic_performance_city_name	,
-- goods_code,
-- goods_name,
div_name,
classify_large_code,
classify_large_name,
classify_middle_code,
classify_middle_name,
sum(sale_qty )sale_qty,
sum(sale_amt ) sale_amt,
sum(profit)  profit,
sum(sale_cost ) sale_cost,

sum(profit)/sum(sale_amt ) profit_rate,
sum(sale_amt)/sum(sum(sale_amt))over(partition by csx_week) as sale_ratio_rate
from csx_analyse_tmp.csx_analyse_tmp_gd_sale30_top 
group by  csx_week,
div_name,
classify_large_code,
classify_large_name,
classify_middle_code,
classify_middle_name
) 
select csx_week,
div_name,
classify_large_code,
classify_large_name,
classify_middle_code,
classify_middle_name,
(sale_qty )sale_qty,
(sale_amt ) sale_amt,
(profit)  profit,
(sale_cost ) sale_cost,

lag(sale_amt,1,'')over(partition by classify_middle_name order by csx_week asc  ) as last_sale_amt,
profit_rate,
lag(profit_rate,1,'')over(partition by classify_middle_name order by csx_week asc  ) as last_profit_rate,
profit_rate- lag(profit_rate,1,'')over(partition by classify_middle_name order by csx_week asc  ) as diff_profit_rate,
-- 品类销售占比
sale_ratio_rate
from sale


;




-- 计算商品排名取TOP30

-- 统计TOP30商品
-- drop table  csx_analyse_tmp.csx_analyse_tmp_gd_sale30_top_01;
create table csx_analyse_tmp.csx_analyse_tmp_gd_sale30_top_01 as 
with sale as (
select *
,dense_rank()over(partition by classify_middle_code order by all_goods_sale desc ) as all_rn
from (
select 
    --   csx_week,
    --   performance_region_name,
    --   performance_province_code,
    --   performance_province_name,
    --   performance_city_code,
    --   performance_city_name,
      csx_week,
      basic_performance_province_code,
      basic_performance_province_name,
      basic_performance_city_code,
      basic_performance_city_name,
      goods_code,
      goods_name,
      div_name,
      classify_large_code,
      classify_large_name,
      classify_middle_code,
      classify_middle_name,
      classify_small_code,
      classify_small_name,
      unit_name,
      sale_qty,
      sale_amt,
      profit,
      sale_cost,
      sum(sale_amt) over(partition by goods_name) as all_goods_sale,
      rank()over(partition by classify_middle_code order by sale_amt desc ) as rank_rn,
    dense_rank()over(partition by classify_middle_code order by sale_amt desc ) as rn
from csx_analyse_tmp.csx_analyse_tmp_gd_sale30_top 
 where div_name='生鲜'
     
)  a 
)
select goods_code,
      goods_name,
      all_rn
 from sale
where 1=1 
 -- and all_rn<31
 group by goods_code,
      goods_name,all_rn
;

--drop table  csx_analyse_tmp.csx_analyse_tmp_gd_cost_jg;
 create table csx_analyse_tmp.csx_analyse_tmp_gd_cost_jg as 
-- 销售商品平均价
select  a.csx_week,
    --  sdt,
      a.basic_performance_province_code,
      a.basic_performance_province_name,
      a.basic_performance_city_code,
      a.basic_performance_city_name,
      a.goods_code,
      a.goods_name,
      a.div_name,
      a.classify_large_code,
      a.classify_large_name,
      a.classify_middle_code,
      a.classify_middle_name,
      a.classify_small_code,
      a.classify_small_name,
      a.unit_name,
      sale_qty,
      profit_rate,
      sale_amt,
      sale_cost,
     coalesce(price,0) price,
     coalesce(cost,0) cost,
     coalesce(avg_market_price,0) avg_market_price,
     all_rn
  from 
(select
    --   substr(sdt,1,6) csx_week,
    --   performance_region_name,
    --   performance_province_code,
    --   performance_province_name,
    --   performance_city_code,
    --   performance_city_name,
      csx_week,
    --  sdt,
      basic_performance_province_code,
      basic_performance_province_name,
      basic_performance_city_code,
      basic_performance_city_name,
      a.goods_code,
      a.goods_name,
      a.div_name,
      a.classify_large_code,
      a.classify_large_name,
      a.classify_middle_code,
      a.classify_middle_name,
      a.classify_small_code,
      a.classify_small_name,
      a.unit_name,
      all_rn,
      sum(sale_qty) sale_qty,
      sum(sale_amt) sale_amt,
      sum(profit) profit,
      sum(profit)/sum(sale_amt) profit_rate,
      sum(sale_cost) sale_cost,
      sum(sale_amt)/sum(sale_qty) as price,
      sum(sale_cost)/sum(sale_qty)as cost
from  csx_analyse_tmp.csx_analyse_tmp_gd_sale30_top a 
 join (
    select 
      a.goods_code,
      all_rn
    from csx_analyse_tmp.csx_analyse_tmp_gd_sale30_top_01  a 
    where 1=1
       and all_rn<31
) b 
     on  a.goods_code=b.goods_code 
     where 1=1 
    -- and csx_week_rn<5
 group by  csx_week,
    --  sdt,
      basic_performance_province_code,
      basic_performance_province_name,
      basic_performance_city_code,
      basic_performance_city_name,
      a.goods_code,
      a.goods_name,
      a.div_name,
      a.classify_large_code,
      a.classify_large_name,
      a.classify_middle_code,
      a.classify_middle_name,
      a.classify_small_code,
      a.classify_small_name,
      a.unit_name,
      all_rn
  ) a 
left join 
-- 市调商品平均价
(select basic_performance_province_name, 
basic_performance_city_name,
product_code,
csx_week,
round(avg(avg_market_price),2)  avg_market_price
from   csx_analyse_tmp.csx_analyse_tmp_last_week_avg_shop_price_tmp a 
join 
(select basic_performance_province_name, basic_performance_city_name,shop_code from csx_dim.csx_dim_shop where sdt='current') b on a.location_code=b.shop_code
group by basic_performance_province_name, basic_performance_city_name,product_code,csx_week
) b on a.basic_performance_city_name=b.basic_performance_city_name and a.goods_code=b.product_code and a.csx_week=b.csx_week
--where a.goods_code='1'
;




with sale as (select *,
          first_value(price)over(partition by goods_code,basic_performance_city_name  order by  csx_week ) first_avg_sale_rate,
          first_value(profit_rate)over(partition by goods_code,basic_performance_city_name  order by  csx_week ) first_avg_profit_rate,
          first_value(avg_market_price) over (partition by goods_code,basic_performance_city_name order by csx_week ) first_avg_market_price,
          first_value(cost) over (partition by goods_code,basic_performance_city_name order by  csx_week    ) first_entry_price,
          row_number() over (  partition by goods_code,  basic_performance_city_name  order by    csx_week) rn,
          avg(price)over(partition by goods_code,  basic_performance_city_name) avg_price,
          avg(cost)over (partition by goods_code,basic_performance_city_name ) avg_cost,
          avg(avg_market_price) over(partition by goods_code ,basic_performance_city_name ) avg_avg_market_price
  from csx_analyse_tmp.csx_analyse_tmp_gd_cost_jg
  )
  select * ,
price/avg_price-1 avg_price_rate,
cost/avg_cost-1 avg_cost_rate,
avg_market_price/avg_avg_market_price-1 avg_avg_market_price_rate,
cast(coalesce( ( pow(avg_market_price / first_avg_market_price, 1 / (rn -1)) - 1),0.00) as decimal(10, 4)  ) month_avg_market_growth_rate,           -- 市调价复合增长率
cast(coalesce( (pow(cost / first_entry_price, 1 / (rn -1)) - 1),  0.00    ) as decimal(10, 4)     ) month_avg_compound_growth_rate,  -- 入库价复合增长率
cast(coalesce( (pow(price / first_avg_sale_rate, 1 / (rn -1)) - 1),  0.00    ) as decimal(10, 4)     ) month_avg_sale_growth_rate  -- 售价复合增长率
from sale 

 ;