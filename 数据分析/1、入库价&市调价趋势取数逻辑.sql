-- 销售TOP商品 
-- 先计算一年每个月每个品类TOP20 商品
-- 销售TOP商品 
-- 先计算一年每个月每个品类TOP20 商品数据分析/1、入库价&市调价趋势取数逻辑.sql
-- drop table csx_analyse_tmp.csx_analyse_tmp_sale_top;
create table csx_analyse_tmp.csx_analyse_tmp_sale_top as 
with sale as 
(select substr(sdt,1,6) sale_month,
    --   performance_region_name,
    --   performance_province_code,
    --   performance_province_name,
    --   performance_city_code,
    --   performance_city_name,
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
     -- sdt>=regexp_replace(add_months(to_date(concat(date_format('${sdt_yes_date}','y'),'-',floor((cast(date_format('${sdt_yes_date}','M') as int)+2)/3)*3-2,'-',date_format(trunc('${sdt_yes_date}','MM'),'dd'))),-3), '-', '') 
     -- sdt >= '20230901'
     -- and sdt <= '20240411'
      sdt>= regexp_replace(date_add(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'),-7-dayofweek(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'))),'-','') 
      and sdt<= regexp_replace(date_add(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'),-1-dayofweek(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'))),'-','')  
    
      and business_type_code = 1
      and shop_low_profit_flag = 0
      and direct_delivery_type_code not in (1, 2, 11, 12) --  'R直送1'  2  'Z直送2'11  '临时加单'12  '紧急补货'
      and order_channel_code not in ('6', '4') -- 6 '调价''4''返利'
      and refund_order_flag = 0 -- 剔除退货
      and performance_region_name like '%大区%'
group by substr(sdt,1,6) ,
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
      basic_performance_city_name
)
select * from (
select *
 ,lead(sale_month,1,'202404')over(partition by basic_performance_province_name,basic_performance_city_name,classify_middle_code,goods_code order by sale_month) lead_month
 ,lag(sale_month,1,'')over(partition by basic_performance_province_name,basic_performance_city_name,classify_middle_code,goods_code order by sale_month) lag_month

from (
select sale_month,
    --   performance_region_name,
    --   performance_province_code,
    --   performance_province_name,
    --   performance_city_code,
    --   performance_city_name,
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
    count(sale_month)over(partition by basic_performance_province_name,basic_performance_city_name,classify_middle_code,goods_code) sale_month_num,
    dense_rank()over(partition by basic_performance_province_name,basic_performance_city_name,sale_month,classify_middle_code order by sale_amt desc ) as rn
from sale 
) a
) a 
-- where rn<21
;
      -- --------------------------------------------------------
      -- ------入库价取数逻辑


-- 1	采购导入
-- 2	客户直送
-- 3	一件代发
-- 4	项目合伙人
-- 5	无单入库
-- 6	寄售调拨
-- 7	自营调拨
-- 8	云超物流采购
-- 9	工厂调拨
-- 10	智能补货
-- 11	商超直送
-- 12	WMS调拨
-- 13	云超门店采购
-- 14	临时地采
-- 15	联营直送
-- 16	永辉生活
-- 17	RDC调拨
-- 18	城市服务商
-- 19	日采补货
-- 20	紧急补货
-- 21	临时加单
-- 22	分仓调拨
-- 23	手工创建

-- 入库明细
-- drop table  csx_analyse_tmp.csx_analyse_tmp_entry 
create table csx_analyse_tmp.csx_analyse_tmp_entry as 
select 
  basic_performance_province_code,
  basic_performance_province_name,
  basic_performance_city_code,
  basic_performance_city_name,
  target_location_code as dc_code,
  super_class,
  order_code,
  goods_code,
  if(
    price2_include_tax != 0,
    price2_include_tax,
    price_include_tax
  ) as received_price1,
  order_qty as received_qty,
  amount_include_tax,
  header_status,
  source_type,
  config_value as source_type_name,
  (
    case
      when super_class = 1 then '供应商订单'
      when super_class = 2 then '供应商退货订单'
      when super_class = 3 then '配送订单'
      when super_class = 4 then '返配订单'
    end
  ) as super_class,
  (
    case
      when header_status = 1 then '已创建'
      when header_status = 2 then '已发货'
      when header_status = 3 then '部分入库'
      when header_status = 4 then '已完成'
      when header_status = 5 then '已取消'
    end
  ) as status,
  supplier_code,
  supplier_name,
  last_delivery_date,
  create_by as order_create_by,
  create_time as order_create_time,
  sdt
from
    csx_dws.csx_dws_scm_order_detail_di a
  join
  (select dc_code,regexp_replace(to_date(enable_time),'-','') enable_date 
 from csx_dim.csx_dim_csx_data_market_conf_supplychain_location 
 where sdt='current') d on a.target_location_code=d.dc_code
  join (
    select
      config_key,
      config_value
    from
      csx_ods.csx_ods_csx_b2b_scm_scm_configuration_df a
    where
      a.config_type = 'PURCHASE_ORDER_SOURCE_TYPE'
      and sdt = regexp_replace(date_sub(current_date(), 1), '-', '')
  ) b on a.source_type = b.config_key
  left join (
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
  ) c on a.target_location_code=c.shop_code
where
  sdt >= '20230101'
  and sdt<='20240320'
  and super_class in (1,2, 3,4)
  and (
    source_type in ('1', '3', '9', '10', '13', '17', '19', '22', '23')
    and header_status in (1, 2, 3, 4)
  ) --   头表状态(1-已创建、2-已发货、3-部分入库、4-已完成、5-已取消)
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
  (select distinct calday as sdt 
  from csx_dim.csx_dim_basic_date 
  where calday>=regexp_replace(date_add(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'),-365),'-','') 
  and calday<='${yes_date}'  
  ) c4 
  where c3.price_begin_date<=c4.sdt and c3.price_end_date>=c4.sdt 
group by 
  c4.sdt,
  c3.location_code,
  c3.product_code,
  c3.shop_code 
  ;

select basic_performance_province_name, basic_performance_city_name,
      a.goods_code,lead_month,sale_month,lag_month
    from csx_analyse_tmp.csx_analyse_tmp_sale_top  a 
    order by goods_code,lead_month
    group by basic_performance_city_name,
      a.goods_code,basic_performance_province_name 


-- 生成结果表
--drop table  csx_analyse_tmp.csx_analyse_tmp_cost_jg;
 create table csx_analyse_tmp.csx_analyse_tmp_cost_jg as 

select  basic_performance_province_code,
  a.basic_performance_province_name,
  a.basic_performance_city_code,
  a.basic_performance_city_name,
  a.goods_code,
  entry_price,
  avg_market_price,
  a.months,
  year_sale_qty,
all_sale_qty,
sale_month_m,
  sy_top_flag,
  arry_months
  from 
(select basic_performance_province_code,
  a.basic_performance_province_name,
  a.basic_performance_city_code,
  a.basic_performance_city_name,
  a.goods_code,
  sum(if(super_class in (1,3), amount_include_tax,0)-if(super_class in (2,4), amount_include_tax,0))/sum(if(super_class in (1,3), received_qty,0)-if(super_class in (2,4), received_qty,0)) entry_price,
  substr(a.sdt,1,6) months,
  max(year_sale_qty)year_sale_qty,
   max(all_sale_qty) all_sale_qty,
  max(sale_month_m)sale_month_m,
  arry_months
 from csx_analyse_tmp.csx_analyse_tmp_entry a 
 -- 关联销售表取TOP20
 join 
 (select basic_performance_province_name, basic_performance_city_name,
      a.goods_code,
      sum(sale_qty) year_sale_qty ,
      count(distinct sale_month) sale_month_m,
      max(all_sale_qty) all_sale_qty,
      max(sale_month_num) sale_month_num,
      concat_ws('|',collect_set(sale_month)) as arry_months
    from csx_analyse_tmp.csx_analyse_tmp_sale_top  a 
    where rn<21
    group by basic_performance_city_name,
      a.goods_code,basic_performance_province_name 
) b 
     on a.basic_performance_city_name=b.basic_performance_city_name and a.goods_code=b.goods_code and a.basic_performance_province_name=b.basic_performance_province_name
     where sdt>='20230901' 
 group by basic_performance_province_code,
  a.basic_performance_province_name,
  a.basic_performance_city_code,
  a.basic_performance_city_name,
  a.goods_code,
  arry_months,substr(a.sdt,1,6)
  ) a 
left join 
-- 市调商品平均价
(select basic_performance_province_name, 
basic_performance_city_name,
product_code,
substr(sdt,1,6) months,
avg(avg_market_price) avg_market_price 
from   csx_analyse_tmp.csx_analyse_tmp_last_week_avg_shop_price_tmp a 
join 
(select basic_performance_province_name, basic_performance_city_name,shop_code from csx_dim.csx_dim_shop where sdt='current') b on a.location_code=b.shop_code
group by basic_performance_province_name, basic_performance_city_name,product_code,substr(sdt,1,6)
) b on a.basic_performance_city_name=b.basic_performance_city_name and a.goods_code=b.product_code and a.months=b.months

-- 判断是否上个月top months_between(FROM_UNIXTIME( unix_timestamp( concat( sale_month, '01'),'yyyyMMdd' ), 'yyyy-MM-dd' ),      FROM_UNIXTIME( unix_timestamp( concat( lag_month, '01'),'yyyyMMdd' ), 'yyyy-MM-dd' ))
left join 
(  select basic_performance_province_name,
    basic_performance_city_name,
     a.goods_code,
     sale_month,lag_month,
     if( ceil(months_between(FROM_UNIXTIME( unix_timestamp( concat( sale_month, '01'),'yyyyMMdd' ), 'yyyy-MM-dd' ),      FROM_UNIXTIME( unix_timestamp( concat( lag_month, '01'),'yyyyMMdd' ), 'yyyy-MM-dd' )))=1,'1','') sy_top_flag
    from csx_analyse_tmp.csx_analyse_tmp_sale_top  a 
) c on a.months=c.sale_month and a.basic_performance_city_name=c.basic_performance_city_name and a.basic_performance_province_name=c.basic_performance_province_name and a.goods_code=c.goods_code


-- 结果表关联维度

select
  if(avg_market_price is not null,1,0) as type,
  basic_performance_province_code,
  basic_performance_province_name,
  basic_performance_city_code,
  basic_performance_city_name,
  a.goods_code,
  goods_name,
  div_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  classify_small_code,
  classify_small_name,
  unit_name,
  entry_price,
  avg_market_price,
  a.months,
  year_sale_qty,
  sale_qty,
  sale_amt,
  profit,
  sale_month_m,
  sy_top_flag,
  arry_months,
  price,
  cost 
from
  csx_analyse_tmp.csx_analyse_tmp_cost_jg a
  join (
    select
      goods_code,
      goods_name,
      case
        when classify_large_code in ('B01', 'B02', 'B03') THEN '生鲜'
        else '食百'
      end div_name,
      classify_large_code,
      classify_large_name,
      classify_middle_code,
      classify_middle_name,
      classify_small_code,
      classify_small_name,
      unit_name
    from
      csx_dim.csx_dim_basic_goods
    where
      sdt = 'current'
  ) b on a.goods_code = b.goods_code
  left join 
  (select   distinct month,csx_week,month_of_year
  from   csx_dim.csx_dim_basic_date ) c on a.months=c.month_of_year
  left join 
  (select sale_month,
  goods_code,
  basic_performance_province_name,
  basic_performance_city_name,
  sum(sale_qty) sale_qty,
  sum(sale_amt)sale_amt,
  sum(profit) profit,
  sum(sale_amt)/sum(sale_qty) as price,
  sum(sale_cost)/sum(sale_qty) cost  
  from csx_analyse_tmp.csx_analyse_tmp_sale_top 
  group by sale_month,
  goods_code,
  basic_performance_province_name,
  basic_performance_city_name) d on a.months=d.sale_month 
  and a.basic_performance_province_name=d.basic_performance_province_name 
  and a.basic_performance_city_name=d.basic_performance_city_name
  and a.goods_code=d.goods_code
where 1=1 
  
  and classify_middle_code in ('B0101',
'B0103',
'B0104',
'B0201',
'B0202',
'B0301',
'B0302',
'B0303',
'B0306',
'B0602',
'B0603',
'B0701',
'B0702')


select
  basic_performance_province_code,
  basic_performance_province_name,
  basic_performance_city_code,
  basic_performance_city_name,
  a.goods_code,
  goods_name,
  div_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  classify_small_code,
  classify_small_name,
  unit_name,
  entry_price,
  avg_market_price,
  a.months,
  sale_qty,
  sale_month_m,
  sy_top_flag,
  arry_months
from
  csx_analyse_tmp.csx_analyse_tmp_cost_jg a
  join (
    select
      goods_code,
      goods_name,
      case
        when classify_large_code in ('B01', 'B02', 'B03') THEN '生鲜'
        else '食百'
      end div_name,
      classify_large_code,
      classify_large_name,
      classify_middle_code,
      classify_middle_name,
      classify_small_code,
      classify_small_name,
      unit_name
    from
      csx_dim.csx_dim_basic_goods
    where
      sdt = 'current'
  ) b on a.goods_code = b.goods_code
  left join 
  (select   distinct month,csx_week,month_of_year
  from   csx_dim.csx_dim_basic_date ) c on a.months=c.month_of_year
where
  avg_market_price is not null
  and classify_middle_code in ('B0101',
'B0103',
'B0104',
'B0201',
'B0202',
'B0301',
'B0302',
'B0303',
'B0306',
'B0602',
'B0603',
'B0701',
'B0702')

;
with top20 as (
select
  basic_performance_province_code,
  basic_performance_province_name,
  basic_performance_city_code,
  basic_performance_city_name,
  a.goods_code,
  goods_name,
  div_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  classify_small_code,
  classify_small_name,
  unit_name,
  entry_price,
  avg_market_price,
  a.months,
  sale_qty,
  sale_month_m,
  sy_top_flag,
  arry_months
from
  csx_analyse_tmp.csx_analyse_tmp_cost_jg a
  join (
    select
      goods_code,
      goods_name,
      case
        when classify_large_code in ('B01', 'B02', 'B03') THEN '生鲜'
        else '食百'
      end div_name,
      classify_large_code,
      classify_large_name,
      classify_middle_code,
      classify_middle_name,
      classify_small_code,
      classify_small_name,
      unit_name
    from
      csx_dim.csx_dim_basic_goods
    where
      sdt = 'current'
  ) b on a.goods_code = b.goods_code
  left join 
  (select   distinct month,csx_week,month_of_year
  from   csx_dim.csx_dim_basic_date ) c on a.months=c.month_of_year
where
  avg_market_price is not null
  and classify_middle_code in ('B0101',
'B0103',
'B0104',
'B0201',
'B0202',
'B0301',
'B0302',
'B0303',
'B0306',
'B0602',
'B0603',
'B0701',
'B0702')
) 
select a.goods_code,goods_name,classify_large_name,	classify_middle_code,	classify_middle_name,	classify_small_code,	classify_small_name,sum(sale_amt)sale_amt ,dense_rank()over(partition by classify_middle_name order by sum(sale_amt) desc ) al_rn,type from csx_analyse_tmp.csx_analyse_tmp_sale_top a 
left join 
(select goods_code,1 as type from top20 where sale_month_m >=5 group by goods_code) b on a.goods_code=b.goods_code
group by a.goods_code,goods_name,classify_large_name,	classify_middle_code,	classify_middle_name,	classify_small_code,	classify_small_name,type


-- 复合增长率

-- 复合增长率

with sale as (
  select
    if(avg_market_price is not null, 1, 0) as type,
    a.basic_performance_province_code,
    a.basic_performance_province_name,
    a.basic_performance_city_code,
    a.basic_performance_city_name,
    a.goods_code,
    goods_name,
    div_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    unit_name,
    entry_price,
    avg_market_price,
    a.months,
 --   a.sale_qty year_sale_qty,
    d.sale_qty,
    sale_amt,
    profit,
    sale_month_m,
    sy_top_flag,
    arry_months,
    if(coalesce(sale_amt, 0) = 0, 0, profit / sale_amt) profit_rate,
    price as sale_price,
    cost as sale_cost
  from
    csx_analyse_tmp.csx_analyse_tmp_cost_jg a
    join (
      select
        goods_code,
        goods_name,
        case
          when classify_large_code in ('B01', 'B02', 'B03') THEN '生鲜'
          else '食百'
        end div_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name,
        unit_name
      from
        csx_dim.csx_dim_basic_goods
      where
        sdt = 'current'
    ) b on a.goods_code = b.goods_code
    left join (
      select
        distinct month_of_year
      from
        csx_dim.csx_dim_basic_date
    ) c on a.months = c.month_of_year
    left join (
      select
        sale_month,
        goods_code,
        basic_performance_province_name,
        basic_performance_city_name,
        sum(sale_qty) sale_qty,
        sum(sale_amt) sale_amt,
        sum(profit) profit,
        sum(sale_amt) / sum(sale_qty) as price,
        sum(sale_cost) / sum(sale_qty) cost
      from
        csx_analyse_tmp.csx_analyse_tmp_sale_top
      group by
        sale_month,
        goods_code,
        basic_performance_province_name,
        basic_performance_city_name
    ) d on a.months = d.sale_month
    and a.basic_performance_province_name = d.basic_performance_province_name
    and a.basic_performance_city_name = d.basic_performance_city_name
    and a.goods_code = d.goods_code
  where
    1 = 1
    and sale_month_m > 4 --  and a.months='202403'
    and classify_middle_code in (
      'B0101',
      'B0103',
      'B0104',
      'B0201',
      'B0202',
      'B0301',
      'B0302',
      'B0303',
      'B0306',
      'B0602',
      'B0603',
      'B0701',
      'B0702'
    )
)
select
  *,
--   case
--     when (
--       month_avg_market_growth_rate > 0
--       and month_avg_compound_growth_rate < 0
--       and month_profit_rate_growth_rate < 0
--     )
--     or (
--       month_avg_market_growth_rate < 0
--       and month_avg_compound_growth_rate > 0
--       and month_profit_rate_growth_rate < 0
--     ) then '重点成本问题'
--     when month_avg_market_growth_rate < 0
--     and month_avg_compound_growth_rate < 0
--     and month_profit_rate_growth_rate > 0 then '疑似成本问题'
--     when month_avg_market_growth_rate > 0
--     and month_avg_compound_growth_rate > 0
--     and month_profit_rate_growth_rate < 0 then '疑似售价问题'
--   end note
  case
    when 
      month_avg_compound_growth_rate > month_avg_sale_growth_rate and month_avg_compound_growth_rate<month_avg_market_growth_rate
    then '重点成本问题'
    when month_avg_sale_growth_rate> month_avg_compound_growth_rate and month_avg_compound_growth_rate>month_avg_market_growth_rate then '售价问题'

  end note
from
  (
    select
      type,
      a.basic_performance_province_code,
      a.basic_performance_province_name,
      a.basic_performance_city_code,
      a.basic_performance_city_name,
      a.goods_code,
      goods_name,
      div_name,
      classify_large_code,
      classify_large_name,
      classify_middle_code,
      classify_middle_name,
      classify_small_code,
      classify_small_name,
      unit_name,
      entry_price,
      avg_market_price,
      a.months,
      a.sale_qty year_sale_qty,
      sale_qty,
      sale_amt,
      profit,
      profit_rate,
      sale_month_m,
      sy_top_flag,
      arry_months,
      sale_price,
      sale_cost,
      first_entry_price,
      rn,
      first_avg_profit_rate,
      case
        when first_avg_profit_rate < 0 AND profit_rate > 0 then 1 - coalesce(( pow((profit_rate / first_avg_profit_rate) * -1, 1 / (rn -1)) ), 0.00)
        when profit_rate / first_avg_profit_rate < 0 then coalesce( (pow((profit_rate / first_avg_profit_rate) * -1, 1 / (rn -1)) * -1 - 1),0.00)
        when first_avg_profit_rate < 0  AND profit_rate < 0  and profit_rate < first_avg_profit_rate 
            then 
            coalesce((pow((profit_rate / first_avg_profit_rate), 1 / (rn -1)) * -1 - 1 ), 0.00  ) else coalesce(  (pow(profit_rate / first_avg_profit_rate, 1 / (rn -1)) - 1), 0.00 )
      end as month_profit_rate_growth_rate,  -- 毛利率复合增长率
      --  cast(coalesce((pow(profit_rate / first_avg_profit_rate, 1 / (rn-1) ) - 1),0.00) as decimal(10,4)) month_avg_profit_growth_rate,
      cast(coalesce( ( pow(avg_market_price / first_avg_market_price, 1 / (rn -1)) - 1),0.00) as decimal(10, 4)  ) month_avg_market_growth_rate,           -- 市调价复合增长率
      cast(coalesce( (pow(entry_price / first_entry_price, 1 / (rn -1)) - 1),  0.00    ) as decimal(10, 4)     ) month_avg_compound_growth_rate,  -- 入库价复合增长率
      cast(coalesce( (pow(sale_price / first_avg_sale_rate, 1 / (rn -1)) - 1),  0.00    ) as decimal(10, 4)     ) month_avg_sale_growth_rate  -- 售价复合增长率
    from
      (
        select
          type,
          a.basic_performance_province_code,
          a.basic_performance_province_name,
          a.basic_performance_city_code,
          a.basic_performance_city_name,
          a.goods_code,
          goods_name,
          div_name,
          classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name,
          classify_small_code,
          classify_small_name,
          unit_name,
          entry_price,
          avg_market_price,
          a.months,
          a.sale_qty year_sale_qty,
          sale_qty,
          sale_amt,
          profit,
          profit_rate,
          sale_month_m,
          sy_top_flag,
          arry_months,
          sale_price,
          sale_cost,
          first_value(sale_price)over(partition by goods_code,basic_performance_city_name,type  order by  months ) first_avg_sale_rate,
          first_value(profit_rate)over(partition by goods_code,basic_performance_city_name,type  order by  months ) first_avg_profit_rate,
          first_value(avg_market_price) over (partition by goods_code,basic_performance_city_name,type order by months ) first_avg_market_price,
          first_value(entry_price) over (partition by goods_code,basic_performance_city_name,type order by  months    ) first_entry_price,
          row_number() over (  partition by goods_code,  basic_performance_city_name,  type  order by    months) rn
        from
          sale a
        where
          type = 1
          -- and type=1
      ) a
  ) a 
  -- where goods_code in ('1480543','1144602','1065512','1015421')