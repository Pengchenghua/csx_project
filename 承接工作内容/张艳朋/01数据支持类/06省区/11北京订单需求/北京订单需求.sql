-- ------------------------------------------------------------------------------------------
-- create temporary table


set hive.tez.container.size=8192;
-- -------1初始数据表
drop table csx_analyse_tmp.c_tmp_init; 
create table csx_analyse_tmp.c_tmp_init as 
select 
	tt0.* 
from 
(select 
      tt.sdt,
      tt.customer_no,
      tt.sub_customer_code,
      tt.goods_code,
      tt.dc_code,
      tt.price_begin_date,
      tt.price_end_date,
      tt.price_date,
      tt.price_yes_date,
      tt.customer_price,
      tt.purchase_price,
      tt.purchase_price_init,
      tt.mov_price,
      tt.last_purchase_price,
      tt.standard_guide_price,
      tt.guide_price_strategy_type,
      tt.price_type,
      tt.market_price_type,
      tt.if_tmp_guide,
      tt.bmk_code,
      tt.bmk_price,
      tt.bmk_type,
      tt.submit_time,
      tt.confirm_time, 
      tt.associate_no,
      tt.price_create_date,
      tt.cost_price_date,  -- 建议售价及采购报价取价时间 
      tt.suggest_bmk_price,
      tt.suggest_bmk_code,
      tt.price_produce_type, -- 报价生成方式  
      (case when tt.price_end_date>=tt.next_price_begin_date then regexp_replace(date_add(from_unixtime(unix_timestamp(tt.next_price_begin_date, 'yyyyMMdd'), 'yyyy-MM-dd'),-1),'-','') else tt.price_end_date end) as fact_price_end_date
from 
(
    select 
        tt2.*,
        lead(tt2.price_begin_date)over(partition by tt2.customer_no,tt2.sub_customer_code,tt2.goods_code order by tt2.price_begin_date) as next_price_begin_date 
    from 
        (select 
            tt1.*,
            row_number()over(partition by tt1.customer_no,tt1.sub_customer_code,tt1.goods_code,tt1.dc_code,tt1.price_begin_date,tt1.price_end_date order by tt1.submit_time desc) as pm 
         from 
            (select 
                sdt,
                customer_code as customer_no,
                sub_customer_code,
                product_code as goods_code,
                warehouse_code as dc_code,
                regexp_replace(date(price_begin_time),'-','') as price_begin_date,
                regexp_replace(date(price_end_time),'-','') as price_end_date,
                datediff(price_end_time,price_begin_time)+1 as price_date,
                datediff(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'),price_begin_time)+1 as price_yes_date,
                customer_price,
                purchase_price,
                get_json_object(get_json_object(cost_detail,'$.purchasePrice'),'$.purchasePrice') as purchase_price_init,
                get_json_object(get_json_object(cost_detail,'$.defaultPrice'),'$.movingAvgPrice') as mov_price,-- 移动平均价
                cast(purchase_price as decimal(20,6))-cast(last_divergence as decimal(20,6)) as last_purchase_price,
                standard_guide_price,
                (case when guide_price_strategy_type=1 then '目标定价法' 
                      when guide_price_strategy_type=2 then '市调价格' 
                      when guide_price_strategy_type=3 then '手动导入' 
                 end) as  guide_price_strategy_type, -- 建议售价取值类型
                (case when price_type=1 then '建议售价' 
                  when price_type=2 then '对标对象' 
                  when price_type=3 then '销售成本价'
                  when price_type=4 then '上一周价格'
                  when price_type=5 then '售价'
                  when price_type=6 then '采购(库存)成本价'  
                 end) as price_type,
                (case when market_price_type=0 then '通用市调' 
                  when market_price_type=1 then '客户市调' 
                 end) as market_price_type,
                (case when is_tmp_price=1 then '是' else '否' end) as if_tmp_guide,
                get_json_object(customer_price_detail,'$.bmkCode') as bmk_code,
                get_json_object(customer_price_detail,'$.price') as bmk_price,
                (case when get_json_object(customer_price_detail,'$.bmkType')=0 then '永辉门店' 
                  when get_json_object(customer_price_detail,'$.bmkType')=1 then '网站' 
                  when get_json_object(customer_price_detail,'$.bmkType')=2 then '批发市场' 
                  when get_json_object(customer_price_detail,'$.bmkType')=3 then '终端' 
                  when get_json_object(customer_price_detail,'$.bmkType')=99 then '无' 
                 end) as bmk_type,
                associate_no,
                submit_time,
                nvl(confirm_time,update_time) as confirm_time,
                regexp_replace(date(create_time),'-','') as price_create_date,
                regexp_replace(date(cost_price_time),'-','') as cost_price_date,  -- 建议售价及采购报价取价时间 
                get_json_object(regexp_replace(get_json_object(suggest_price_detail,'$.markets'),'\\[|\\]',''),'$.price') as suggest_bmk_price,
                get_json_object(regexp_replace(get_json_object(suggest_price_detail,'$.markets'),'\\[|\\]',''),'$.marketCode') as suggest_bmk_code,
                (case when get_json_object(customer_price_detail,'$.priceType') is null then '手工导入' 
                      when get_json_object(customer_price_detail,'$.status')=1 then '系统策略生成后手工调整' 
                else '系统策略生成' end) as price_produce_type -- 报价生成方式  
             from csx_dwd.csx_dwd_price_customer_price_guide_di where warehouse_code='W0A3'

             union all 
             -- 取近20天失效的报价数据
             select 
                sdt,
                customer_code as customer_no,
                sub_customer_code,
                product_code as goods_code,
                warehouse_code as dc_code,
                regexp_replace(date(price_begin_time),'-','') as price_begin_date,
                regexp_replace(date(date_add(update_time,-1)),'-','') as price_end_date,
                datediff(price_end_time,price_begin_time)+1 as price_date,
                datediff(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'),price_begin_time)+1 as price_yes_date,
                customer_price,
                purchase_price,
                get_json_object(get_json_object(cost_detail,'$.purchasePrice'),'$.purchasePrice') as purchase_price_init,
                get_json_object(get_json_object(cost_detail,'$.defaultPrice'),'$.movingAvgPrice') as mov_price,
                cast(purchase_price as decimal(20,6))-cast(last_divergence as decimal(20,6)) as last_purchase_price,
                standard_guide_price,
                (case when guide_price_strategy_type=1 then '目标定价法' 
                      when guide_price_strategy_type=2 then '市调价格' 
                      when guide_price_strategy_type=3 then '手动导入' 
                 end) as  guide_price_strategy_type, -- 建议售价取值类型
                (case when price_type=1 then '建议售价' 
                  when price_type=2 then '对标对象' 
                  when price_type=3 then '销售成本价'
                  when price_type=4 then '上一周价格'
                  when price_type=5 then '售价'
                  when price_type=6 then '采购(库存)成本价'  
                 end) as price_type,
                (case when market_price_type=0 then '通用市调' 
                  when market_price_type=1 then '客户市调' 
                 end) as market_price_type,
                (case when is_tmp_price=1 then '是' else '否' end) as if_tmp_guide,
                get_json_object(customer_price_detail,'$.bmkCode') as bmk_code,
                get_json_object(customer_price_detail,'$.price') as bmk_price,
                (case when get_json_object(customer_price_detail,'$.bmkType')=0 then '永辉门店' 
                  when get_json_object(customer_price_detail,'$.bmkType')=1 then '网站' 
                  when get_json_object(customer_price_detail,'$.bmkType')=2 then '批发市场' 
                  when get_json_object(customer_price_detail,'$.bmkType')=3 then '终端' 
                  when get_json_object(customer_price_detail,'$.bmkType')=99 then '无' 
                 end) as bmk_type,
                associate_no,
                submit_time,
                nvl(confirm_time,submit_time) as confirm_time, -- 失效区报价的update_time是失效时间
                regexp_replace(date(create_time),'-','') as price_create_date,
                regexp_replace(date(cost_price_time),'-','') as cost_price_date,  -- 建议售价及采购报价取价时间 
                get_json_object(regexp_replace(get_json_object(suggest_price_detail,'$.markets'),'\\[|\\]',''),'$.price') as suggest_bmk_price,
                get_json_object(regexp_replace(get_json_object(suggest_price_detail,'$.markets'),'\\[|\\]',''),'$.marketCode') as suggest_bmk_code,
                (case when get_json_object(customer_price_detail,'$.priceType') is null then '手工导入' 
                      when get_json_object(customer_price_detail,'$.status')=1 then '系统策略生成后手工调整' 
                else '系统策略生成' end) as price_produce_type -- 报价生成方式   
             from csx_dwd.csx_dwd_price_customer_price_guide_invalid_di 
             where sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${start_date}','yyyyMMdd'),'yyyy-MM-dd'),-30),'-','') 
             and sdt<='${end_date}'  
             and (date(update_time)<>date(submit_time) or (date(update_time)<>date(submit_time) and is_tmp_price=0)) and warehouse_code='W0A3'
         ) tt1 
     ) tt2 
    where tt2.pm=1 
) tt 
) tt0 
left join 
(select 
	customer_code,
	goods_code 
from csx_dws.csx_dws_sale_detail_di 
where sdt>='${start_date}' 
and sdt<='${end_date}' 
and business_type_code=1 and inventory_dc_code='W0A3'
group by 
	customer_code,
	goods_code
) tt1 
on tt0.customer_no=tt1.customer_code and tt0.goods_code=tt1.goods_code 
where tt1.goods_code is not null
;  


-- ------------------------------------------------------------------------------------------
-- 2子客户报价数据
drop table csx_analyse_tmp.c_tmp_sub_cus_price_guide; 
create table csx_analyse_tmp.c_tmp_sub_cus_price_guide as 
select 
  t2.sdt as order_date,
  t1.customer_no,
  t1.sub_customer_code,
  t1.dc_code,
  t1.goods_code,
  t1.customer_price,
  t1.purchase_price,
  t1.purchase_price_init,
  t1.mov_price,
  t1.last_purchase_price,
  t1.price_begin_date,
  t1.price_end_date,
  t1.price_date,
  t1.standard_guide_price,
  t1.guide_price_strategy_type,
  t1.price_type,
  t1.market_price_type,
  t1.if_tmp_guide,
  t1.bmk_code,
  t1.bmk_price,
  t1.bmk_type,
  t1.submit_time,
  t1.confirm_time, 
  t1.associate_no,
  t1.price_create_date,
  t1.cost_price_date,  -- 建议售价及采购报价取价时间 
  t1.suggest_bmk_price,
  t1.suggest_bmk_code,
  t1.price_produce_type,
  row_number()over(partition by t2.sdt,t1.customer_no,t1.sub_customer_code,t1.dc_code,t1.goods_code order by t1.associate_no) as num      
from 
  (select * 
   from csx_analyse_tmp.c_tmp_init 
   where sub_customer_code is not null and length(sub_customer_code)>0 
  ) t1
  cross join 
  (select distinct calday as sdt 
  from csx_dim.csx_dim_basic_date 
  where calday>=regexp_replace(date_add(from_unixtime(unix_timestamp('${start_date}','yyyyMMdd'),'yyyy-MM-dd'),-30),'-','')  
  and calday<='${end_date}' -- 结束日期往后延1个月
  ) t2 
where t1.price_begin_date<=t2.sdt and t1.fact_price_end_date>=t2.sdt ;

-- ------------------------------------------------------------------------------------------
-- 3子客户一天多次报价数据
drop table csx_analyse_tmp.c_tmp_sub_cus_price_guide_more; 
create table csx_analyse_tmp.c_tmp_sub_cus_price_guide_more as 
select 
  ddd4.* 
 from 
  (select 
    dd3.*,
    lead(dd3.confirm_time,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_confirm_time,
    lead(dd3.customer_price,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_customer_price,
    lead(dd3.submit_time,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_submit_time,
    lead(dd3.submit_date,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_submit_date,
    lead(dd3.price_begin_date,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_price_begin_date,
    lead(dd3.price_end_date,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_price_end_date,
    lead(dd3.if_tmp_guide,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_if_tmp_guide,
    lead(dd3.price_type,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_price_type,
    lead(dd3.associate_no,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_associate_no,
    lead(dd3.market_price_type,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_market_price_type, 
    lead(dd3.bmk_code,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_bmk_code, 
    lead(dd3.bmk_price,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_bmk_price, 
    lead(dd3.guide_price_strategy_type,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_guide_price_strategy_type,
    lead(dd3.bmk_type,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_bmk_type,

    lead(dd3.purchase_price,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_purchase_price,
    lead(dd3.last_purchase_price,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_last_purchase_price,

    lead(dd3.purchase_price_init,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_purchase_price_init,
    lead(dd3.mov_price,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_mov_price,
    lead(dd3.price_date,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_price_date,
    lead(dd3.standard_guide_price,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_standard_guide_price,

    lead(dd3.price_create_date,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_price_create_date,
    lead(dd3.cost_price_date,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_cost_price_date,
    lead(dd3.suggest_bmk_price,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_suggest_bmk_price,
    lead(dd3.suggest_bmk_code,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_suggest_bmk_code,
    lead(dd3.price_produce_type,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_price_produce_type  
   from 
    (select 
      d2.calday,
      d1.customer_no,
      d1.sub_customer_code,
      d1.goods_code,
      d1.dc_code,
      d1.customer_price,
      d1.purchase_price,
      d1.purchase_price_init,
      d1.mov_price,
      d1.last_purchase_price,
      d1.price_begin_date,
      d1.price_end_date,
      d1.price_date,
      d1.standard_guide_price,
      d1.guide_price_strategy_type,
      d1.price_type,
      d1.market_price_type,
      d1.if_tmp_guide,
      d1.bmk_code,
      d1.bmk_price,
      d1.bmk_type,
      d1.submit_time,
      d1.confirm_time, 
      d1.associate_no,
      d1.price_create_date,
      d1.cost_price_date,  -- 建议售价及采购报价取价时间 
      d1.suggest_bmk_price,
      d1.suggest_bmk_code,
      d1.price_produce_type,
      regexp_replace(split(d1.submit_time, ' ')[0], '-', '') as submit_date,
      count(calday)over(partition by calday,customer_no,goods_code,dc_code) as num, -- 先选出一天有多次报价的商品
      row_number()over(partition by calday,customer_no,sub_customer_code,goods_code,dc_code order by submit_time) as zx_pm, -- 正序排名
      row_number()over(partition by calday,customer_no,sub_customer_code,goods_code,dc_code order by submit_time desc) as dx_pm -- 倒序排名
    from 
      (select * 
      from csx_analyse_tmp.c_tmp_init 
      where sub_customer_code is not null and length(sub_customer_code)>0  -- 只取子客户有报价的数据
      ) d1 
      cross join 
      (select distinct calday 
      from csx_dim.csx_dim_basic_date 
      where calday>=regexp_replace(date_add(from_unixtime(unix_timestamp('${start_date}','yyyyMMdd'),'yyyy-MM-dd'),-30),'-','')  
      and calday<='${end_date}' -- 结束日期往后延1个月
      ) d2 
    where d1.price_begin_date<=d2.calday and d1.price_end_date>=d2.calday
    ) dd3 
  where dd3.num>1 and (zx_pm=1 or dx_pm=1)
  ) ddd4 
where ddd4.calday=ddd4.next_submit_date and ddd4.zx_pm=1;

-- ------------------------------------------------------------------------------------------
-- 4主客户报价数据
drop table csx_analyse_tmp.c_tmp_cus_price_guide; 
create table csx_analyse_tmp.c_tmp_cus_price_guide as 
select 
  a.* 
from 
(select 
  t2.sdt as order_date,
  t1.customer_no,
  t1.dc_code,
  t1.goods_code,
  t1.customer_price,
  t1.purchase_price,
  t1.purchase_price_init,
  t1.mov_price,
  t1.last_purchase_price,
  t1.price_begin_date,
  t1.price_end_date,
  t1.price_date,
  t1.standard_guide_price,
  t1.guide_price_strategy_type,
  t1.price_type,
  t1.market_price_type,
  t1.if_tmp_guide,
  t1.bmk_code,
  t1.bmk_price,
  t1.bmk_type,
  t1.submit_time,
  t1.confirm_time, 
  t1.associate_no,
  t1.price_create_date,
  t1.cost_price_date,  -- 建议售价及采购报价取价时间 
  t1.suggest_bmk_price,
  t1.suggest_bmk_code,
  t1.price_produce_type,
  row_number()over(partition by t2.sdt,t1.customer_no,t1.dc_code,t1.goods_code order by t1.associate_no) as num     
 from 
  (select * 
   from csx_analyse_tmp.c_tmp_init 
   where (sub_customer_code is null or length(sub_customer_code)=0) 
  ) t1
  cross join 
  (select distinct calday as sdt 
  from csx_dim.csx_dim_basic_date 
  where calday>=regexp_replace(date_add(from_unixtime(unix_timestamp('${start_date}','yyyyMMdd'),'yyyy-MM-dd'),-30),'-','')  
  and calday<='${end_date}' -- 结束日期往后延1个月
  ) t2 
  where t1.price_begin_date<=t2.sdt and t1.fact_price_end_date>=t2.sdt
) a 
where a.num=1;

-- ------------------------------------------------------------------------------------------
-- 5主客户一天多次报价数据
drop table csx_analyse_tmp.c_tmp_cus_price_guide_more; 
create table csx_analyse_tmp.c_tmp_cus_price_guide_more as 
select 
  ddd4.* 
 from 
  (select 
    dd3.*,
    lead(dd3.confirm_time,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_confirm_time,
    lead(dd3.customer_price,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_customer_price,
    lead(dd3.submit_time,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_submit_time,
    lead(dd3.submit_date,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_submit_date,
    lead(dd3.price_begin_date,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_price_begin_date,
    lead(dd3.price_end_date,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_price_end_date,
    lead(dd3.if_tmp_guide,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_if_tmp_guide,
    lead(dd3.price_type,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_price_type,
    lead(dd3.associate_no,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_associate_no,
    lead(dd3.market_price_type,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_market_price_type, 
    lead(dd3.bmk_code,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_bmk_code, 
    lead(dd3.bmk_price,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_bmk_price,
    lead(dd3.bmk_type,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_bmk_type,
    lead(dd3.guide_price_strategy_type,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_guide_price_strategy_type,

    lead(dd3.purchase_price,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_purchase_price,
    lead(dd3.last_purchase_price,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_last_purchase_price,

    lead(dd3.purchase_price_init,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_purchase_price_init,
    lead(dd3.mov_price,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_mov_price,
    lead(dd3.price_date,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_price_date,
    lead(dd3.standard_guide_price,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_standard_guide_price,

    lead(dd3.price_create_date,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_price_create_date,
    lead(dd3.cost_price_date,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_cost_price_date,
    lead(dd3.suggest_bmk_price,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_suggest_bmk_price,
    lead(dd3.suggest_bmk_code,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_suggest_bmk_code,
    lead(dd3.price_produce_type,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_price_produce_type               
   from 
    (select 
      d2.calday,
      d1.customer_no,
      d1.goods_code,
      d1.dc_code,
      d1.customer_price,
      d1.purchase_price,
      d1.purchase_price_init,
      d1.mov_price,
      d1.last_purchase_price,
      d1.price_begin_date,
      d1.price_end_date,
      d1.price_date,
      d1.standard_guide_price,
      d1.guide_price_strategy_type,
      d1.price_type,
      d1.market_price_type,
      d1.if_tmp_guide,
      d1.bmk_code,
      d1.bmk_price,
      d1.bmk_type,
      d1.submit_time,
      d1.confirm_time, 
      d1.associate_no,
      d1.price_create_date,
      d1.cost_price_date,  -- 建议售价及采购报价取价时间 
      d1.suggest_bmk_price,
      d1.suggest_bmk_code,
      d1.price_produce_type,
      regexp_replace(split(d1.submit_time, ' ')[0], '-', '') as submit_date,
      count(calday)over(partition by calday,customer_no,goods_code,dc_code) as num, -- 先选出一天有多次报价的商品
      row_number()over(partition by calday,customer_no,goods_code,dc_code order by submit_time) as zx_pm, -- 正序排名
      row_number()over(partition by calday,customer_no,goods_code,dc_code order by submit_time desc) as dx_pm -- 倒序排名
    from 
      (select * 
       from csx_analyse_tmp.c_tmp_init 
       where (sub_customer_code is null or length(sub_customer_code)=0) 
      ) d1 
      cross join 
      (select distinct calday 
      from csx_dim.csx_dim_basic_date 
      where calday>=regexp_replace(date_add(from_unixtime(unix_timestamp('${start_date}','yyyyMMdd'),'yyyy-MM-dd'),-30),'-','')  
      and calday<='${end_date}' -- 结束日期往后延1个月
      ) d2 
    where d1.price_begin_date<=d2.calday and d1.price_end_date>=d2.calday
    ) dd3 
  where dd3.num>1 and (zx_pm=1 or dx_pm=1)
  ) ddd4 
where ddd4.calday=ddd4.next_submit_date and ddd4.zx_pm=1;

-- ------------------------------------------------------------------------------------------
-- 建议售价策略
drop table csx_analyse_tmp.suggest_price; 
create table if not exists csx_analyse_tmp.suggest_price as 
select 
  tt.* 
from 
    (select 
      b.calday,
      a.warehouse_code as dc_code,
      a.product_code as goods_code,
      (case when suggest_price_type='1' then '目标定价法' 
            when suggest_price_type='2' then '市调价格' 
            when suggest_price_type='3' then '手动导入' 
      end) as suggest_price_type_name,
      a.purchase_price,
      a.suggest_price_mid,-- 建议售价中
      get_json_object(regexp_replace(get_json_object(a.suggest_price_detail,'$.markets'),'\\[|\\]',''),'$.price') as suggest_bmk_price,
      get_json_object(regexp_replace(get_json_object(a.suggest_price_detail,'$.markets'),'\\[|\\]',''),'$.marketCode') as suggest_bmk_code, 
      row_number()over(partition by b.calday,a.warehouse_code,a.product_code order by a.create_time desc) as pm 
    from 
      (select 
          *,
          regexp_replace(substr(price_begin_time,1,10),'-','') as price_begin_date,
          regexp_replace(substr(price_end_time,1,10),'-','') as price_end_date  
      from csx_dwd.csx_dwd_price_goods_price_guide_di  
      where regexp_replace(substr(price_end_time,1,10),'-','')>=regexp_replace(date_add(from_unixtime(unix_timestamp('${start_date}','yyyyMMdd'),'yyyy-MM-dd'),-90),'-','') 
      ) a 
      cross join 
      (select distinct calday 
      from csx_dim.csx_dim_basic_date 
      where calday>=regexp_replace(date_add(from_unixtime(unix_timestamp('${start_date}','yyyyMMdd'),'yyyy-MM-dd'),-30),'-','')  
      and calday<='${end_date}' -- 结束日期往后延1个月
      ) b  
    where a.price_begin_date<=b.calday and a.price_end_date>=b.calday 
    ) tt 
where tt.pm=1 
;

-- -------------------------------------------------------------------------------
-- ------生成报价监控相应底表数据
drop table csx_analyse_tmp.c_tmp_cus_price_guide_order; 
create table if not exists csx_analyse_tmp.c_tmp_cus_price_guide_order as 
select 
  a.sdt,
  a.order_date,
  a.order_time,
  a.require_delivery_date,
  a.final_qj_date,
  a.performance_region_name,
  a.performance_province_name,
  a.performance_city_name,
  a.customer_code,
  a.customer_name,
  a.sub_customer_code,
  a.sub_customer_name,
  a.order_code,
  a.classify_middle_code,
  g.classify_middle_threshold,
  a.goods_code,
  a.goods_name,
  a.inventory_dc_code,
  (case when b.dc_code is not null then '是' else '否' end) as if_zs,
  a.delivery_type_name,
  (case 
    when a.delivery_type_name='配送' then ''
    when a.direct_delivery_type=1 then 'R直送1'
    when a.direct_delivery_type=2 then 'Z直送2'
    when a.direct_delivery_type=11 then '临时加单'
    when a.direct_delivery_type=12 then '紧急补货'
    when a.direct_delivery_type=0 then '普通' else '普通' 
  end) direct_delivery_type,   -- 订单配送模式
  if(a.order_channel_code=6 ,'是','否') as is_tiaojia,
  if(a.order_channel_code=4 ,'是','否') as is_fanli,
  a.sale_price,
  a.cost_price,
  a.sale_qty,
  a.sale_amt,
  a.profit,
  f.price_source,
  (case -- 子客户多次报价 
        when length(e2.sub_customer_code)>0 and a.final_qj_time<e2.next_submit_time then e2.customer_price 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_submit_time and a.final_qj_time<e2.next_price_begin_date then 0 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_price_begin_date then e2.next_customer_price 

        -- 子客户一次报价
        when length(e1.sub_customer_code)>0 then e1.customer_price 

        -- 主客户多次报价
        when length(e4.customer_no)>0 and a.final_qj_time<e4.next_submit_time then e4.customer_price 
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_submit_time and a.final_qj_time<e4.next_price_begin_date then 0 
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_price_begin_date then e4.next_customer_price 

        -- 主客户一次报价
        when length(e3.customer_no)>0 then e3.customer_price end) as customer_price,

  (case -- 子客户多次报价 
        when length(e2.sub_customer_code)>0 and a.final_qj_time<e2.next_submit_time then e2.confirm_time 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_submit_time and a.final_qj_time<e2.next_price_begin_date then '' 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_price_begin_date then e2.next_confirm_time 

        -- 子客户一次报价
        when length(e1.sub_customer_code)>0 then e1.confirm_time 

        -- 主客户多次报价
        when length(e4.customer_no)>0 and a.final_qj_time<e4.next_submit_time then e4.confirm_time 
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_submit_time and a.final_qj_time<e4.next_price_begin_date then ''
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_price_begin_date then e4.next_confirm_time 

        -- 主客户一次报价
        when length(e3.customer_no)>0 then e3.confirm_time end) as confirm_time,


  (case -- 子客户多次报价 
        when length(e2.sub_customer_code)>0 and a.final_qj_time<e2.next_submit_time then e2.price_type 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_submit_time and a.final_qj_time<e2.next_price_begin_date then '无' 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_price_begin_date then e2.next_price_type

        -- 子客户一次报价
        when length(e1.sub_customer_code)>0 then e1.price_type 

        -- 主客户多次报价
        when length(e4.customer_no)>0 and a.final_qj_time<e4.next_submit_time then e4.price_type 
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_submit_time and a.final_qj_time<e4.next_price_begin_date then '无' 
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_price_begin_date then e4.next_price_type 

        -- 主客户一次报价
        when length(e3.customer_no)>0 then e3.price_type end) as price_type,  -- 定价类型 
  (case -- 子客户多次报价 
        when length(e2.sub_customer_code)>0 and a.final_qj_time<e2.next_submit_time then e2.price_begin_date 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_submit_time and a.final_qj_time<e2.next_price_begin_date then '无' 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_price_begin_date then e2.next_price_begin_date

        -- 子客户一次报价
        when length(e1.sub_customer_code)>0 then e1.price_begin_date 

        -- 主客户多次报价
        when length(e4.customer_no)>0 and a.final_qj_time<e4.next_submit_time then e4.price_begin_date 
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_submit_time and a.final_qj_time<e4.next_price_begin_date then '无' 
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_price_begin_date then e4.next_price_begin_date 

        -- 主客户一次报价
        when length(e3.customer_no)>0 then e3.price_begin_date end) as price_begin_date,  
  (case -- 子客户多次报价 
        when length(e2.sub_customer_code)>0 and a.final_qj_time<e2.next_submit_time then e2.price_end_date 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_submit_time and a.final_qj_time<e2.next_price_begin_date then '无' 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_price_begin_date then e2.next_price_end_date

        -- 子客户一次报价
        when length(e1.sub_customer_code)>0 then e1.price_end_date 

        -- 主客户多次报价
        when length(e4.customer_no)>0 and a.final_qj_time<e4.next_submit_time then e4.price_end_date 
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_submit_time and a.final_qj_time<e4.next_price_begin_date then '无' 
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_price_begin_date then e4.next_price_end_date 

        -- 主客户一次报价
        when length(e3.customer_no)>0 then e3.price_end_date end) as price_end_date, 
  (case -- 子客户多次报价 
        when length(e2.sub_customer_code)>0 and a.final_qj_time<e2.next_submit_time then e2.guide_price_strategy_type 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_submit_time and a.final_qj_time<e2.next_price_begin_date then '无' 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_price_begin_date then e2.next_guide_price_strategy_type

        -- 子客户一次报价
        when length(e1.sub_customer_code)>0 then e1.guide_price_strategy_type 

        -- 主客户多次报价
        when length(e4.customer_no)>0 and a.final_qj_time<e4.next_submit_time then e4.guide_price_strategy_type 
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_submit_time and a.final_qj_time<e4.next_price_begin_date then 0  
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_price_begin_date then e4.next_guide_price_strategy_type 

        -- 主客户一次报价
        when length(e3.customer_no)>0 then e3.guide_price_strategy_type end) as guide_price_strategy_type, 
  (case -- 子客户多次报价 
        when length(e2.sub_customer_code)>0 and a.final_qj_time<e2.next_submit_time then e2.purchase_price 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_submit_time and a.final_qj_time<e2.next_price_begin_date then '无' 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_price_begin_date then e2.next_purchase_price

        -- 子客户一次报价
        when length(e1.sub_customer_code)>0 then e1.purchase_price 

        -- 主客户多次报价
        when length(e4.customer_no)>0 and a.final_qj_time<e4.next_submit_time then e4.purchase_price 
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_submit_time and a.final_qj_time<e4.next_price_begin_date then 0  
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_price_begin_date then e4.next_purchase_price 

        -- 主客户一次报价
        when length(e3.customer_no)>0 then e3.purchase_price end) as purchase_price, 
   (case -- 子客户多次报价 
        when length(e2.sub_customer_code)>0 and a.final_qj_time<e2.next_submit_time then e2.standard_guide_price 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_submit_time and a.final_qj_time<e2.next_price_begin_date then '无' 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_price_begin_date then e2.next_standard_guide_price 
        -- 子客户一次报价
        when length(e1.sub_customer_code)>0 then e1.standard_guide_price 

        -- 主客户多次报价
        when length(e4.customer_no)>0 and a.final_qj_time<e4.next_submit_time then e4.standard_guide_price 
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_submit_time and a.final_qj_time<e4.next_price_begin_date then 0  
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_price_begin_date then e4.next_standard_guide_price 

        -- 主客户一次报价
        when length(e3.customer_no)>0 then e3.standard_guide_price end) as standard_guide_price,
  (case -- 子客户多次报价 
        when length(e2.sub_customer_code)>0 and a.final_qj_time<e2.next_submit_time then e2.bmk_code 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_submit_time and a.final_qj_time<e2.next_price_begin_date then '无' 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_price_begin_date then e2.next_bmk_code 
        -- 子客户一次报价
        when length(e1.sub_customer_code)>0 then e1.bmk_code 

        -- 主客户多次报价
        when length(e4.customer_no)>0 and a.final_qj_time<e4.next_submit_time then e4.bmk_code 
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_submit_time and a.final_qj_time<e4.next_price_begin_date then 0  
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_price_begin_date then e4.next_bmk_code 

        -- 主客户一次报价
        when length(e3.customer_no)>0 then e3.bmk_code end) as bmk_code, 
  (case -- 子客户多次报价 
        when length(e2.sub_customer_code)>0 and a.final_qj_time<e2.next_submit_time then e2.bmk_price 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_submit_time and a.final_qj_time<e2.next_price_begin_date then '无' 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_price_begin_date then e2.next_bmk_price 
        -- 子客户一次报价
        when length(e1.sub_customer_code)>0 then e1.bmk_price 

        -- 主客户多次报价
        when length(e4.customer_no)>0 and a.final_qj_time<e4.next_submit_time then e4.bmk_price 
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_submit_time and a.final_qj_time<e4.next_price_begin_date then 0  
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_price_begin_date then e4.next_bmk_price 

        -- 主客户一次报价
        when length(e3.customer_no)>0 then e3.bmk_price end) as bmk_price,

  (case -- 子客户多次报价 
        when length(e2.sub_customer_code)>0 and a.final_qj_time<e2.next_submit_time then e2.price_create_date 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_submit_time and a.final_qj_time<e2.next_price_begin_date then '无' 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_price_begin_date then e2.next_price_create_date 
        -- 子客户一次报价
        when length(e1.sub_customer_code)>0 then e1.price_create_date 

        -- 主客户多次报价
        when length(e4.customer_no)>0 and a.final_qj_time<e4.next_submit_time then e4.price_create_date 
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_submit_time and a.final_qj_time<e4.next_price_begin_date then '无'  
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_price_begin_date then e4.next_price_create_date 

        -- 主客户一次报价
        when length(e3.customer_no)>0 then e3.price_create_date end) as price_create_date,

  (case -- 子客户多次报价 
        when length(e2.sub_customer_code)>0 and a.final_qj_time<e2.next_submit_time then e2.cost_price_date 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_submit_time and a.final_qj_time<e2.next_price_begin_date then '无' 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_price_begin_date then e2.next_cost_price_date 
        -- 子客户一次报价
        when length(e1.sub_customer_code)>0 then e1.cost_price_date 

        -- 主客户多次报价
        when length(e4.customer_no)>0 and a.final_qj_time<e4.next_submit_time then e4.cost_price_date 
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_submit_time and a.final_qj_time<e4.next_price_begin_date then '无'  
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_price_begin_date then e4.next_cost_price_date 

        -- 主客户一次报价
        when length(e3.customer_no)>0 then e3.cost_price_date end) as cost_price_date,
  (case -- 子客户多次报价 
        when length(e2.sub_customer_code)>0 and a.final_qj_time<e2.next_submit_time then e2.suggest_bmk_price 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_submit_time and a.final_qj_time<e2.next_price_begin_date then 0 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_price_begin_date then e2.next_suggest_bmk_price 
        -- 子客户一次报价
        when length(e1.sub_customer_code)>0 then e1.suggest_bmk_price 

        -- 主客户多次报价
        when length(e4.customer_no)>0 and a.final_qj_time<e4.next_submit_time then e4.suggest_bmk_price 
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_submit_time and a.final_qj_time<e4.next_price_begin_date then 0  
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_price_begin_date then e4.next_suggest_bmk_price 

        -- 主客户一次报价
        when length(e3.customer_no)>0 then e3.suggest_bmk_price end) as suggest_bmk_price,
  (case -- 子客户多次报价 
        when length(e2.sub_customer_code)>0 and a.final_qj_time<e2.next_submit_time then e2.suggest_bmk_code 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_submit_time and a.final_qj_time<e2.next_price_begin_date then '无' 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_price_begin_date then e2.next_suggest_bmk_code 
        -- 子客户一次报价
        when length(e1.sub_customer_code)>0 then e1.suggest_bmk_code 

        -- 主客户多次报价
        when length(e4.customer_no)>0 and a.final_qj_time<e4.next_submit_time then e4.suggest_bmk_code 
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_submit_time and a.final_qj_time<e4.next_price_begin_date then '无'  
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_price_begin_date then e4.next_suggest_bmk_code 

        -- 主客户一次报价
        when length(e3.customer_no)>0 then e3.suggest_bmk_code end) as suggest_bmk_code,
  (case -- 子客户多次报价 
        when length(e2.sub_customer_code)>0 and a.final_qj_time<e2.next_submit_time then e2.price_produce_type 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_submit_time and a.final_qj_time<e2.next_price_begin_date then '无' 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_price_begin_date then e2.next_price_produce_type 
        -- 子客户一次报价
        when length(e1.sub_customer_code)>0 then e1.price_produce_type 

        -- 主客户多次报价
        when length(e4.customer_no)>0 and a.final_qj_time<e4.next_submit_time then e4.price_produce_type 
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_submit_time and a.final_qj_time<e4.next_price_begin_date then '无'  
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_price_begin_date then e4.next_price_produce_type 

        -- 主客户一次报价
        when length(e3.customer_no)>0 then e3.price_produce_type end) as price_produce_type,
  f.mark_code ,-- 两类对标订单的对标地点
  f.mark_price,-- 两类对标订单的对标地点价格 
  a.if_abnormal,
  a.sdt 
from 
  (select 
      a1.*,
      (case when a1.fir_qj_date<a2.refresh_date and a2.refresh_date is not null then a2.refresh_date else a1.fir_qj_date end) as final_qj_date,
      (case when a1.fir_qj_time<a2.refresh_time and a2.refresh_time is not null then a2.refresh_time else a1.fir_qj_time end) as final_qj_time,
      split(a1.id, '&')[0] as credential_no  
   from 
    (-- 非调剂返利单
    select 
        *,
        concat(customer_code,sub_customer_code) as cus_no_new,
        regexp_replace(substr(order_time,1,10),'-','') as order_date,
        regexp_replace(substr(delivery_time,1,10),'-','') as delivery_date,
        (case when require_delivery_date<regexp_replace(substr(order_time,1,10),'-','') then regexp_replace(substr(order_time,1,10),'-','') else require_delivery_date end) as fir_qj_date,
        (case when require_delivery_date<=regexp_replace(substr(order_time,1,10),'-','') then order_time 
              when require_delivery_date>regexp_replace(substr(order_time,1,10),'-','') then from_unixtime(unix_timestamp(require_delivery_date,'yyyyMMdd'),'yyyy-MM-dd HH:mm:ss') end) as fir_qj_time,
        (case when order_channel_code in (4,6) or order_channel_detail_code in (26) or refund_order_flag=1 or delivery_type_code=2 then '是' else '否' end) as if_abnormal  
    from csx_dws.csx_dws_sale_detail_di   
    where sdt>='${start_date}'  
  and sdt<='${end_date}' -- 结束日期往后延1个月
    -- and delivery_type_name='配送' 
    and business_type_code=1 
	and inventory_dc_code='W0A3'
    -- and order_channel_code not in (4,6)  
    -- and order_channel_detail_code not in (26)  
    -- and refund_order_flag=0 -- 剔除退货 
    ) a1 
    left join 
    -- 匹配刷价时间
    (select 
        order_no,
        product_code,
        max(regexp_replace(substr(update_time,1,10),'-','')) as refresh_date,
        max(update_time) as refresh_time 
     from csx_ods.csx_ods_b2b_mall_prod_yszx_order_refresh_price_record_df 
     group by 
        order_no,
        product_code
    ) a2 
    on a1.order_code=a2.order_no and a1.goods_code=a2.product_code 
  ) a 
  left join 
  -- 看是否是直送仓
  (select distinct shop_code as dc_code 
  from csx_dim.csx_dim_shop  
  where sdt='current' 
  and shop_low_profit_flag=1 ) b 
  on a.inventory_dc_code=b.dc_code  
  left join 
  -- 子客户报价相关数据
  (select * 
  from csx_analyse_tmp.c_tmp_sub_cus_price_guide 
  where num=1) e1 
  on a.final_qj_date=e1.order_date and a.customer_code=e1.customer_no and a.goods_code=e1.goods_code and a.inventory_dc_code=e1.dc_code and a.sub_customer_code=e1.sub_customer_code 
  left join
  -- 子客户一天有多次报价的数据处理
  csx_analyse_tmp.c_tmp_sub_cus_price_guide_more e2 
  on a.final_qj_date=e2.calday and a.customer_code=e2.customer_no and a.goods_code=e2.goods_code and a.inventory_dc_code=e2.dc_code and a.sub_customer_code=e2.sub_customer_code 
  left join 
  -- 主客户报价相关数据
  (select * 
  from csx_analyse_tmp.c_tmp_cus_price_guide 
  where num=1) e3  
  on a.final_qj_date=e3.order_date and a.customer_code=e3.customer_no and a.goods_code=e3.goods_code and a.inventory_dc_code=e3.dc_code 
  left join
  -- 主客户一天有多次报价的数据处理
  csx_analyse_tmp.c_tmp_cus_price_guide_more e4 
  on a.final_qj_date=e4.calday and a.customer_code=e4.customer_no and a.goods_code=e4.goods_code and a.inventory_dc_code=e4.dc_code 
  left join 
  -- 匹配订单那价格来源
  (select 
    *,
    CASE WHEN sale_price_explain LIKE '输入价%' THEN '输入价'
      WHEN sale_price_explain LIKE '指定成交价%' THEN '指定成交价'
      WHEN sale_price_explain LIKE '指定售价%' THEN '指定售价' 
      WHEN sale_price_explain LIKE '手工调整价%' THEN '手工调整价'
      WHEN sale_price_explain LIKE '库存%' THEN '库存地点售价' -- 客户报价策略中取高中低哪个建议售价，然后再看上下浮比例，最后用这两个值求得的值为库存地点售价
      WHEN sale_price_explain LIKE '对标门店正常价%' THEN '对标门店正常价'
      WHEN sale_price_explain LIKE '对标永辉门店%' THEN '对标永辉门店'
      WHEN sale_price_explain LIKE '永辉门店%' THEN '对标永辉门店' 
      WHEN sale_price_explain LIKE '对标市调%' THEN '对标市调'
      WHEN sale_price_explain LIKE '市调%' THEN '对标市调' 
      WHEN sale_price_explain LIKE '毛利事中控制%' THEN '毛利事中控制改价' 
      ELSE sale_price_explain END AS price_source 
  from csx_dwd.csx_dwd_csms_yszx_order_detail_di 
  where (sdt >= regexp_replace(date_add(from_unixtime(unix_timestamp('${start_date}','yyyyMMdd'),'yyyy-MM-dd'),-60),'-','')  OR sdt = '19990101')
  ) f 
  on a.original_order_code = f.order_code and a.goods_code=f.goods_code 
  left join 
  csx_analyse_tmp.classify_middle_threshold_list g 
  on a.classify_middle_code=g.classify_middle_code 
where b.dc_code is null 
;

-- -------------------------------------------------------------------------------
-- ------倒数第二张订单表
drop table csx_analyse_tmp.c_tmp_cus_price_guide_order_final_tmp; 
create table if not exists csx_analyse_tmp.c_tmp_cus_price_guide_order_final_tmp as 
select 
  a.sdt,
  a.order_date,
  a.order_time,
  a.require_delivery_date,
  a.final_qj_date,
  a.performance_region_name,
  a.performance_province_name,
  a.performance_city_name,
  a.customer_code,
  a.customer_name,
  a.sub_customer_code,
  a.sub_customer_name,
  a.order_code,
  a.goods_code,
  a.goods_name,
  a.inventory_dc_code,
  a.delivery_type_name,
  a.direct_delivery_type,
  a.is_fanli,
  a.is_tiaojia,
  a.sale_amt,
  a.profit,
  a.sale_price,
  --a.classify_middle_threshold,
  a.cost_price,
  a.sale_qty,
  a.price_source,
  (case when a.if_abnormal='是' then '异常数据' 
        when a.price_source in ('对标市调','对标永辉门店') or (a.price_source='指定成交价' and a.price_type='对标对象') then '对标对象' 
        when a.price_source in ('库存地点售价') or (a.price_source='指定成交价' and a.price_type='建议售价') then '建议售价' 
        when a.price_source in ('手工调整价') then a.price_source  
        when a.price_source in ('指定成交价') then a.price_type  
   else a.price_source end) as price_type_final, -- 最终的定价类型
  (case when a.if_abnormal='否' and a.price_source in ('指定成交价') then a.confirm_time end) as confirm_time,
  (case when a.if_abnormal='否' and a.price_source in ('指定成交价') then a.price_begin_date end) as price_begin_date, -- 报价开始时间
  (case when a.if_abnormal='否' and a.price_source in ('指定成交价') then a.price_begin_date end) as price_end_date, -- 报价结束时间
  (case when a.if_abnormal='否' and (a.price_source in ('对标市调','对标永辉门店') or (a.price_source='指定成交价' and a.price_type='对标对象')) then nvl(a.mark_code,a.bmk_code) end) as bmk_code, -- 对标地点编码
  (case when a.if_abnormal='否' and (a.price_source in ('对标市调','对标永辉门店') or (a.price_source='指定成交价' and a.price_type='对标对象')) then nvl(a.mark_price,a.bmk_price) end) as bmk_price, -- 对标地点价格 
  (case when a.if_abnormal='否' and a.price_source='库存地点售价' then b.suggest_price_mid 
        when a.if_abnormal='否' and a.price_source='指定成交价' and a.price_type='建议售价' then a.standard_guide_price
  end) as suggest_price,
  (case when a.if_abnormal='否' and a.price_source='库存地点售价' then b.suggest_price_type_name 
        when a.if_abnormal='否' and a.price_source='指定成交价' and a.price_type='建议售价' then a.guide_price_strategy_type
  end) as suggest_price_type,  
(case when a.if_abnormal='否' and a.price_source='库存地点售价' then b.purchase_price 
      when a.if_abnormal='否' and a.price_source='指定成交价' and a.price_type in ('建议售价','采购(库存)成本价') then a.purchase_price
  end) as purchase_price,
(case when a.if_abnormal='否' and a.price_source='库存地点售价' then b.suggest_bmk_price 
      when a.if_abnormal='否' and a.price_source='指定成交价' and a.price_type in ('建议售价','采购(库存)成本价') then a.suggest_bmk_price
  end) as suggest_bmk_price,
(case when a.if_abnormal='否' and a.price_source='库存地点售价' then b.suggest_bmk_code 
      when a.if_abnormal='否' and a.price_source='指定成交价' and a.price_type in ('建议售价','采购(库存)成本价') then a.suggest_bmk_code
  end) as suggest_bmk_code,
(case when a.if_abnormal='否' and a.price_source='指定成交价' then a.price_produce_type end) as price_produce_type,
c.received_price -- 近期入库成本  
from 
csx_analyse_tmp.c_tmp_cus_price_guide_order a 
left join 
-- 建议售价数据
csx_analyse_tmp.suggest_price b 
on a.inventory_dc_code=b.dc_code and a.goods_code=b.goods_code and a.final_qj_date=b.calday 
left join 
-- 近期成本数据
csx_analyse_tmp.dc_goods_received_tmp c
on a.inventory_dc_code=c.target_location_code and a.goods_code=c.goods_code 
; 

select * from csx_analyse_tmp.c_tmp_cus_price_guide_order_final_tmp