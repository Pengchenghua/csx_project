-- ------------------------------------------------------------------------------------------
-- 子客户报价数据
drop table if exists csx_analyse_tmp.csx_analyse_tmp_sub_cus_price_guide; 
create table if not exists csx_analyse_tmp.csx_analyse_tmp_sub_cus_price_guide as 
select 
  la.* 
from 
(select 
  t2.sdt as order_date,
  t1.customer_no,
  t1.sub_customer_code,
  t1.dc_code,
  t1.goods_code,
  t1.sys_price,
  t1.sc_price,
  t1.customer_price,
  t1.purchase_price,
  t1.price_begin_date,
  t1.price_end_date,
  t1.price_type,
  t1.if_tmp_guide,
  t1.associate_no,
  t1.submit_by,
  row_number()over(partition by t2.sdt,t1.customer_no,t1.sub_customer_code,t1.dc_code,t1.goods_code order by t1.associate_no desc) as num      
 from 
  (select 
    b1.* 
   from 
      (select 
          tt.sdt,
          tt.customer_no,
          tt.sub_customer_code,
          tt.goods_code,
          tt.dc_code,
          tt.price_begin_date,
          tt.price_end_date,
          tt.sys_price,
          tt.sc_price,
        tt.customer_price,
        tt.purchase_price,
        tt.price_type,
        tt.if_tmp_guide,
        tt.associate_no,
        tt.submit_by,
          (case when tt.price_end_date>=tt.next_price_begin_date then regexp_replace(date_add(from_unixtime(unix_timestamp(tt.next_price_begin_date, 'yyyyMMdd'), 'yyyy-MM-dd'),-1),'-','') else tt.price_end_date end) as fact_price_end_date
      from 
          (select 
            t1.*,
              lead(price_begin_date)over(partition by customer_no,sub_customer_code,goods_code order by price_begin_date) as next_price_begin_date 
           from 
            (select 
              sdt,
                customer_no,
                customer_name,
                sub_customer_code,
                sub_customer_name,
                goods_code,
                goods_name,
                dc_code,
                price_begin_date,
                price_end_date,
                sys_price,
              customer_price,
              purchase_price,
              price_type,
              associate_no,
              if_tmp_guide,
              sc_price,
              submit_by,
              row_number()over(partition by customer_no,sub_customer_code,goods_code,dc_code,price_begin_date,price_end_date order by submit_time desc) as pm
             from csx_analyse.csx_analyse_report_price_customer_price_guide_today_effect_di 
            where sdt>=regexp_replace(substr(date_add('${sdt_start_date}',-30),1,10),'-','') and sdt<='${sdt_end}'
            and price_end_date>='${sdt_start}' 
            and sub_customer_code is not null and length(sub_customer_code)>0 -- 只取子客户有报价的数据 
            ) t1 
          where t1.pm=1
          ) tt  
      ) b1 
    -- 去掉失效报价数据
    left join 
    (select 
      *,
      regexp_replace(split(price_begin_time, ' ')[0], '-', '') as price_begin_date,
      regexp_replace(split(price_end_time, ' ')[0], '-', '') as price_end_date,
      regexp_replace(split(update_time, ' ')[0], '-', '') as update_date   
     from csx_dwd.csx_dwd_price_customer_price_guide_invalid_di 
     where sdt>=regexp_replace(substr(date_add('${sdt_start_date}',-30),1,10),'-','') and sdt<='${sdt_end}'
    ) b4 
    on b1.associate_no=b4.associate_no
    -- and b1.customer_no=b4.customer_code and b1.dc_code=b4.warehouse_code and b1.goods_code=b4.product_code and b1.price_begin_date=b4.price_begin_date and b1.price_end_date=b4.price_end_date 
    where b4.update_date>='${sdt_start}' or b4.update_date is null 
  ) t1
  cross join 
  (select distinct calday as sdt 
  from csx_dim.csx_dim_basic_date 
  where calday>='${sdt_start}' 
  and calday<=regexp_replace(substr(date_add('${sdt_end_date}',30),1,10),'-','') -- 结束日期往后延1个月
  ) t2 
  where t1.price_begin_date<=t2.sdt and t1.fact_price_end_date>=t2.sdt
  ) la 
where la.num=1 ;



-- ------------------------------------------------------------------------------------------
-- 子客户一天多次报价数据
drop table if exists csx_analyse_tmp.csx_analyse_tmp_sub_cus_price_guide_more; 
create table if not exists csx_analyse_tmp.csx_analyse_tmp_sub_cus_price_guide_more as 
select 
  ddd4.* 
 from 
  (select 
    dd3.*,
    lead(dd3.customer_price,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_customer_price,
    lead(dd3.submit_time,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_submit_time,
    lead(dd3.submit_date,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_submit_date,
    lead(dd3.price_begin_date,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_price_begin_date,
    lead(dd3.sc_price,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_sc_price,
    lead(dd3.sys_price,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_sys_price,
    lead(dd3.if_tmp_guide,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_if_tmp_guide,
    lead(dd3.price_type,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_price_type,
    lead(dd3.submit_by,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_submit_by,
    lead(dd3.associate_no,1)over(partition by dd3.customer_no,dd3.sub_customer_code,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_associate_no      
   from 
    (select 
      d2.calday,
      d1.customer_no,
      d1.sub_customer_code,
      d1.goods_code,
      d1.dc_code,
      d1.price_begin_date,
      d1.price_end_date,
      d1.sys_price,
      d1.sc_price,
      d1.customer_price,
      d1.price_type,
      d1.submit_time,
      d1.if_tmp_guide,
      d1.submit_by,
      d1.associate_no,
      regexp_replace(split(d1.submit_time, ' ')[0], '-', '') as submit_date,
      count(calday)over(partition by calday,customer_no,goods_code,dc_code) as num, -- 先选出一天有多次报价的商品
      row_number()over(partition by calday,customer_no,sub_customer_code,goods_code,dc_code order by submit_time) as zx_pm, -- 正序排名
      row_number()over(partition by calday,customer_no,sub_customer_code,goods_code,dc_code order by submit_time desc) as dx_pm -- 倒序排名
    from 
      (select 
        sdt,
        customer_no,
        sub_customer_code,
        goods_code,
        dc_code,
        price_begin_date,
        price_end_date,
        sys_price,
        sc_price,
        customer_price,
        price_type,
        submit_time,
        submit_by,
        associate_no,
        if_tmp_guide  
      from csx_analyse.csx_analyse_report_price_customer_price_guide_today_effect_di 
      where sdt>=regexp_replace(substr(date_add('${sdt_start_date}',-30),1,10),'-','')  and sdt<='${sdt_end}' 
      and price_end_date>='${sdt_start}'
      and sub_customer_code is not null and length(sub_customer_code)>0  -- 只取子客户有报价的数据
      ) d1 
      cross join 
      (select distinct calday 
      from csx_dim.csx_dim_basic_date 
      where calday>=regexp_replace(substr(date_add('${sdt_start_date}',-30),1,10),'-','')  
      and calday<=regexp_replace(substr(date_add('${sdt_end_date}',30),1,10),'-','')  -- 结束日期往后延1个月
      ) d2 
    where d1.price_begin_date<=d2.calday and d1.price_end_date>=d2.calday
    ) dd3 
  where dd3.num>1 and (zx_pm=1 or dx_pm=1)
  ) ddd4 
where ddd4.calday=ddd4.next_submit_date and ddd4.zx_pm=1;



-- ------------------------------------------------------------------------------------------
-- 主客户报价数据
drop table if exists csx_analyse_tmp.csx_analyse_tmp_cus_price_guide; 
create table if not exists csx_analyse_tmp.csx_analyse_tmp_cus_price_guide as 
select 
  a.* 
from 
(select 
  t2.sdt as order_date,
  t1.customer_no,
  t1.dc_code,
  t1.goods_code,
  t1.sys_price,
  t1.sc_price,
  t1.customer_price,
  t1.purchase_price,
  t1.price_begin_date,
  t1.price_end_date,
  t1.price_type,
  t1.associate_no,
  t1.if_tmp_guide,
  t1.submit_by,
  row_number()over(partition by t2.sdt,t1.customer_no,t1.dc_code,t1.goods_code order by t1.associate_no desc) as num     
 from 
  (select 
    b1.* 
   from 
      (select 
          tt.sdt,
          tt.customer_no,
          tt.goods_code,
          tt.dc_code,
          tt.price_begin_date,
          tt.price_end_date,
          tt.sys_price,
          tt.sc_price,
        tt.customer_price,
        tt.purchase_price,
        tt.price_type,
        tt.if_tmp_guide,
        tt.associate_no,
        tt.submit_by,
          (case when tt.price_end_date>=tt.next_price_begin_date then regexp_replace(date_add(from_unixtime(unix_timestamp(tt.next_price_begin_date, 'yyyyMMdd'), 'yyyy-MM-dd'),-1),'-','') else tt.price_end_date end) as fact_price_end_date
      from 
          (select 
            t1.*,
              lead(price_begin_date)over(partition by customer_no,goods_code order by price_begin_date) as next_price_begin_date 
           from 
            (select 
              sdt,
                customer_no,
                customer_name,
                goods_code,
                goods_name,
                dc_code,
                price_begin_date,
                price_end_date,
                sys_price,
              customer_price,
              purchase_price,
              price_type,
              associate_no,
              if_tmp_guide,
              sc_price,
              submit_by,
              row_number()over(partition by customer_no,goods_code,dc_code,price_begin_date,price_end_date order by submit_time desc) as pm
             from csx_analyse.csx_analyse_report_price_customer_price_guide_today_effect_di 
            where sdt>=regexp_replace(substr(date_add('${sdt_start_date}',-30),1,10),'-','')  and sdt<='${sdt_end}' 
            and price_end_date>='${sdt_start}' 
            and (sub_customer_code is null or length(sub_customer_code)=0) -- 只取没有子客户有报价的数据 
            ) t1 
          where t1.pm=1
          ) tt  
      ) b1 
    -- 去掉失效报价数据
    left join 
    (select 
      *,
      regexp_replace(split(price_begin_time, ' ')[0], '-', '') as price_begin_date,
      regexp_replace(split(price_end_time, ' ')[0], '-', '') as price_end_date,
      regexp_replace(split(update_time, ' ')[0], '-', '') as update_date   
     from csx_dwd.csx_dwd_price_customer_price_guide_invalid_di 
     where sdt>=regexp_replace(substr(date_add('${sdt_start_date}',-30),1,10),'-','')
    ) b4 
    on b1.associate_no=b4.associate_no
    -- and b1.customer_no=b4.customer_code and b1.dc_code=b4.warehouse_code and b1.goods_code=b4.product_code and b1.price_begin_date=b4.price_begin_date and b1.price_end_date=b4.price_end_date 
    where b4.update_date>='${sdt_start}' or b4.update_date is null 
  ) t1
  cross join 
  (select distinct calday as sdt 
  from csx_dim.csx_dim_basic_date 
  where calday>=regexp_replace(substr(date_add('${sdt_start_date}',-30),1,10),'-','') 
  and calday<=regexp_replace(substr(date_add('${sdt_end_date}',30),1,10),'-','') -- 结束日期往后延1个月
  ) t2 
  where t1.price_begin_date<=t2.sdt and t1.fact_price_end_date>=t2.sdt
) a 
where a.num=1;



-- ------------------------------------------------------------------------------------------
-- 主客户一天多次报价数据
drop table if exists csx_analyse_tmp.csx_analyse_tmp_cus_price_guide_more; 
create table if not exists csx_analyse_tmp.csx_analyse_tmp_cus_price_guide_more as 
select 
  ddd4.* 
 from 
  (select 
    dd3.*,
    lead(dd3.customer_price,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_customer_price,
    lead(dd3.submit_time,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_submit_time,
    lead(dd3.submit_date,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_submit_date,
    lead(dd3.price_begin_date,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_price_begin_date,
    lead(dd3.sc_price,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_sc_price,
    lead(dd3.sys_price,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_sys_price,
    lead(dd3.if_tmp_guide,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_if_tmp_guide,
    lead(dd3.price_type,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_price_type,
    lead(dd3.submit_by,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_submit_by,
    lead(dd3.associate_no,1)over(partition by dd3.customer_no,dd3.goods_code,dd3.calday,dd3.dc_code order by dd3.submit_time) as next_associate_no      
   from 
    (select 
      d2.calday,
      d1.customer_no,
      d1.goods_code,
      d1.dc_code,
      d1.price_begin_date,
      d1.price_end_date,
      d1.sys_price,
      d1.sc_price,
      d1.customer_price,
      d1.price_type,
      d1.submit_time,
      d1.if_tmp_guide,
      d1.submit_by,
      d1.associate_no,
      regexp_replace(split(d1.submit_time, ' ')[0], '-', '') as submit_date,
      count(calday)over(partition by calday,customer_no,goods_code,dc_code) as num, -- 先选出一天有多次报价的商品
      row_number()over(partition by calday,customer_no,goods_code,dc_code order by submit_time) as zx_pm, -- 正序排名
      row_number()over(partition by calday,customer_no,goods_code,dc_code order by submit_time desc) as dx_pm -- 倒序排名
    from 
      (select 
        sdt,
        customer_no,
        goods_code,
        dc_code,
        price_begin_date,
        price_end_date,
        sys_price,
        sc_price,
        customer_price,
        price_type,
        submit_time,
        if_tmp_guide,
        submit_by,
        associate_no   
      from csx_analyse.csx_analyse_report_price_customer_price_guide_today_effect_di 
      where sdt>=regexp_replace(substr(date_add('${sdt_start_date}',-30),1,10),'-','')  and sdt<='${sdt_end}' 
      and price_end_date>='${sdt_start}' 
      and (sub_customer_code is null or length(sub_customer_code)=0)  -- 只取没有子客户有报价的数据
      ) d1 
      cross join 
      (select distinct calday 
      from csx_dim.csx_dim_basic_date 
      where calday>=regexp_replace(substr(date_add('${sdt_start_date}',-30),1,10),'-','') 
      and calday<=regexp_replace(substr(date_add('${sdt_end_date}',30),1,10),'-','') -- 结束日期往后延1个月
      ) d2 
    where d1.price_begin_date<=d2.calday and d1.price_end_date>=d2.calday
    ) dd3 
  where dd3.num>1 and (zx_pm=1 or dx_pm=1)
  ) ddd4 
where ddd4.calday=ddd4.next_submit_date and ddd4.zx_pm=1;



-- -------------------------------------------------------------------------------
-- ------生成报价监控相应底表数据
drop table if exists csx_analyse_tmp.csx_analyse_tmp_cus_price_guide_order; 
create table if not exists csx_analyse_tmp.csx_analyse_tmp_cus_price_guide_order as 
select 
  a.order_date,
  a.require_delivery_date,
  a.final_qj_date,
  a.performance_region_name,
  a.performance_province_name,
  a.performance_city_name,
  a.customer_code,
  a.customer_name,
  a.sub_customer_code,
  a.sub_customer_name,
  h.first_category_name,
  h.second_category_name,
  a.order_code,
  a.goods_code,
  a.goods_name,
  g.classify_large_name,
  g.classify_middle_name,
  g.classify_small_name,
  a.inventory_dc_code,
  (case when b.dc_code is not null then '是' else '否' end) as if_zs,
  (case when c.dc_code is not null then '是' else '否' end) as if_zc,
  a.delivery_type_name,
  a.sale_price,
  a.sale_qty,
  a.sale_amt,
  a.profit,
  
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
        when length(e2.sub_customer_code)>0 and a.final_qj_time<e2.next_submit_time then e2.sc_price 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_submit_time and a.final_qj_time<e2.next_price_begin_date then 0 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_price_begin_date then e2.next_sc_price 

        -- 子客户一次报价
        when length(e1.sub_customer_code)>0 then e1.sc_price 

        -- 主客户多次报价
        when length(e4.customer_no)>0 and a.final_qj_time<e4.next_submit_time then e4.sc_price 
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_submit_time and a.final_qj_time<e4.next_price_begin_date then 0 
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_price_begin_date then e4.next_sc_price 

        -- 主客户一次报价
        when length(e3.customer_no)>0 then e3.sc_price end) as sc_price,



  (case -- 子客户多次报价 
        when length(e2.sub_customer_code)>0 and a.final_qj_time<e2.next_submit_time then e2.sys_price 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_submit_time and a.final_qj_time<e2.next_price_begin_date then 0 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_price_begin_date then e2.next_sys_price 

        -- 子客户一次报价
        when length(e1.sub_customer_code)>0 then e1.sys_price 

        -- 主客户多次报价
        when length(e4.customer_no)>0 and a.final_qj_time<e4.next_submit_time then e4.sys_price 
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_submit_time and a.final_qj_time<e4.next_price_begin_date then 0 
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_price_begin_date then e4.next_sys_price 

        -- 主客户一次报价
        when length(e3.customer_no)>0 then e3.sys_price end) as sys_price,

  (case -- 子客户多次报价 
        when length(e2.sub_customer_code)>0 and a.final_qj_time<e2.next_submit_time then e2.if_tmp_guide 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_submit_time and a.final_qj_time<e2.next_price_begin_date then '否' 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_price_begin_date then e2.next_if_tmp_guide

        -- 子客户一次报价
        when length(e1.sub_customer_code)>0 then e1.if_tmp_guide 

        -- 主客户多次报价
        when length(e4.customer_no)>0 and a.final_qj_time<e4.next_submit_time then e4.if_tmp_guide 
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_submit_time and a.final_qj_time<e4.next_price_begin_date then '否' 
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_price_begin_date then e4.next_if_tmp_guide 

        -- 主客户一次报价
        when length(e3.customer_no)>0 then e3.if_tmp_guide end) as if_tmp_guide,  


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
        when length(e3.customer_no)>0 then e3.price_type end) as price_type,  

  f.price_source,
  (case when f.price_source='手工调整价' then '否' else '是' end) as if_sg,
  a.sdt,

 (case -- 子客户多次报价 
        when length(e2.sub_customer_code)>0 and a.final_qj_time<e2.next_submit_time then e2.submit_by 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_submit_time and a.final_qj_time<e2.next_price_begin_date then '无' 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_price_begin_date then e2.next_submit_by

        -- 子客户一次报价
        when length(e1.sub_customer_code)>0 then e1.submit_by 

        -- 主客户多次报价
        when length(e4.customer_no)>0 and a.final_qj_time<e4.next_submit_time then e4.submit_by 
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_submit_time and a.final_qj_time<e4.next_price_begin_date then '无' 
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_price_begin_date then e4.next_submit_by 

        -- 主客户一次报价
        when length(e3.customer_no)>0 then e3.submit_by end) as submit_by,  

  (case when a.type='调价单' then '是' else '否' end) as if_tj,


  (case -- 子客户多次报价 
        when length(e2.sub_customer_code)>0 and a.final_qj_time<e2.next_submit_time then e2.associate_no 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_submit_time and a.final_qj_time<e2.next_price_begin_date then '无' 
        when length(e2.sub_customer_code)>0 and a.final_qj_time>=e2.next_price_begin_date then e2.next_associate_no

        -- 子客户一次报价
        when length(e1.sub_customer_code)>0 then e1.associate_no 

        -- 主客户多次报价
        when length(e4.customer_no)>0 and a.final_qj_time<e4.next_submit_time then e4.associate_no 
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_submit_time and a.final_qj_time<e4.next_price_begin_date then '无' 
        when length(e4.customer_no)>0 and a.final_qj_time>=e4.next_price_begin_date then e4.next_associate_no 

        -- 主客户一次报价
        when length(e3.customer_no)>0 then e3.associate_no end) as associate_no 
from 
  (select 
      a1.*,
      (case when a1.fir_qj_date<a2.refresh_date and a2.refresh_date is not null then a2.refresh_date else a1.fir_qj_date end) as final_qj_date,
      (case when a1.fir_qj_time<a2.refresh_time and a2.refresh_time is not null then a2.refresh_time else a1.fir_qj_time end) as final_qj_time,
      (case when a3.original_order_code is not null then '调价单' else '非调价单' end) as type  
   from 
    (-- 非调剂返利单
    select 
        *,
        regexp_replace(substr(order_time,1,10),'-','') as order_date,
        regexp_replace(substr(delivery_time,1,10),'-','') as delivery_date,
        (case when require_delivery_date<regexp_replace(substr(order_time,1,10),'-','') then regexp_replace(substr(order_time,1,10),'-','') else require_delivery_date end) as fir_qj_date,
        (case when require_delivery_date<=regexp_replace(substr(order_time,1,10),'-','') then order_time 
              when require_delivery_date>regexp_replace(substr(order_time,1,10),'-','') then from_unixtime(unix_timestamp(require_delivery_date,'yyyyMMdd'),'yyyy-MM-dd HH:mm:ss') end) as fir_qj_time 
    from csx_dws.csx_dws_sale_detail_di  
    where sdt>='${sale_sdt_start}' 
    and sdt<='${sale_sdt_end}' 
    -- and regexp_replace(substr(order_time,1,10),'-','')>='${sdt_start}' 
    and delivery_type_name='配送' 
    and business_type_code=1 
    and order_channel_code not in (4,6)  
    and refund_order_flag=0 -- 剔除退单数据 
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
    left join 
    (select 
        '调价单' as type,
        *,
        regexp_replace(substr(order_time,1,10),'-','') as order_date,
        regexp_replace(substr(delivery_time,1,10),'-','') as delivery_date,
        (case when require_delivery_date<regexp_replace(substr(order_time,1,10),'-','') then regexp_replace(substr(order_time,1,10),'-','') else require_delivery_date end) as fir_qj_date 
    from csx_dws.csx_dws_sale_detail_di  
    where sdt>='${sdt_start}' 
    and sdt<='${sdt_end}' 
    and regexp_replace(substr(order_time,1,10),'-','')>='${sdt_start}' 
    and delivery_type_name='配送' 
    and business_type_code=1 
    and order_channel_code=6 
    and refund_order_flag=0 -- 剔除退单数据
    ) a3 
    on a1.order_code=a3.original_order_code 
  ) a 
  left join 
  -- 看是否是直送仓
  (select distinct shop_code as dc_code 
  from csx_dim.csx_dim_shop  
  where sdt='current' 
  and shop_low_profit_flag=1 ) b 
  on a.inventory_dc_code=b.dc_code  
  left join 
  -- 看是否是主仓
  csx_ods.csx_ods_data_analysis_prd_city_main_dc_df c 
  on a.inventory_dc_code=c.dc_code  
  -- left join 
  -- -- 匹配价格来源
  -- (select 
  --   * 
  --  from 
  --   (select 
  --     *,
  --     row_number()over(partition by order_code,goods_code order by item_create_time desc) as pm  
  --   from csx_dwd.csx_dwd_csms_yszx_order_detail_di 
  --   where (sdt >=regexp_replace(add_months(trunc('${sdt_start_date}','MM'),-2), '-', '') OR sdt = '19990101')  
  --   ) d1 
  -- where d1.pm=1) d 
  -- on a.original_order_code = d.order_code and a.goods_code=d.goods_code 
  left join 
  -- 子客户报价相关数据
  (select * 
  from csx_analyse_tmp.csx_analyse_tmp_sub_cus_price_guide 
  where num=1) e1 
  on a.final_qj_date=e1.order_date and a.customer_code=e1.customer_no and a.goods_code=e1.goods_code and a.inventory_dc_code=e1.dc_code and a.sub_customer_code=e1.sub_customer_code 
  left join
  -- 子客户一天有多次报价的数据处理
  csx_analyse_tmp.csx_analyse_tmp_sub_cus_price_guide_more e2 
  on a.final_qj_date=e2.calday and a.customer_code=e2.customer_no and a.goods_code=e2.goods_code and a.inventory_dc_code=e2.dc_code and a.sub_customer_code=e2.sub_customer_code 
  left join 
  -- 主客户报价相关数据
  (select * 
  from csx_analyse_tmp.csx_analyse_tmp_cus_price_guide 
  where num=1) e3  
  on a.final_qj_date=e3.order_date and a.customer_code=e3.customer_no and a.goods_code=e3.goods_code and a.inventory_dc_code=e3.dc_code 
  left join
  -- 主客户一天有多次报价的数据处理
  csx_analyse_tmp.csx_analyse_tmp_cus_price_guide_more e4 
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
      ELSE sale_price_explain END AS price_source 
  from csx_dwd.csx_dwd_csms_yszx_order_detail_di 
  where (sdt >= regexp_replace(add_months(trunc('${sdt_start_date}','MM'),-2), '-', '') OR sdt = '19990101')
  ) f 
  on a.original_order_code = f.order_code and a.goods_code=f.goods_code 
  left join 
  -- 商品码表
  (select * 
  from csx_dim.csx_dim_basic_goods 
  where sdt='current'
  ) g 
  on a.goods_code=g.goods_code 
  left join 
  -- 客户行业数据
  (select * 
  from csx_dim.csx_dim_crm_customer_info 
  where sdt='current'
  ) h 
  on a.customer_code=h.customer_code 


-- ---------------------------------------------------
-- --生成最终明细底表(明秀)
drop table if exists csx_analyse_tmp.csx_analyse_tmp_cus_price_guide_order_final; 
create table if not exists csx_analyse_tmp.csx_analyse_tmp_cus_price_guide_order_final as 
select 
  a.order_date,
  a.require_delivery_date,
  a.final_qj_date,
  a.performance_region_name,
  a.performance_province_name,
  a.performance_city_name,
  a.customer_code,
  a.customer_name,
  a.sub_customer_code,
  a.sub_customer_name,
  a.first_category_name,
  a.second_category_name,
  a.order_code,
  a.goods_code,
  a.goods_name,
  a.classify_large_name,
  a.classify_middle_name,
  a.classify_small_name,
  a.inventory_dc_code,
  a.if_zs,
  a.if_zc,
  a.delivery_type_name,
  a.sale_price,
  a.sale_qty,
  a.sale_amt,
  a.profit,
  nvl(a.customer_price,0) as customer_price,
  nvl(a.sc_price,0) as sc_price,
  nvl((case when price_type like '%自动报价%' then a.sc_price else a.sys_price end),0) as sys_price,
  (case when a.if_tmp_guide is null then '无客户报价' else a.if_tmp_guide end) as if_tmp_guide,
  (case when a.price_type is null then '无客户报价' else a.price_type end) as price_type,
  a.price_source,
  a.if_sg,
  (case when a.sc_price>0 and price_type like '%自动报价%' then (a.sale_price-a.sc_price)/a.sale_price 
        when a.sys_price>0 and price_type not like '%自动报价%' then (a.sale_price-a.sys_price)/a.sale_price end) as price_diff,
  (case when (a.sc_price>0 and price_type like '%自动报价%' and abs((a.sale_price-a.sc_price)/a.sale_price)<=0.01) 
             or (a.sys_price>0 and price_type not like '%自动报价%' and abs((a.sale_price-a.sys_price)/a.sale_price)<=0.01) 
             or a.price_source='库存地点售价' then '合格' 
        -- when a.sc_price=0 then '合格' 
        else '不合格' end) as if_hg,
  (case when a.customer_price>0 then '有' else '无' end) as if_hav_cus_price,
  a.sdt,
  (case when a.submit_by is not null then a.submit_by end) as submit_by,
  (case when b1.employee_name is not null then b1.position_name 
        when b2.employee_name is not null then b2.position_name end) as submit_position,
  (case when a.sc_price>0 then (a.sale_price-a.sc_price)/a.sc_price end) as sc_price_diff,
  a.if_tj,
  c.fir_price_type,
  c.sec_price_type,
  (case when a.price_source='指定成交价' and a.if_tmp_guide is null then 1 else 0 end) as if_tmp_price_tc  -- 临时报价占比中剔除的数据
from 
csx_analyse_tmp.csx_analyse_tmp_cus_price_guide_order a 
left join -- 是否是运营
(select employee_name,max(position_name) as position_name  
from csx_dim.csx_dim_basic_employee 
where sdt='current' 
and employee_status='3' 
and position_name like '%运营%' 
group by employee_name) b1 
on a.submit_by=b1.employee_name 
left join -- 是否是运营
(select employee_name,max(position_name) as position_name  
from csx_dim.csx_dim_basic_employee 
where sdt='current' 
and employee_status='3' 
and position_name not like '%运营%' 
group by employee_name) b2 
on a.submit_by=b2.employee_name 
left join -- 客户定价方式
csx_analyse.csx_analyse_price_customer_price_type_df c 
on a.customer_code=c.customer_no and a.inventory_dc_code=c.dc_code ;



-- ---------------------------------------------------
-- --生成最终明细表（调整版，和明秀对完逻辑的数据）
drop table if exists csx_analyse_tmp.csx_analyse_tmp_cus_price_guide_order_final_supple; 
create table if not exists csx_analyse_tmp.csx_analyse_tmp_cus_price_guide_order_final_supple as 

select 
  a.*,
  b.shop_price 
from 
csx_analyse_tmp.csx_analyse_tmp_cus_price_guide_order_final a 
left join 
-- 关联北京永辉工厂价
(select 
        b2.sdt,
        b1.dc_code,
        b1.shop_code,
        b1.goods_code,
        max(b1.price) as shop_price  
from 
          (select 
              t1.*,
              t2.product_code as goods_code,
              t2.location_code as dc_code   
           from 
              (select 
                  *,
                  regexp_replace(substr(price_begin_time,1,10),'-','') as price_begin_date,
                  regexp_replace(substr(price_end_time,1,10),'-','') as price_end_date 
              from csx_ods.csx_ods_csx_price_prod_market_research_not_yh_price_di  
              where sdt>=regexp_replace(substr(date_add('${sdt_start_date}',-30),1,10),'-','')  
              and shop_code='YW121' 
              ) t1 
              left join 
              (select * 
              from csx_ods.csx_ods_csx_price_prod_market_research_product_df 
              where sdt='${yes_sdt}'
              ) t2 
              on t1.product_id=t2.id 
          ) b1 
          cross join 
          (select distinct calday as sdt 
            from csx_dim.csx_dim_basic_date 
            where calday>='${sdt_start}' 
            and calday<=regexp_replace(substr(date_add('${sdt_end_date}',30),1,10),'-','') -- 结束日期往后延1个月
          ) b2 
where b1.price_begin_date<=b2.sdt and b1.price_end_date>=b2.sdt 
group by 
        b2.sdt,
        b1.dc_code,
        b1.shop_code,
        b1.goods_code
) b 
on a.final_qj_date=b.sdt and a.inventory_dc_code=b.dc_code and a.goods_code=b.goods_code       
left join 
-- 关联客户报价方式
csx_analyse.csx_analyse_price_customer_price_type_df c 
on a.customer_code=c.customer_no and a.inventory_dc_code=c.dc_code 

-- ---------------------------------------------------
-- --生成最终汇总表（调整版，和明秀对完逻辑的数据）
select 
    performance_region_name as `大区`,
    performance_province_name as `省区`,
    performance_city_name as `城市`,
    customer_code as `客户编码`,
    customer_name as `客户名称`,
    first_category_name as `一级客户分类`,
    second_category_name as `二级客户分类`,
    classify_middle_name as `管理中类名称`,
    goods_code as `商品编码`,
    goods_name as `商品名称`,
    price_source as `订单价格来源`,
    price_type as `定价类型`,
    (case when sec_price_type='全对标' then '对标对象' 
          when price_source='对标市调' and shop_price=sale_price then '建议售价' 
          when price_source like '对标%' then '对标对象' 
          when price_source='指定成交价' then price_type 
          when price_source='库存地点售价' then '建议售价' 
     else '其他报价方式' end) as `定价类型新`,
      price_type as `定价类型`,
    sum(sale_amt) as `销售额`,
    sum(profit) as `毛利额`,
    sum(sale_qty) as `销量`  
from csx_analyse_tmp.csx_analyse_tmp_cus_price_guide_order_final_supple 
group by 
    performance_region_name,
    performance_province_name,
    performance_city_name,
    customer_code,
    customer_name,
    first_category_name,
    second_category_name,
    classify_middle_name,
    goods_code,
    goods_name,
    price_source,
    price_type,
    (case when sec_price_type='全对标' then '对标对象' 
          when price_source='对标市调' and shop_price=sale_price then '建议售价' 
          when price_source like '对标%' then '对标对象' 
          when price_source='指定成交价' then price_type 
          when price_source='库存地点售价' then '建议售价' 
     else '其他报价方式' end)
