--三个条件：
--2、好的中台报价，订单商品明细取前端毛利率（0，0.08】的
--3、出库日期+DC销售额top800
--4、中台报价高于成本价

-- 昨日、昨日、14天前、14天前
--select ${hiveconf:i_sdate_11},${hiveconf:i_sdate_12},${hiveconf:i_sdate_21},${hiveconf:i_sdate_22};

set i_sdate_11 =regexp_replace(date_sub(current_date,1),'-','');  --昨日
set i_sdate_12 =date_sub(current_date,1);  --昨日
set i_sdate_21 =regexp_replace(date_sub(current_date,1+14),'-','');
set i_sdate_22 =date_sub(current_date,1+14);  


	
--01、AI覆盖情况
insert overwrite directory '/tmp/raoyanhua/01fugai' row format delimited fields terminated by '\t'
select 
  c.classify_middle_code,
  c.classify_middle_name,   
  count(distinct a.goods_code) sku,
  count(distinct case when a.business_type_code='1' then a.goods_code end) sku_ripei,
  count(distinct case when a.business_type_code='1' then b.goods_code end) sku_ripei_AI,
  count(distinct case when a.business_type_code='1' then d.goods_code end) sku_ripei_top800,  
  count(distinct case when a.business_type_code='1' then e.goods_code end) sku_ripei_mid_good,
  count(distinct case when a.business_type_code='1' then f.goods_code end) sku_ripei_cost_bad,
  count(distinct case when a.business_type_code='1' and d.goods_code is not null then g.goods_code end) sku_ripei_S,
  
  sum(a.sales_value) sales_value,
  sum(a.profit) profit,
  cast(sum(a.profit)/sum(a.sales_value) as decimal(30,6)) prorate,
  sum(if(a.business_type_code='1',a.sales_value,0)) sales_value_ripei,
  sum(if(a.business_type_code='1',a.profit,0)) profit_ripei, 
  cast(sum(if(a.business_type_code='1',a.profit,0))/sum(if(a.business_type_code='1',a.sales_value,0)) as decimal(30,6)) prorate_ripei,
  
  sum(if(a.business_type_code='1' and b.goods_code is not null,a.sales_value,0)) sales_value_ripei_AI,
  sum(if(a.business_type_code='1' and b.goods_code is not null,a.profit,0)) profit_ripei_AI,
  cast(sum(if(a.business_type_code='1' and b.goods_code is not null,a.profit,0))/
  sum(if(a.business_type_code='1' and b.goods_code is not null,a.sales_value,0)) as decimal(30,6)) prorate_ripei_AI,
  
  sum(if(a.business_type_code='1' and d.goods_code is not null,a.sales_value,0)) sales_value_ripei_top800,
  sum(if(a.business_type_code='1' and d.goods_code is not null,a.profit,0)) profit_ripei_top800,
  cast(sum(if(a.business_type_code='1' and d.goods_code is not null,a.profit,0))/
  sum(if(a.business_type_code='1' and d.goods_code is not null,a.sales_value,0)) as decimal(30,6)) prorate_ripei_top800,  
  
  sum(if(a.business_type_code='1' and e.goods_code is not null,a.sales_value,0)) sales_value_ripei_mid_good,
  sum(if(a.business_type_code='1' and e.goods_code is not null,a.profit,0)) profit_ripei_mid_good,
  cast(sum(if(a.business_type_code='1' and e.goods_code is not null,a.profit,0))/
  sum(if(a.business_type_code='1' and e.goods_code is not null,a.sales_value,0)) as decimal(30,6)) prorate_ripei_mid_good,  

  sum(if(a.business_type_code='1' and f.goods_code is not null,a.sales_value,0)) sales_value_ripei_cost_bad,
  sum(if(a.business_type_code='1' and f.goods_code is not null,a.profit,0)) profit_ripei_cost_bad,
  cast(sum(if(a.business_type_code='1' and f.goods_code is not null,a.profit,0))/
  sum(if(a.business_type_code='1' and f.goods_code is not null,a.sales_value,0)) as decimal(30,6)) prorate_ripei_cost_bad,  
  
  sum(if(a.business_type_code='1' and d.goods_code is not null and g.goods_code is not null,a.sales_value,0)) sales_value_ripei_S,
  sum(if(a.business_type_code='1' and d.goods_code is not null and g.goods_code is not null,a.profit,0)) profit_ripei_S,
  cast(sum(if(a.business_type_code='1' and d.goods_code is not null and g.goods_code is not null,a.profit,0))/
  sum(if(a.business_type_code='1' and d.goods_code is not null and g.goods_code is not null,a.sales_value,0)) as decimal(30,6)) prorate_ripei_S
  
from
(select 
  substr(shipped_time,1,10) sdt,business_type_code,
  classify_middle_code,classify_middle_name, 
  dc_code,goods_code,goods_name,
  sum(sales_qty) sales_qty,
  sum(sales_value) sales_value,
  sum(sales_cost) sales_cost,
  sum(profit) profit
from csx_dw.dws_sale_r_d_detail 
     where sdt >= ${hiveconf:i_sdate_21} --and sdt<=${hiveconf:i_sdate_11}
	 and substr(shipped_time,1,10)>= ${hiveconf:i_sdate_22} and  substr(shipped_time,1,10)<${hiveconf:i_sdate_12}
	and channel_code in ('1', '7', '9')	
	--and business_type_code='1'
	and sales_type<>'fanli'
	and return_flag<>'X'
    and province_name='重庆市'
group by substr(shipped_time,1,10),business_type_code,  
  classify_middle_code,classify_middle_name,
  dc_code,goods_code,goods_name
)a
left join
 (select distinct goods_code,dc_code
  --,delivery_dt,middle_office_price
  from  csx_ods.pred_table 
  where sdt=${hiveconf:i_sdate_11}
  --group by goods_code,dc_code,delivery_dt,middle_office_price
  )b on b.goods_code=a.goods_code and a.dc_code=b.dc_code 
  --and b.delivery_dt=a.sdt and a.middle_office_price=b.middle_office_price
join
  (
  select goods_id,goods_name,classify_middle_code,classify_middle_name		 
  from csx_dw.dws_basic_w_a_csx_product_m 
  where sdt='current' 
  and classify_middle_code in('B0101','B0102','B0103','B0201','B0202','B0301','B0302','B0304','B0501','B0602') 
)c on c.goods_id=a.goods_code
--筛选1：出库日期+DC销售额top800
left join
(select distinct business_type_code,dc_code,goods_code
  from 
  (
  select 
    substr(shipped_time,1,10) sdt,business_type_code,dc_code,goods_code,
    sum(sales_qty) sales_qty,
    sum(sales_value) sales_value,
    sum(sales_cost) sales_cost,
    sum(profit) profit,
    row_number() over (partition by substr(shipped_time,1,10), dc_code order by sum(sales_value) desc) as rank_num
  from csx_dw.dws_sale_r_d_detail 
       where sdt >= ${hiveconf:i_sdate_21} --and sdt<=${hiveconf:i_sdate_11}
  	 and substr(shipped_time,1,10)>= ${hiveconf:i_sdate_22} and  substr(shipped_time,1,10)<${hiveconf:i_sdate_12}
  	and channel_code in ('1', '7', '9')	
  	and business_type_code='1'
  	and sales_type<>'fanli'
  	and return_flag<>'X'
      and province_name='重庆市'
  group by substr(shipped_time,1,10),business_type_code,dc_code,goods_code
  )d1
  where rank_num<=800
)d on d.goods_code=a.goods_code and a.dc_code=d.dc_code and a.business_type_code=d.business_type_code
--筛选2：好的中台报价，订单商品明细取前端毛利率（0，0.08]
left join
(select distinct business_type_code,dc_code,goods_code
  from
  ( 
  select 
    substr(shipped_time,1,10) sdt,business_type_code,dc_code,goods_code,
    --if(0<(sales_price - middle_office_price)/sales_price<=0.08,if(cost_price<middle_office_price,"前端毛利好,成本不高于中台价","成本高于中台价"),"前端毛利不好") flag_price, --标签
    sum(sales_qty) sales_qty,
    sum(sales_value) sales_value,
    sum(sales_cost) sales_cost,
    sum(profit) profit
  from csx_dw.dws_sale_r_d_detail 
       where sdt >= ${hiveconf:i_sdate_21} --and sdt<=${hiveconf:i_sdate_11}
  	 and substr(shipped_time,1,10)>= ${hiveconf:i_sdate_22} and  substr(shipped_time,1,10)<${hiveconf:i_sdate_12}
  	and channel_code in ('1', '7', '9')	
  	and business_type_code='1'
  	and sales_type<>'fanli'
  	and return_flag<>'X'
      and province_name='重庆市'
  	and cast(((sales_price - middle_office_price)/sales_price) as decimal(30,6))>0
  	and cast(((sales_price - middle_office_price)/sales_price) as decimal(30,6))<=0.08
  group by substr(shipped_time,1,10),business_type_code,dc_code,goods_code
  )e1
)e on e.goods_code=a.goods_code and a.dc_code=e.dc_code and a.business_type_code=e.business_type_code
--筛选3：中台报价高于成本价
left join
(select distinct business_type_code,dc_code,goods_code
  from
  (
  select 
    substr(shipped_time,1,10) sdt,business_type_code,
    dc_code,goods_code,
    sum(sales_qty) sales_qty,
    sum(sales_value) sales_value,
    sum(sales_cost) sales_cost,
    sum(profit) profit
  from csx_dw.dws_sale_r_d_detail 
       where sdt >= ${hiveconf:i_sdate_21} --and sdt<=${hiveconf:i_sdate_11}
  	 and substr(shipped_time,1,10)>= ${hiveconf:i_sdate_22} and  substr(shipped_time,1,10)<${hiveconf:i_sdate_12}
  	and channel_code in ('1', '7', '9')	
  	and business_type_code='1'
  	and sales_type<>'fanli'
  	and return_flag<>'X'
      and province_name='重庆市'
  	and cost_price<middle_office_price
  group by substr(shipped_time,1,10),business_type_code,dc_code,goods_code
  )f1  
)f on f.goods_code=a.goods_code and a.dc_code=f.dc_code and a.business_type_code=f.business_type_code
--筛选2&筛选3
left join
(select distinct business_type_code,dc_code,goods_code
  from
  (
  select 
    substr(shipped_time,1,10) sdt,business_type_code,dc_code,goods_code,
    --if(0<(sales_price - middle_office_price)/sales_price<=0.08,if(cost_price<middle_office_price,"前端毛利好,成本不高于中台价","成本高于中台价"),"前端毛利不好") flag_price, --标签
    sum(sales_qty) sales_qty,
    sum(sales_value) sales_value,
    sum(sales_cost) sales_cost,
    sum(profit) profit
  from csx_dw.dws_sale_r_d_detail 
       where sdt >= ${hiveconf:i_sdate_21} --and sdt<=${hiveconf:i_sdate_11}
  	 and substr(shipped_time,1,10)>= ${hiveconf:i_sdate_22} and  substr(shipped_time,1,10)<${hiveconf:i_sdate_12}
  	and channel_code in ('1', '7', '9')	
  	and business_type_code='1'
  	and sales_type<>'fanli'
  	and return_flag<>'X'
      and province_name='重庆市'
  	and cast(((sales_price - middle_office_price)/sales_price) as decimal(30,6))>0
  	and cast(((sales_price - middle_office_price)/sales_price) as decimal(30,6))<=0.08
  	and cost_price<middle_office_price
  group by substr(shipped_time,1,10),business_type_code,dc_code,goods_code
  )g1
)g on g.goods_code=a.goods_code and a.dc_code=g.dc_code and a.business_type_code=g.business_type_code 
group by c.classify_middle_code,c.classify_middle_name;


----日配AI剔除商品TOP
--insert overwrite directory '/tmp/raoyanhua/01.2tichu_top_goods' row format delimited fields terminated by '\t'
--select 
--  c.classify_middle_code,
--  c.classify_middle_name, 
--  a.goods_code,c.goods_name,  
--  sum(sales_value) sales_value,
--  sum(profit) profit,
--  sum(profit)/sum(sales_value) prorate
--from
--(select 
--  substr(shipped_time,1,10) sdt,business_type_code,
--  classify_middle_code,classify_middle_name, 
--  dc_code,goods_code,goods_name,
--  sum(sales_qty) sales_qty,
--  sum(sales_value) sales_value,
--  sum(sales_cost) sales_cost,
--  sum(profit) profit
--from csx_dw.dws_sale_r_d_detail 
--     where sdt >= ${hiveconf:i_sdate_21} --and sdt<=${hiveconf:i_sdate_11}
--	 and substr(shipped_time,1,10)>= ${hiveconf:i_sdate_22} and  substr(shipped_time,1,10)<${hiveconf:i_sdate_12}
--	and channel_code in ('1', '7', '9')	
--	and business_type_code='1'
--	and sales_type<>'fanli'
--	and return_flag<>'X'
--    and province_name='重庆市'
--group by substr(shipped_time,1,10),business_type_code,  
--  classify_middle_code,classify_middle_name,
--  dc_code,goods_code,goods_name
--)a
--left join
-- (select distinct goods_code,dc_code
--  from  csx_ods.pred_table 
--  where sdt=${hiveconf:i_sdate_11}
--  )b on b.goods_code=a.goods_code and a.dc_code=b.dc_code 
--join
--  (
--  select goods_id,goods_name,classify_middle_code,classify_middle_name		 
--  from csx_dw.dws_basic_w_a_csx_product_m 
--  where sdt='current' 
--  and classify_middle_code in('B0101','B0102','B0103','B0201','B0202','B0301','B0302','B0304','B0501','B0602') 
--)c on c.goods_id=a.goods_code
--where b.goods_code is null
--group by c.classify_middle_code,c.classify_middle_name,a.goods_code,c.goods_name;


----AI剔除TOP商品
--insert overwrite directory '/tmp/raoyanhua/01.2tichu_top_goods' row format delimited fields terminated by '\t'
--select business_type_name,channel_name,province_name,
--dc_code,dc_name,customer_no,customer_name,channel_name,
--first_category_name,sales_name,work_no,
--order_time,goods_code,goods_name,origin_order_no,order_no,
--purchase_price_flag,cost_price,purchase_price,middle_office_price,sales_price,
--order_qty,sales_qty,sales_value,sales_cost,profit,front_profit,return_flag,logistics_mode_name,order_category_desc,sdt,sales_type
--from csx_dw.dws_sale_r_d_detail 
--where sdt >= ${hiveconf:i_sdate_21} --and sdt<=${hiveconf:i_sdate_11}
--and substr(shipped_time,1,10)>= ${hiveconf:i_sdate_22} and  substr(shipped_time,1,10)<${hiveconf:i_sdate_12}
--and channel_code in ('1', '7', '9')	
--and business_type_code='1'
--and sales_type<>'fanli'
--and return_flag<>'X'
--and province_name='重庆市'
--and goods_code in('1128044','1180404','1301361','1182510','1178832','1128058','1301376','1287832','1159463','1027428','1370716','1215327','852439','1360286','238914','1360285');

	
	


--02预测波动
drop table csx_tmp.tmp_AI_01;
create table csx_tmp.tmp_AI_01
as
select *,
case when error_percent_1>=shxian then concat('误差为',round(error_percent_1,2),'点 大于等于 上限值',shxian,'%')
    when error_percent_1<shxian and error_percent_1>=0 then concat('误差为',round(error_percent_1,2),'点 小于 上限值',shxian,'% 大于',0)
	when error_percent_1>=xiaxian and error_percent_1<0 then concat('误差为',round(error_percent_1,2),'点 小于',0,'大于 下限值',xiaxian,'%')
	when error_percent_1<xiaxian then concat('误差为',round(error_percent_1,2),'点 小于 下限值',xiaxian,'%') end flag_1,	

case when error_percent>=shxian then 3
    when error_percent<shxian and error_percent>=0 then 2
	when error_percent>=xiaxian and error_percent<0 then 1
	when error_percent<xiaxian then 0 end flag_group,
	
case when error_percent_1>=shxian then 3
    when error_percent_1<shxian and error_percent_1>=0 then 2
	when error_percent_1>=xiaxian and error_percent_1<0 then 1
	when error_percent_1<xiaxian then 0 end flag_group_1
from 
(
select 
  delivery_dt,
  a.dc_code,
  a.goods_code,
  goods_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  classify_small_code,
  classify_small_name,
  cost_price
  ,stock_price_last_0_day
  ,stock_vs_previous_rate
  ,error_percent,
  prediction_score,
  middle_office_price,
  b.csx_distribute_price,
  c.csx_cost_price,
  j.rank_num,
  j.sales_value as sales_value_14,
  row_number() over (partition by a.dc_code order by j.sales_value desc) as rank_num_AI,
  xiaxian,
  shxian,
  if(c.goods_code is not null,'是','否') as is_sale,
  if(k.goods_code is not null,'是','否') as is_good_midprice_current,
  if(e.goods_code is not null,'是','否') as is_good_midprice,
  if(d.goods_code is not null and g.goods_code is not null,'是','否') is_3select,
  --e.middle_office_price,
  round((prediction_score-b.csx_distribute_price)/b.csx_distribute_price*100,2) as error_percent_1,
case when error_percent>=shxian then concat('误差为',round(error_percent,2),'点 大于等于 上限值',shxian,'%')
    when error_percent<shxian and error_percent>=0 then concat('误差为',round(error_percent,2),'点 小于 上限值',shxian,'% 大于',0)
	when error_percent>=xiaxian and error_percent<0 then concat('误差为',round(error_percent,2),'点 小于',0,'大于 下限值',xiaxian,'%')
	when error_percent<xiaxian then concat('误差为',round(error_percent,2),'点 小于 下限值',xiaxian,'%') end flag
	
--case when error_percent>=shxian then null
--    when error_percent<shxian and error_percent>=0  then prediction_score
--	when  error_percent>=xiaxian and error_percent<0 then middle_office_price
--	when error_percent<xiaxian then middle_office_price end update_price		
from 
(select
a.delivery_dt,
a.dc_code,
a.goods_code,
b.goods_name,
classify_large_code,
classify_large_name,
b.classify_middle_code,
b.classify_middle_name,
classify_small_code,
classify_small_name,
case when b.classify_middle_code='B0101' then	 -3
     when b.classify_middle_code='B0102' then	 -2
     when b.classify_middle_code='B0103' then	 -2
     when b.classify_middle_code='B0201' then	 -2
     when b.classify_middle_code='B0202' then	 -3
     when b.classify_middle_code='B0301' then	 -2
     when b.classify_middle_code='B0302' then	 -1
     when b.classify_middle_code='B0304' then	 -3
     when b.classify_middle_code='B0501' then	 -3
     when b.classify_middle_code='B0602' then	 -4
end as xiaxian,
case when b.classify_middle_code='B0101' then 2
     when b.classify_middle_code='B0102' then 4
     when b.classify_middle_code='B0103' then 2
     when b.classify_middle_code='B0201' then 4
     when b.classify_middle_code='B0202' then 4
     when b.classify_middle_code='B0301' then 3
     when b.classify_middle_code='B0302' then 2
     when b.classify_middle_code='B0304' then 3
     when b.classify_middle_code='B0501' then 4
     when b.classify_middle_code='B0602' then 3
end as shxian,
prediction_score,
middle_office_price,
error_percent,
cost_price
,stock_price_last_0_day
,stock_vs_previous_rate
from
(select * from csx_ods.pred_table where sdt=${hiveconf:i_sdate_11} )a
--商品维表
left outer join 
  (
    select *
    from csx_dw.dws_basic_w_a_csx_product_m 
    where sdt = 'current'
  )b on b.goods_id = a.goods_code
where a.sdt=${hiveconf:i_sdate_11} )a
--采购报价、中台报价
left join 
(
  select  sdt,warehouse_code,goods_code,middle_office_price as csx_distribute_price
  from csx_dw.dws_price_r_d_goods_prices_m
  where sdt=${hiveconf:i_sdate_11}  and type='普通单'
)b on regexp_replace(a.delivery_dt,'-','')=b.sdt and a.dc_code=b.warehouse_code and a.goods_code=b.goods_code
--销售表取成本价,当天是否有销售
left join 
(
  select 
    substr(shipped_time,1,10) sdt,
    dc_code, 
    goods_code,
    sum(sales_cost)/sum(sales_qty) csx_cost_price
    --percentile_approx(cost_price,0.5)  price
    -- percentile_approx(middle_office_price,0.5) distribute_price
  from csx_dw.dws_sale_r_d_detail 
  where sdt >= ${hiveconf:i_sdate_11}
  and substr(shipped_time,1,10)= ${hiveconf:i_sdate_12}
  and channel_code in ('1', '7', '9')
  and business_type_code ='1'
  and sales_type<>'fanli'
  and return_flag<>'X'
  and province_name='重庆市'
  group by substr(shipped_time,1,10),
  dc_code,
  goods_code
)c  on a.delivery_dt=c.sdt and a.dc_code=c.dc_code and a.goods_code=c.goods_code
--销售表TOP商品 商品排名 --每天分别排名后取商品各天里最小排名
--left join 
--(select business_type_code,dc_code,goods_code,
--  min(rank_num) as rank_num
--  from 
--  (
--  select 
--    substr(shipped_time,1,10) sdt,business_type_code,dc_code,goods_code,
--    sum(sales_qty) sales_qty,
--    sum(sales_value) sales_value,
--    sum(sales_cost) sales_cost,
--    sum(profit) profit,
--    row_number() over (partition by substr(shipped_time,1,10), dc_code order by sum(sales_value) desc) as rank_num
--  from csx_dw.dws_sale_r_d_detail 
--    where sdt >= ${hiveconf:i_sdate_21} --and sdt<=${hiveconf:i_sdate_11}
--  	and substr(shipped_time,1,10)>= ${hiveconf:i_sdate_22} and  substr(shipped_time,1,10)<${hiveconf:i_sdate_12}
--  	and channel_code in ('1', '7', '9')	
--  	and business_type_code='1'
--  	and sales_type<>'fanli'
--  	and return_flag<>'X'
--    and province_name='重庆市'
--  group by substr(shipped_time,1,10),business_type_code,dc_code,goods_code
--  )d1
--group by business_type_code,dc_code,goods_code
--)j  on a.dc_code=j.dc_code and a.goods_code=j.goods_code
--销售表TOP商品 商品排名 
left join  
(
  select 
    business_type_code,dc_code,goods_code,
    sum(sales_qty) sales_qty,
    sum(sales_value) sales_value,
    sum(sales_cost) sales_cost,
    sum(profit) profit,
    row_number() over (partition by dc_code order by sum(sales_value) desc) as rank_num
  from csx_dw.dws_sale_r_d_detail 
  where sdt >= ${hiveconf:i_sdate_21} --and sdt<=${hiveconf:i_sdate_11}
  and substr(shipped_time,1,10)>= ${hiveconf:i_sdate_22} and  substr(shipped_time,1,10)<${hiveconf:i_sdate_12}
  and channel_code in ('1', '7', '9')	
  and business_type_code='1'
  and sales_type<>'fanli'
  and return_flag<>'X'
  and province_name='重庆市'
  group by business_type_code,dc_code,goods_code
)j  on a.dc_code=j.dc_code and a.goods_code=j.goods_code 
--是否满足中台报价8个点内
left join
(select distinct
  business_type_code,dc_code,goods_code
from csx_dw.dws_sale_r_d_detail 
    where sdt >= ${hiveconf:i_sdate_11}
    and substr(shipped_time,1,10)= ${hiveconf:i_sdate_12}
    --where sdt >= ${hiveconf:i_sdate_21} --and sdt<=${hiveconf:i_sdate_11}
	--and substr(shipped_time,1,10)>= ${hiveconf:i_sdate_22} and  substr(shipped_time,1,10)<${hiveconf:i_sdate_12}
	and channel_code in ('1', '7', '9')	
	and business_type_code='1'
	and sales_type<>'fanli'
	and return_flag<>'X'
    and province_name='重庆市'
  	and cast(((sales_price - middle_office_price)/sales_price) as decimal(30,6))>0
  	and cast(((sales_price - middle_office_price)/sales_price) as decimal(30,6))<=0.08
)k on k.goods_code=a.goods_code and a.dc_code=k.dc_code 
left join
(select distinct business_type_code,dc_code,goods_code
  from 
  (
  select 
    substr(shipped_time,1,10) sdt,business_type_code,dc_code,goods_code,
    sum(sales_qty) sales_qty,
    sum(sales_value) sales_value,
    sum(sales_cost) sales_cost,
    sum(profit) profit,
    row_number() over (partition by substr(shipped_time,1,10), dc_code order by sum(sales_value) desc) as rank_num
  from csx_dw.dws_sale_r_d_detail 
       where sdt >= ${hiveconf:i_sdate_21} --and sdt<=${hiveconf:i_sdate_11}
  	 and substr(shipped_time,1,10)>= ${hiveconf:i_sdate_22} and  substr(shipped_time,1,10)<${hiveconf:i_sdate_12}
  	and channel_code in ('1', '7', '9')	
  	and business_type_code='1'
  	and sales_type<>'fanli'
  	and return_flag<>'X'
      and province_name='重庆市'
  group by substr(shipped_time,1,10),business_type_code,dc_code,goods_code
  )d1
  where rank_num<=800
)d on d.goods_code=a.goods_code and a.dc_code=d.dc_code 
--筛选2：好的中台报价，订单商品明细取前端毛利率（0，0.08]
left join
(select distinct business_type_code,dc_code,goods_code
  from
  ( 
  select 
    substr(shipped_time,1,10) sdt,business_type_code,dc_code,goods_code,
    --if(0<(sales_price - middle_office_price)/sales_price<=0.08,if(cost_price<middle_office_price,"前端毛利好,成本不高于中台价","成本高于中台价"),"前端毛利不好") flag_price, --标签
    sum(sales_qty) sales_qty,
    sum(sales_value) sales_value,
    sum(sales_cost) sales_cost,
    sum(profit) profit
  from csx_dw.dws_sale_r_d_detail 
       where sdt >= ${hiveconf:i_sdate_21} --and sdt<=${hiveconf:i_sdate_11}
  	 and substr(shipped_time,1,10)>= ${hiveconf:i_sdate_22} and  substr(shipped_time,1,10)<${hiveconf:i_sdate_12}
  	and channel_code in ('1', '7', '9')	
  	and business_type_code='1'
  	and sales_type<>'fanli'
  	and return_flag<>'X'
      and province_name='重庆市'
  	and cast(((sales_price - middle_office_price)/sales_price) as decimal(30,6))>0
  	and cast(((sales_price - middle_office_price)/sales_price) as decimal(30,6))<=0.08
  group by substr(shipped_time,1,10),business_type_code,dc_code,goods_code
  )e1
)e on e.goods_code=a.goods_code and a.dc_code=e.dc_code 
--筛选3：中台报价高于成本价
left join
(select distinct business_type_code,dc_code,goods_code
  from
  (
  select 
    substr(shipped_time,1,10) sdt,business_type_code,
    dc_code,goods_code,
    sum(sales_qty) sales_qty,
    sum(sales_value) sales_value,
    sum(sales_cost) sales_cost,
    sum(profit) profit
  from csx_dw.dws_sale_r_d_detail 
       where sdt >= ${hiveconf:i_sdate_21} --and sdt<=${hiveconf:i_sdate_11}
  	 and substr(shipped_time,1,10)>= ${hiveconf:i_sdate_22} and  substr(shipped_time,1,10)<${hiveconf:i_sdate_12}
  	and channel_code in ('1', '7', '9')	
  	and business_type_code='1'
  	and sales_type<>'fanli'
  	and return_flag<>'X'
      and province_name='重庆市'
  	and cost_price<middle_office_price
  group by substr(shipped_time,1,10),business_type_code,dc_code,goods_code
  )f1  
)f on f.goods_code=a.goods_code and a.dc_code=f.dc_code 
--筛选2&筛选3
left join
(select distinct business_type_code,dc_code,goods_code
  from
  (
  select 
    substr(shipped_time,1,10) sdt,business_type_code,dc_code,goods_code,
    --if(0<(sales_price - middle_office_price)/sales_price<=0.08,if(cost_price<middle_office_price,"前端毛利好,成本不高于中台价","成本高于中台价"),"前端毛利不好") flag_price, --标签
    sum(sales_qty) sales_qty,
    sum(sales_value) sales_value,
    sum(sales_cost) sales_cost,
    sum(profit) profit
  from csx_dw.dws_sale_r_d_detail 
       where sdt >= ${hiveconf:i_sdate_21} --and sdt<=${hiveconf:i_sdate_11}
  	 and substr(shipped_time,1,10)>= ${hiveconf:i_sdate_22} and  substr(shipped_time,1,10)<${hiveconf:i_sdate_12}
  	and channel_code in ('1', '7', '9')	
  	and business_type_code='1'
  	and sales_type<>'fanli'
  	and return_flag<>'X'
      and province_name='重庆市'
  	and cast(((sales_price - middle_office_price)/sales_price) as decimal(30,6))>0
  	and cast(((sales_price - middle_office_price)/sales_price) as decimal(30,6))<=0.08
  	and cost_price<middle_office_price
  group by substr(shipped_time,1,10),business_type_code,dc_code,goods_code
  )g1
)g on g.goods_code=a.goods_code and a.dc_code=g.dc_code  
)a;  



insert overwrite directory '/tmp/raoyanhua/02yucebodong' row format delimited fields terminated by '\t'
select
classify_middle_code,classify_middle_name,
count(distinct goods_code) allgod,
count(distinct case when  flag_group_1=0 then goods_code end) less_below,
count(distinct case when  flag_group_1 in (1,2) or flag_group_1 is null then goods_code end) below_zero_up,
count(distinct case when  flag_group_1=3 then goods_code end) up_more,

count(distinct case when  rank_num_AI<=800 then goods_code end) allgod_TOP800,
count(distinct case when  rank_num_AI<=800 and flag_group_1=0 then goods_code end) less_below_TOP800,
count(distinct case when  rank_num_AI<=800 and (flag_group_1 in (1,2) or flag_group_1 is null) then goods_code end) below_zero_up_TOP800,
count(distinct case when  rank_num_AI<=800 and flag_group_1=3 then goods_code end) up_more_TOP800,

count(distinct case when  rank_num_AI<=1000 then goods_code end) allgod_TOP1000,
count(distinct case when  rank_num_AI<=1000 and flag_group_1=0 then goods_code end) less_below_TOP1000,
count(distinct case when  rank_num_AI<=1000 and (flag_group_1 in (1,2) or flag_group_1 is null) then goods_code end) below_zero_up_TOP1000,
count(distinct case when  rank_num_AI<=1000 and flag_group_1=3 then goods_code end) up_more_TOP1000,

count(distinct case when  rank_num_AI<=1200 then goods_code end) allgod_TOP1200,
count(distinct case when  rank_num_AI<=1200 and flag_group_1=0 then goods_code end) less_below_TOP1200,
count(distinct case when  rank_num_AI<=1200 and (flag_group_1 in (1,2) or flag_group_1 is null) then goods_code end) below_zero_up_TOP1200,
count(distinct case when  rank_num_AI<=1200 and flag_group_1=3 then goods_code end) up_more_TOP1200,

count(distinct case when  rank_num_AI<=1500 then goods_code end) allgod_TOP1500,
count(distinct case when  rank_num_AI<=1500 and flag_group_1=0 then goods_code end) less_below_TOP1500,
count(distinct case when  rank_num_AI<=1500 and (flag_group_1 in (1,2) or flag_group_1 is null) then goods_code end) below_zero_up_TOP1500,
count(distinct case when  rank_num_AI<=1500 and flag_group_1=3 then goods_code end) up_more_TOP1500,

count(distinct case when  is_sale='是' then goods_code end) allgod_is_sale,
count(distinct case when  is_sale='是' and flag_group_1=0 then goods_code end) less_below_is_sale,
count(distinct case when  is_sale='是' and (flag_group_1 in (1,2) or flag_group_1 is null) then goods_code end) below_zero_up_is_sale,
count(distinct case when  is_sale='是' and flag_group_1=3 then goods_code end) up_more_is_sale,

count(distinct case when  is_sale='是' and is_good_midprice_current='是' then goods_code end) allgod_sale_good_mid,
count(distinct case when  is_sale='是' and is_good_midprice_current='是' and flag_group_1=0 then goods_code end) less_below_sale_good_mid,
count(distinct case when  is_sale='是' and is_good_midprice_current='是' and (flag_group_1 in (1,2) or flag_group_1 is null) then goods_code end) below_zero_up_sale_good_mid,
count(distinct case when  is_sale='是' and is_good_midprice_current='是' and flag_group_1=3 then goods_code end) up_more_sale_good_mid,

count(distinct case when  is_sale='是' and is_good_midprice_current='否' then goods_code end) allgod_sale_bad_mid,
count(distinct case when  is_sale='是' and is_good_midprice_current='否' and flag_group_1=0 then goods_code end) less_below_sale_bad_mid,
count(distinct case when  is_sale='是' and is_good_midprice_current='否' and (flag_group_1 in (1,2) or flag_group_1 is null) then goods_code end) below_zero_up_sale_bad_mid,
count(distinct case when  is_sale='是' and is_good_midprice_current='否' and flag_group_1=3 then goods_code end) up_more_sale_bad_mid,

count(distinct case when  is_3select='是' then goods_code end) allgod_is_3select,
count(distinct case when  is_3select='是' and flag_group_1=0 then goods_code end) less_below_is_3select,
count(distinct case when  is_3select='是' and (flag_group_1 in (1,2) or flag_group_1 is null) then goods_code end) below_zero_up_is_3select,
count(distinct case when  is_3select='是' and flag_group_1=3 then goods_code end) up_more_is_3select,

count(distinct case when  middle_office_price is not null then goods_code end) allgod_AI,
count(distinct case when  middle_office_price is not null  and flag_group_1=0 then goods_code end) less_below_AI,
count(distinct case when  middle_office_price is not null  and (flag_group_1 in (1,2) or flag_group_1 is null) then goods_code end) below_zero_up_AI,
count(distinct case when  middle_office_price is not null  and flag_group_1=3 then goods_code end) up_more_is_AI
from csx_tmp.tmp_AI_01
group by classify_middle_code,classify_middle_name;
