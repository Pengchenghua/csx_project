--覆盖率&前端毛利率


set date_current =regexp_replace(date_sub(current_date,3),'-','');  --昨日
set date_current_1 =date_sub(current_date,3);  --昨日
set date_14before =regexp_replace(date_sub(current_date,3+13),'-','');
set date_14before_1 =date_sub(current_date,3+13);  
set date_7before =regexp_replace(date_sub(current_date,3+7),'-','');
set date_7before_1 =date_sub(current_date,3+7); 
set date_21before =regexp_replace(date_sub(current_date,3+20),'-','');
set date_21before_1 =date_sub(current_date,3+20); 

	
--01、AI覆盖情况
insert overwrite directory '/tmp/raoyanhua/01fugai' row format delimited fields terminated by '\t'
select 
  a.province_name,c.classify_middle_code,
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
  sum(a.front_profit) front_profit,
  cast(sum(a.front_profit)/sum(a.sales_value) as decimal(30,6)) fnt_prorate,
  
  sum(if(a.business_type_code='1',a.sales_value,0)) sales_value_ripei,
  sum(if(a.business_type_code='1',a.profit,0)) profit_ripei, 
  cast(sum(if(a.business_type_code='1',a.profit,0))/sum(if(a.business_type_code='1',a.sales_value,0)) as decimal(30,6)) prorate_ripei,
  sum(if(a.business_type_code='1',a.front_profit,0)) front_profit_ripei,  
  cast(sum(if(a.business_type_code='1',a.front_profit,0))/sum(if(a.business_type_code='1',a.sales_value,0)) as decimal(30,6)) fnt_prorate_ripei,
  
  sum(if(a.business_type_code='1' and b.goods_code is not null,a.sales_value,0)) sales_value_ripei_AI,
  sum(if(a.business_type_code='1' and b.goods_code is not null,a.profit,0)) profit_ripei_AI,
  cast(sum(if(a.business_type_code='1' and b.goods_code is not null,a.profit,0))/
  sum(if(a.business_type_code='1' and b.goods_code is not null,a.sales_value,0)) as decimal(30,6)) prorate_ripei_AI,
  sum(if(a.business_type_code='1' and b.goods_code is not null,a.front_profit,0)) front_profit_ripei_AI,  
  cast(sum(if(a.business_type_code='1' and b.goods_code is not null,a.front_profit,0))/
  sum(if(a.business_type_code='1' and b.goods_code is not null,a.sales_value,0)) as decimal(30,6)) fnt_prorate_ripei_AI,  
  
  sum(if(a.business_type_code='1' and d.goods_code is not null,a.sales_value,0)) sales_value_ripei_top800,
  sum(if(a.business_type_code='1' and d.goods_code is not null,a.profit,0)) profit_ripei_top800,
  cast(sum(if(a.business_type_code='1' and d.goods_code is not null,a.profit,0))/
  sum(if(a.business_type_code='1' and d.goods_code is not null,a.sales_value,0)) as decimal(30,6)) prorate_ripei_top800, 
  sum(if(a.business_type_code='1' and d.goods_code is not null,a.front_profit,0)) front_profit_ripei_top800,  
  cast(sum(if(a.business_type_code='1' and d.goods_code is not null,a.front_profit,0))/
  sum(if(a.business_type_code='1' and d.goods_code is not null,a.sales_value,0)) as decimal(30,6)) fnt_prorate_ripei_top800, 
  
  sum(if(a.business_type_code='1' and e.goods_code is not null,a.sales_value,0)) sales_value_ripei_mid_good,
  sum(if(a.business_type_code='1' and e.goods_code is not null,a.profit,0)) profit_ripei_mid_good,
  cast(sum(if(a.business_type_code='1' and e.goods_code is not null,a.profit,0))/
  sum(if(a.business_type_code='1' and e.goods_code is not null,a.sales_value,0)) as decimal(30,6)) prorate_ripei_mid_good, 
  sum(if(a.business_type_code='1' and e.goods_code is not null,a.front_profit,0)) front_profit_ripei_mid_good,  
  cast(sum(if(a.business_type_code='1' and e.goods_code is not null,a.front_profit,0))/
  sum(if(a.business_type_code='1' and e.goods_code is not null,a.sales_value,0)) as decimal(30,6)) fnt_prorate_ripei_mid_good,
  
  sum(if(a.business_type_code='1' and f.goods_code is not null,a.sales_value,0)) sales_value_ripei_cost_bad,
  sum(if(a.business_type_code='1' and f.goods_code is not null,a.profit,0)) profit_ripei_cost_bad,
  cast(sum(if(a.business_type_code='1' and f.goods_code is not null,a.profit,0))/
  sum(if(a.business_type_code='1' and f.goods_code is not null,a.sales_value,0)) as decimal(30,6)) prorate_ripei_cost_bad, 
  sum(if(a.business_type_code='1' and f.goods_code is not null,a.front_profit,0)) front_profit_ripei_cost_bad,  
  cast(sum(if(a.business_type_code='1' and f.goods_code is not null,a.front_profit,0))/
  sum(if(a.business_type_code='1' and f.goods_code is not null,a.sales_value,0)) as decimal(30,6)) fnt_prorate_ripei_cost_bad,
  
  sum(if(a.business_type_code='1' and d.goods_code is not null and g.goods_code is not null,a.sales_value,0)) sales_value_ripei_S,
  sum(if(a.business_type_code='1' and d.goods_code is not null and g.goods_code is not null,a.profit,0)) profit_ripei_S,
  cast(sum(if(a.business_type_code='1' and d.goods_code is not null and g.goods_code is not null,a.profit,0))/
  sum(if(a.business_type_code='1' and d.goods_code is not null and g.goods_code is not null,a.sales_value,0)) as decimal(30,6)) prorate_ripei_S,
  sum(if(a.business_type_code='1' and d.goods_code is not null and g.goods_code is not null,a.front_profit,0)) front_profit_ripei_S,  
  cast(sum(if(a.business_type_code='1' and d.goods_code is not null and g.goods_code is not null,a.front_profit,0))/
  sum(if(a.business_type_code='1' and d.goods_code is not null and g.goods_code is not null,a.sales_value,0)) as decimal(30,6)) fnt_prorate_ripei_S  
from
(select 
  province_name,substr(shipped_time,1,10) sdt,business_type_code,
  classify_middle_code,classify_middle_name, 
  dc_code,goods_code,goods_name,
  sum(sales_qty) sales_qty,
  sum(sales_value) sales_value,
  sum(sales_cost) sales_cost,
  sum(profit) profit,
  sum(front_profit) front_profit
from csx_dw.dws_sale_r_d_detail 
     where sdt >= ${hiveconf:date_21before} --and sdt<=${hiveconf:date_current}
	 and substr(shipped_time,1,10)>= ${hiveconf:date_21before_1} and  substr(shipped_time,1,10)<${hiveconf:date_current_1}
	and channel_code in ('1', '7', '9')	
	--and business_type_code='1'
	and sales_type<>'fanli'
	and return_flag<>'X'
    and province_name in('四川省','福建省','安徽省','北京市','重庆市')
	and dc_code in('W0A6','W0A8','W0A2','W0A3','W0A7')
group by province_name,substr(shipped_time,1,10),business_type_code,  
  classify_middle_code,classify_middle_name,
  dc_code,goods_code,goods_name
)a
left join
 (select distinct goods_code,dc_code
  --,delivery_dt,middle_office_price
  from  csx_ods.pred_table_extend 
  where sdt>=${hiveconf:date_7before} and sdt<=${hiveconf:date_current}
  and dc_code in('W0A6','W0A8','W0A2','W0A3','W0A7')
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
  select province_name,
    substr(shipped_time,1,10) sdt,business_type_code,dc_code,goods_code,
    sum(sales_qty) sales_qty,
    sum(sales_value) sales_value,
    sum(sales_cost) sales_cost,
    sum(profit) profit,
    row_number() over (partition by substr(shipped_time,1,10), dc_code order by sum(sales_value) desc) as rank_num
  from csx_dw.dws_sale_r_d_detail 
       where sdt >= ${hiveconf:date_21before} --and sdt<=${hiveconf:date_current}
  	 and substr(shipped_time,1,10)>= ${hiveconf:date_21before_1} and  substr(shipped_time,1,10)<${hiveconf:date_current_1}
  	and channel_code in ('1', '7', '9')	
  	and business_type_code='1'
  	and sales_type<>'fanli'
  	and return_flag<>'X'
    and province_name in('四川省','福建省','安徽省','北京市','重庆市')
	and dc_code in('W0A6','W0A8','W0A2','W0A3','W0A7')
  group by province_name,substr(shipped_time,1,10),business_type_code,dc_code,goods_code
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
       where sdt >= ${hiveconf:date_21before} --and sdt<=${hiveconf:date_current}
  	 and substr(shipped_time,1,10)>= ${hiveconf:date_21before_1} and  substr(shipped_time,1,10)<${hiveconf:date_current_1}
  	and channel_code in ('1', '7', '9')	
  	and business_type_code='1'
  	and sales_type<>'fanli'
  	and return_flag<>'X'
    and province_name in('四川省','福建省','安徽省','北京市','重庆市')
	and dc_code in('W0A6','W0A8','W0A2','W0A3','W0A7')
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
       where sdt >= ${hiveconf:date_21before} --and sdt<=${hiveconf:date_current}
  	 and substr(shipped_time,1,10)>= ${hiveconf:date_21before_1} and  substr(shipped_time,1,10)<${hiveconf:date_current_1}
  	and channel_code in ('1', '7', '9')	
  	and business_type_code='1'
  	and sales_type<>'fanli'
  	and return_flag<>'X'
    and province_name in('四川省','福建省','安徽省','北京市','重庆市')
	and dc_code in('W0A6','W0A8','W0A2','W0A3','W0A7')
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
       where sdt >= ${hiveconf:date_21before} --and sdt<=${hiveconf:date_current}
  	 and substr(shipped_time,1,10)>= ${hiveconf:date_21before_1} and  substr(shipped_time,1,10)<${hiveconf:date_current_1}
  	and channel_code in ('1', '7', '9')	
  	and business_type_code='1'
  	and sales_type<>'fanli'
  	and return_flag<>'X'
    and province_name in('四川省','福建省','安徽省','北京市','重庆市')
	and dc_code in('W0A6','W0A8','W0A2','W0A3','W0A7')
  	and cast(((sales_price - middle_office_price)/sales_price) as decimal(30,6))>0
  	and cast(((sales_price - middle_office_price)/sales_price) as decimal(30,6))<=0.08
  	and cost_price<middle_office_price
  group by substr(shipped_time,1,10),business_type_code,dc_code,goods_code
  )g1
)g on g.goods_code=a.goods_code and a.dc_code=g.dc_code and a.business_type_code=g.business_type_code 
group by a.province_name,c.classify_middle_code,c.classify_middle_name;


	


