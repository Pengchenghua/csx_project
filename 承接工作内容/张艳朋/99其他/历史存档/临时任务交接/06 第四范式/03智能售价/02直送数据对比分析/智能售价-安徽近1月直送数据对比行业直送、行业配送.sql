--近一个月安徽直送订单级、商品级对比配送分析、历史近几日的  如面粉

-- 昨日、上月昨日
--select ${hiveconf:current_day},${hiveconf:before1_last_mon},${hiveconf:before1_last_mon_before7};
set current_day =regexp_replace(date_sub(current_date,1),'-','');
set before1_last_mon =regexp_replace(add_months(date_sub(current_date,1),-1),'-','');
set before1_last_mon_before7 =regexp_replace(date_sub(add_months(date_sub(current_date,1),-1),7),'-','');


--part1：商品维度直送对比历史、对比配送历史
--临时表
drop table csx_tmp.tmp_goods_price_1;
create temporary table csx_tmp.tmp_goods_price_1
as
select from_unixtime(unix_timestamp(a.sdt,'yyyymmdd'),'yyyy-mm-dd') as sdt_1,
  a.sdt,a.region_name,a.province_name,a.customer_no,b.customer_name,b.second_category_code,b.second_category_name,a.goods_code,c.goods_name,c.classify_middle_code,c.classify_middle_name,
  a.business_type_code,a.business_type_name,a.logistics_mode_code,a.logistics_mode_name,
  a.cost_price,a.middle_office_price,a.sales_price,a.sales_qty,a.sales_value,a.profit
  --cast(sum(a.sales_price*a.sales_qty)/sum(a.sales_qty) as decimal(30,6)) sales_price,   --含税售价 客户商品加权平均价
from 
(
  select *
  from csx_dw.dws_sale_r_d_detail
  --where sdt>${hiveconf:before1_last_mon}
  where sdt>${hiveconf:before1_last_mon_before7}
  and sdt<=${hiveconf:current_day}
  and channel_code in('1','7','9')
  and business_type_code='1'
  and dc_code='W0A2'
  and (return_flag<>'X' or return_flag is null)   --剔除退货单
  and substr(order_no,1,1)<>'R' --差异单补救单也会是R开头，剔除
  and sales_type<>'fanli'
  and customer_no not in('116932','117001','117464','117017','120830','120595','108102')  --战略客户
)a 
left join 
(
  select customer_no,customer_name,second_category_code,second_category_name,third_category_code,third_category_name
  from csx_dw.dws_crm_w_a_customer
  where sdt='current'
)b on a.customer_no=b.customer_no
left join 
(
  select goods_id,goods_name,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name
  from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current'
)c on a.goods_code=c.goods_id;

--结果：商品维度
insert overwrite directory '/tmp/raoyanhua/shangpin' row format delimited fields terminated by '\t'
select a.customer_no,a.customer_name,a.second_category_code,a.second_category_name,a.goods_code,a.goods_name,a.classify_middle_code,a.classify_middle_name,a.sdt,
  a.sales_qty,a.cost_price,a.middle_office_price,a.sales_price,a.sales_value,a.profit,
  round(sum(a.profit)/abs(sum(a.sales_value)),6) profit_rate, 
  round(sum(if(b.sdt>=regexp_replace(date_sub(a.sdt_1,3),'-','')  and b.sdt<a.sdt,b.cost_price*b.sales_qty,0))
        /sum(if(b.sdt>=regexp_replace(date_sub(a.sdt_1,3),'-','')  and b.sdt<a.sdt,b.sales_qty,0)),6) cost_price3,    
  round(sum(if(b.sdt>=regexp_replace(date_sub(a.sdt_1,3),'-','')  and b.sdt<a.sdt,b.middle_office_price*b.sales_qty,0))
        /sum(if(b.sdt>=regexp_replace(date_sub(a.sdt_1,3),'-','')  and b.sdt<a.sdt,b.sales_qty,0)),6) middle_office_price3,
  round(sum(if(b.sdt>=regexp_replace(date_sub(a.sdt_1,3),'-','')  and b.sdt<a.sdt,b.sales_price*b.sales_qty,0))
        /sum(if(b.sdt>=regexp_replace(date_sub(a.sdt_1,3),'-','')  and b.sdt<a.sdt,b.sales_qty,0)),6) sales_price3,
  round(sum(if(b.sdt>=regexp_replace(date_sub(a.sdt_1,3),'-','')  and b.sdt<a.sdt,b.profit,0))
        /abs(sum(if(b.sdt>=regexp_replace(date_sub(a.sdt_1,3),'-','')  and b.sdt<a.sdt,b.sales_value,0))),6) profit_rate3,
		
  round(sum(if(b.sdt>=regexp_replace(date_sub(a.sdt_1,7),'-','')  and b.sdt<a.sdt,b.cost_price*b.sales_qty,0))
        /sum(if(b.sdt>=regexp_replace(date_sub(a.sdt_1,7),'-','')  and b.sdt<a.sdt,b.sales_qty,0)),6) cost_price7,    
  round(sum(if(b.sdt>=regexp_replace(date_sub(a.sdt_1,7),'-','')  and b.sdt<a.sdt,b.middle_office_price*b.sales_qty,0))
        /sum(if(b.sdt>=regexp_replace(date_sub(a.sdt_1,7),'-','')  and b.sdt<a.sdt,b.sales_qty,0)),6) middle_office_price7,
  round(sum(if(b.sdt>=regexp_replace(date_sub(a.sdt_1,7),'-','')  and b.sdt<a.sdt,b.sales_price*b.sales_qty,0))
        /sum(if(b.sdt>=regexp_replace(date_sub(a.sdt_1,7),'-','')  and b.sdt<a.sdt,b.sales_qty,0)),6) sales_price7,	
  round(sum(if(b.sdt>=regexp_replace(date_sub(a.sdt_1,7),'-','')  and b.sdt<a.sdt,b.profit,0))
        /abs(sum(if(b.sdt>=regexp_replace(date_sub(a.sdt_1,7),'-','')  and b.sdt<a.sdt,b.sales_value,0))),6) profit_rate7,
		
  round(sum(if(c.sdt>=regexp_replace(date_sub(a.sdt_1,3),'-','')  and c.sdt<a.sdt,c.cost_price*c.sales_qty,0))
        /sum(if(c.sdt>=regexp_replace(date_sub(a.sdt_1,3),'-','')  and c.sdt<a.sdt,c.sales_qty,0)),6) cost_price3_pei,    
  round(sum(if(c.sdt>=regexp_replace(date_sub(a.sdt_1,3),'-','')  and c.sdt<a.sdt,c.middle_office_price*c.sales_qty,0))
        /sum(if(c.sdt>=regexp_replace(date_sub(a.sdt_1,3),'-','')  and c.sdt<a.sdt,c.sales_qty,0)),6) middle_office_price3_pei,
  round(sum(if(c.sdt>=regexp_replace(date_sub(a.sdt_1,3),'-','')  and c.sdt<a.sdt,c.sales_price*c.sales_qty,0))
        /sum(if(c.sdt>=regexp_replace(date_sub(a.sdt_1,3),'-','')  and c.sdt<a.sdt,c.sales_qty,0)),6) sales_price3_pei,
  round(sum(if(c.sdt>=regexp_replace(date_sub(a.sdt_1,3),'-','')  and c.sdt<a.sdt,c.profit,0))
        /abs(sum(if(c.sdt>=regexp_replace(date_sub(a.sdt_1,3),'-','')  and c.sdt<a.sdt,c.sales_value,0))),6) profit_rate3_pei,
		
  round(sum(if(c.sdt>=regexp_replace(date_sub(a.sdt_1,7),'-','')  and c.sdt<a.sdt,c.cost_price*c.sales_qty,0))
        /sum(if(c.sdt>=regexp_replace(date_sub(a.sdt_1,7),'-','')  and c.sdt<a.sdt,c.sales_qty,0)),6) cost_price7_pei,    
  round(sum(if(c.sdt>=regexp_replace(date_sub(a.sdt_1,7),'-','')  and c.sdt<a.sdt,c.middle_office_price*c.sales_qty,0))
        /sum(if(c.sdt>=regexp_replace(date_sub(a.sdt_1,7),'-','')  and c.sdt<a.sdt,c.sales_qty,0)),6) middle_office_price7_pei,
  round(sum(if(c.sdt>=regexp_replace(date_sub(a.sdt_1,7),'-','')  and c.sdt<a.sdt,c.sales_price*c.sales_qty,0))
        /sum(if(c.sdt>=regexp_replace(date_sub(a.sdt_1,7),'-','')  and c.sdt<a.sdt,c.sales_qty,0)),6) sales_price7_pei,
  round(sum(if(c.sdt>=regexp_replace(date_sub(a.sdt_1,7),'-','')  and c.sdt<a.sdt,c.profit,0))
        /abs(sum(if(c.sdt>=regexp_replace(date_sub(a.sdt_1,7),'-','')  and c.sdt<a.sdt,c.sales_value,0))),6) profit_rate7_pei		
from 
--直送 客户商品每日价格
(
  select customer_no,customer_name,second_category_code,second_category_name,goods_code,goods_name,classify_middle_code,classify_middle_name,sdt,sdt_1,
    sum(sales_qty) sales_qty,
    round(sum(cost_price*sales_qty)/sum(sales_qty),6) cost_price,  
    round(sum(middle_office_price*sales_qty)/sum(sales_qty),6) middle_office_price,  
    round(sum(sales_price*sales_qty)/sum(sales_qty),6) sales_price,
    sum(sales_value) sales_value,
	sum(profit) profit
  from csx_tmp.tmp_goods_price_1
  where logistics_mode_code='1'
  and sdt>${hiveconf:before1_last_mon}
  group by customer_no,customer_name,second_category_code,second_category_name,goods_code,goods_name,classify_middle_code,classify_middle_name,sdt,sdt_1
)a
left join
--同行业直送
(
  select logistics_mode_code,logistics_mode_name,second_category_code,second_category_name,goods_code,goods_name,classify_middle_code,classify_middle_name,sdt,
    sum(sales_qty) sales_qty,
    round(sum(cost_price*sales_qty)/sum(sales_qty),6) cost_price,  
    round(sum(middle_office_price*sales_qty)/sum(sales_qty),6) middle_office_price,  
    round(sum(sales_price*sales_qty)/sum(sales_qty),6) sales_price,
    sum(sales_value) sales_value,
	sum(profit) profit
  from csx_tmp.tmp_goods_price_1
  where logistics_mode_code='1'
  group by logistics_mode_code,logistics_mode_name,second_category_code,second_category_name,goods_code,goods_name,classify_middle_code,classify_middle_name,sdt
) b on b.second_category_code=a.second_category_code and b.goods_code=a.goods_code
left join
--同行业配送
(
  select logistics_mode_code,logistics_mode_name,second_category_code,second_category_name,goods_code,goods_name,classify_middle_code,classify_middle_name,sdt,
    sum(sales_qty) sales_qty,
    round(sum(cost_price*sales_qty)/sum(sales_qty),6) cost_price,  
    round(sum(middle_office_price*sales_qty)/sum(sales_qty),6) middle_office_price,  
    round(sum(sales_price*sales_qty)/sum(sales_qty),6) sales_price,
    sum(sales_value) sales_value,
	sum(profit) profit
  from csx_tmp.tmp_goods_price_1
  where logistics_mode_code='2'
  group by logistics_mode_code,logistics_mode_name,second_category_code,second_category_name,goods_code,goods_name,classify_middle_code,classify_middle_name,sdt
) c on c.second_category_code=a.second_category_code and c.goods_code=a.goods_code
group by a.customer_no,a.customer_name,a.second_category_code,a.second_category_name,a.goods_code,a.goods_name,a.classify_middle_code,a.classify_middle_name,a.sdt,
  a.sales_qty,a.cost_price,a.middle_office_price,a.sales_price,a.sales_value,a.profit;


--part2：订单维度直送对比历史、对比配送历史
--临时表
drop table csx_tmp.tmp_order_price_1;
create temporary table csx_tmp.tmp_order_price_1
as
select from_unixtime(unix_timestamp(a.sdt,'yyyymmdd'),'yyyy-mm-dd') as sdt_1,
  a.sdt,a.region_name,a.province_name,a.customer_no,b.customer_name,b.second_category_code,b.second_category_name,a.order_no,
  a.business_type_code,a.business_type_name,a.logistics_mode_code,a.logistics_mode_name,
  a.cost_price,a.middle_office_price,a.sales_price,a.sales_qty,a.sales_value,a.profit
  --cast(sum(a.sales_price*a.sales_qty)/sum(a.sales_qty) as decimal(30,6)) sales_price,   --含税售价 客户商品加权平均价
from 
(
  select *
  from csx_dw.dws_sale_r_d_detail
  --where sdt>${hiveconf:before1_last_mon}
  where sdt>${hiveconf:before1_last_mon_before7}
  and sdt<=${hiveconf:current_day}
  and channel_code in('1','7','9')
  and business_type_code='1'
  and dc_code='W0A2'
  and (return_flag<>'X' or return_flag is null)   --剔除退货单
  and substr(order_no,1,1)<>'R' --差异单补救单也会是R开头，剔除
  and sales_type<>'fanli'
  and customer_no not in('116932','117001','117464','117017','120830','120595','108102')  --战略客户
)a 
left join 
(
  select customer_no,customer_name,second_category_code,second_category_name,third_category_code,third_category_name
  from csx_dw.dws_crm_w_a_customer
  where sdt='current'
)b on a.customer_no=b.customer_no;

--结果：订单维度
insert overwrite directory '/tmp/raoyanhua/dingdan' row format delimited fields terminated by '\t'
select a.customer_no,a.customer_name,a.second_category_code,a.second_category_name,a.order_no,a.sdt,
  a.sales_qty,a.cost_price,a.middle_office_price,a.sales_price,a.sales_value,a.profit,
  round(sum(a.profit)/abs(sum(a.sales_value)),6) profit_rate, 
  
  round(sum(if(b.sdt>=regexp_replace(date_sub(a.sdt_1,3),'-','')  and b.sdt<a.sdt,b.cost_price*b.sales_qty,0))
        /sum(if(b.sdt>=regexp_replace(date_sub(a.sdt_1,3),'-','')  and b.sdt<a.sdt,b.sales_qty,0)),6) cost_price3,    
  round(sum(if(b.sdt>=regexp_replace(date_sub(a.sdt_1,3),'-','')  and b.sdt<a.sdt,b.middle_office_price*b.sales_qty,0))
        /sum(if(b.sdt>=regexp_replace(date_sub(a.sdt_1,3),'-','')  and b.sdt<a.sdt,b.sales_qty,0)),6) middle_office_price3,
  round(sum(if(b.sdt>=regexp_replace(date_sub(a.sdt_1,3),'-','')  and b.sdt<a.sdt,b.sales_price*b.sales_qty,0))
        /sum(if(b.sdt>=regexp_replace(date_sub(a.sdt_1,3),'-','')  and b.sdt<a.sdt,b.sales_qty,0)),6) sales_price3,
  round(sum(if(b.sdt>=regexp_replace(date_sub(a.sdt_1,3),'-','')  and b.sdt<a.sdt,b.profit,0))
        /abs(sum(if(b.sdt>=regexp_replace(date_sub(a.sdt_1,3),'-','')  and b.sdt<a.sdt,b.sales_value,0))),6) profit_rate3,
		
  round(sum(if(b.sdt>=regexp_replace(date_sub(a.sdt_1,7),'-','')  and b.sdt<a.sdt,b.cost_price*b.sales_qty,0))
        /sum(if(b.sdt>=regexp_replace(date_sub(a.sdt_1,7),'-','')  and b.sdt<a.sdt,b.sales_qty,0)),6) cost_price7,    
  round(sum(if(b.sdt>=regexp_replace(date_sub(a.sdt_1,7),'-','')  and b.sdt<a.sdt,b.middle_office_price*b.sales_qty,0))
        /sum(if(b.sdt>=regexp_replace(date_sub(a.sdt_1,7),'-','')  and b.sdt<a.sdt,b.sales_qty,0)),6) middle_office_price7,
  round(sum(if(b.sdt>=regexp_replace(date_sub(a.sdt_1,7),'-','')  and b.sdt<a.sdt,b.sales_price*b.sales_qty,0))
        /sum(if(b.sdt>=regexp_replace(date_sub(a.sdt_1,7),'-','')  and b.sdt<a.sdt,b.sales_qty,0)),6) sales_price7,	
  round(sum(if(b.sdt>=regexp_replace(date_sub(a.sdt_1,7),'-','')  and b.sdt<a.sdt,b.profit,0))
        /abs(sum(if(b.sdt>=regexp_replace(date_sub(a.sdt_1,7),'-','')  and b.sdt<a.sdt,b.sales_value,0))),6) profit_rate7,
		
  round(sum(if(c.sdt>=regexp_replace(date_sub(a.sdt_1,3),'-','')  and c.sdt<a.sdt,c.cost_price*c.sales_qty,0))
        /sum(if(c.sdt>=regexp_replace(date_sub(a.sdt_1,3),'-','')  and c.sdt<a.sdt,c.sales_qty,0)),6) cost_price3_pei,    
  round(sum(if(c.sdt>=regexp_replace(date_sub(a.sdt_1,3),'-','')  and c.sdt<a.sdt,c.middle_office_price*c.sales_qty,0))
        /sum(if(c.sdt>=regexp_replace(date_sub(a.sdt_1,3),'-','')  and c.sdt<a.sdt,c.sales_qty,0)),6) middle_office_price3_pei,
  round(sum(if(c.sdt>=regexp_replace(date_sub(a.sdt_1,3),'-','')  and c.sdt<a.sdt,c.sales_price*c.sales_qty,0))
        /sum(if(c.sdt>=regexp_replace(date_sub(a.sdt_1,3),'-','')  and c.sdt<a.sdt,c.sales_qty,0)),6) sales_price3_pei,
  round(sum(if(c.sdt>=regexp_replace(date_sub(a.sdt_1,3),'-','')  and c.sdt<a.sdt,c.profit,0))
        /abs(sum(if(c.sdt>=regexp_replace(date_sub(a.sdt_1,3),'-','')  and c.sdt<a.sdt,c.sales_value,0))),6) profit_rate3_pei,
		
  round(sum(if(c.sdt>=regexp_replace(date_sub(a.sdt_1,7),'-','')  and c.sdt<a.sdt,c.cost_price*c.sales_qty,0))
        /sum(if(c.sdt>=regexp_replace(date_sub(a.sdt_1,7),'-','')  and c.sdt<a.sdt,c.sales_qty,0)),6) cost_price7_pei,    
  round(sum(if(c.sdt>=regexp_replace(date_sub(a.sdt_1,7),'-','')  and c.sdt<a.sdt,c.middle_office_price*c.sales_qty,0))
        /sum(if(c.sdt>=regexp_replace(date_sub(a.sdt_1,7),'-','')  and c.sdt<a.sdt,c.sales_qty,0)),6) middle_office_price7_pei,
  round(sum(if(c.sdt>=regexp_replace(date_sub(a.sdt_1,7),'-','')  and c.sdt<a.sdt,c.sales_price*c.sales_qty,0))
        /sum(if(c.sdt>=regexp_replace(date_sub(a.sdt_1,7),'-','')  and c.sdt<a.sdt,c.sales_qty,0)),6) sales_price7_pei,
  round(sum(if(c.sdt>=regexp_replace(date_sub(a.sdt_1,7),'-','')  and c.sdt<a.sdt,c.profit,0))
        /abs(sum(if(c.sdt>=regexp_replace(date_sub(a.sdt_1,7),'-','')  and c.sdt<a.sdt,c.sales_value,0))),6) profit_rate7_pei		
from 
--直送 客户商品每日价格
(
  select customer_no,customer_name,second_category_code,second_category_name,order_no,sdt,sdt_1,
    sum(sales_qty) sales_qty,
    round(sum(cost_price*sales_qty)/sum(sales_qty),6) cost_price,  
    round(sum(middle_office_price*sales_qty)/sum(sales_qty),6) middle_office_price,  
    round(sum(sales_price*sales_qty)/sum(sales_qty),6) sales_price,
    sum(sales_value) sales_value,
	sum(profit) profit
  from csx_tmp.tmp_order_price_1
  where logistics_mode_code='1'
  and sdt>${hiveconf:before1_last_mon}
  group by customer_no,customer_name,second_category_code,second_category_name,order_no,sdt,sdt_1
)a
left join
--同行业直送
(
  select logistics_mode_code,logistics_mode_name,second_category_code,second_category_name,sdt,
    sum(sales_qty) sales_qty,
    round(sum(cost_price*sales_qty)/sum(sales_qty),6) cost_price,  
    round(sum(middle_office_price*sales_qty)/sum(sales_qty),6) middle_office_price,  
    round(sum(sales_price*sales_qty)/sum(sales_qty),6) sales_price,
    sum(sales_value) sales_value,
	sum(profit) profit
  from csx_tmp.tmp_order_price_1
  where logistics_mode_code='1'
  group by logistics_mode_code,logistics_mode_name,second_category_code,second_category_name,sdt
) b on b.second_category_code=a.second_category_code
left join
--同行业配送
(
  select logistics_mode_code,logistics_mode_name,second_category_code,second_category_name,sdt,
    sum(sales_qty) sales_qty,
    round(sum(cost_price*sales_qty)/sum(sales_qty),6) cost_price,  
    round(sum(middle_office_price*sales_qty)/sum(sales_qty),6) middle_office_price,  
    round(sum(sales_price*sales_qty)/sum(sales_qty),6) sales_price,
    sum(sales_value) sales_value,
	sum(profit) profit
  from csx_tmp.tmp_order_price_1
  where logistics_mode_code='2'
  group by logistics_mode_code,logistics_mode_name,second_category_code,second_category_name,sdt
) c on c.second_category_code=a.second_category_code
group by a.customer_no,a.customer_name,a.second_category_code,a.second_category_name,a.order_no,a.sdt,
  a.sales_qty,a.cost_price,a.middle_office_price,a.sales_price,a.sales_value,a.profit;


--part3：品类直送配送客户数、销售额占比、毛利率差异
select classify_middle_code,classify_middle_name,
  count_cust,sales_value,profit,profit/abs(sales_value) as profit_rate,
  count_cust_2,sales_value_2,profit_2,profit_2/abs(sales_value_2) as profit_rate_2,
  count_cust_zhi,sales_value_zhi,profit_zhi,profit_zhi/abs(sales_value_zhi) as profit_rate_zhi,
  count_cust_pei,sales_value_pei,profit_pei,profit_pei/abs(sales_value_pei) as profit_rate_pei,
  count_cust_zhi/count_cust_2 count_cust_zhi_rate,
  sales_value_zhi/sales_value_2 sales_value_zhi_rate,
  profit_zhi/abs(sales_value_zhi)-profit_2/abs(sales_value_2) profit_rate_zhi_diff
from
(
  select 
    --a.customer_no,b.customer_name,b.second_category_code,b.second_category_name,a.goods_code,c.goods_name,
    c.classify_middle_code,c.classify_middle_name,
    count(distinct a.customer_no) count_cust,
    sum(a.sales_value) sales_value,
    sum(a.profit) profit,
	
	count(distinct case when d.sales_value_zhi>0 and d.sales_value_pei>0 then a.customer_no end) count_cust_2,
    sum(case when d.sales_value_zhi>0 and d.sales_value_pei>0 then a.sales_value end) sales_value_2,
    sum(case when d.sales_value_zhi>0 and d.sales_value_pei>0 then a.profit end) profit_2,

	count(distinct case when d.sales_value_zhi>0 and d.sales_value_pei>0 and a.logistics_mode_code='1' then a.customer_no end) count_cust_zhi,
    sum(case when d.sales_value_zhi>0 and d.sales_value_pei>0 and a.logistics_mode_code='1' then a.sales_value end) sales_value_zhi,
    sum(case when d.sales_value_zhi>0 and d.sales_value_pei>0 and a.logistics_mode_code='1' then a.profit end) profit_zhi,

	count(distinct case when d.sales_value_zhi>0 and d.sales_value_pei>0 and a.logistics_mode_code='2' then a.customer_no end) count_cust_pei,
    sum(case when d.sales_value_zhi>0 and d.sales_value_pei>0 and a.logistics_mode_code='2' then a.sales_value end) sales_value_pei,
    sum(case when d.sales_value_zhi>0 and d.sales_value_pei>0 and a.logistics_mode_code='2' then a.profit end) profit_pei
  from 
  (
    select *
    from csx_dw.dws_sale_r_d_detail
    where sdt>${hiveconf:before1_last_mon}
    --where sdt>${hiveconf:before1_last_mon_before7}
    and sdt<=${hiveconf:current_day}
    and channel_code in('1','7','9')
    and business_type_code='1'
    and dc_code='W0A2'
    and (return_flag<>'X' or return_flag is null)   --剔除退货单
    and substr(order_no,1,1)<>'R' --差异单补救单也会是R开头，剔除
    and sales_type<>'fanli'
    and customer_no not in('116932','117001','117464','117017','120830','120595','108102')  --战略客户
  )a 
  left join 
  (
    select customer_no,customer_name,second_category_code,second_category_name,third_category_code,third_category_name
    from csx_dw.dws_crm_w_a_customer
    where sdt='current'
  )b on a.customer_no=b.customer_no
  left join 
  (
    select goods_id,goods_name,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name
    from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current'
  )c on a.goods_code=c.goods_id
  left join  
  (
    select goods_code,
	  sum(case when logistics_mode_code='1' then sales_value end) sales_value_zhi,
	  sum(case when logistics_mode_code='2' then sales_value end) sales_value_pei
    from csx_dw.dws_sale_r_d_detail
    where sdt>${hiveconf:before1_last_mon}
    --where sdt>${hiveconf:before1_last_mon_before7}
    and sdt<=${hiveconf:current_day}
    and channel_code in('1','7','9')
    and business_type_code='1'
    and dc_code='W0A2'
    and (return_flag<>'X' or return_flag is null)   --剔除退货单
    and substr(order_no,1,1)<>'R' --差异单补救单也会是R开头，剔除
    and sales_type<>'fanli'
    and customer_no not in('116932','117001','117464','117017','120830','120595','108102')  --战略客户
	group by goods_code
  )d on d.goods_code=a.goods_code
  group by c.classify_middle_code,c.classify_middle_name
)a;


--part4：TOP客户直送配送占比及差异 占比：商品数、销售额 差异：毛利率
--品类直送配送客户数、销售额占比、毛利率差异
select *
from (
select customer_no,customer_name,second_category_code,second_category_name,
  count_sku,sales_value,profit,profit/abs(sales_value) as profit_rate,
  count_sku_2,sales_value_2,profit_2,profit_2/abs(sales_value_2) as profit_rate_2,
  count_sku_zhi,sales_value_zhi,profit_zhi,profit_zhi/abs(sales_value_zhi) as profit_rate_zhi,
  count_sku_pei,sales_value_pei,profit_pei,profit_pei/abs(sales_value_pei) as profit_rate_pei,
  count_sku_zhi/count_sku_2 count_sku_zhi_rate,
  sales_value_zhi/sales_value_2 sales_value_zhi_rate,
  profit_zhi/abs(sales_value_zhi)-profit_2/abs(sales_value_2) profit_rate_zhi_diff,
  row_number() over(order by sales_value desc) rno,
  row_number() over(order by sales_value_zhi desc) rno_zhi
from
(
  select 
    a.customer_no,b.customer_name,b.second_category_code,b.second_category_name,
    count(distinct a.goods_code) count_sku,
    sum(a.sales_value) sales_value,
    sum(a.profit) profit,
	
	count(distinct case when d.sales_value_zhi>0 and d.sales_value_pei>0 then a.goods_code end) count_sku_2,
    sum(case when d.sales_value_zhi>0 and d.sales_value_pei>0 then a.sales_value end) sales_value_2,
    sum(case when d.sales_value_zhi>0 and d.sales_value_pei>0 then a.profit end) profit_2,

	count(distinct case when d.sales_value_zhi>0 and d.sales_value_pei>0 and a.logistics_mode_code='1' then a.goods_code end) count_sku_zhi,
    sum(case when d.sales_value_zhi>0 and d.sales_value_pei>0 and a.logistics_mode_code='1' then a.sales_value end) sales_value_zhi,
    sum(case when d.sales_value_zhi>0 and d.sales_value_pei>0 and a.logistics_mode_code='1' then a.profit end) profit_zhi,

	count(distinct case when d.sales_value_zhi>0 and d.sales_value_pei>0 and a.logistics_mode_code='2' then a.goods_code end) count_sku_pei,
    sum(case when d.sales_value_zhi>0 and d.sales_value_pei>0 and a.logistics_mode_code='2' then a.sales_value end) sales_value_pei,
    sum(case when d.sales_value_zhi>0 and d.sales_value_pei>0 and a.logistics_mode_code='2' then a.profit end) profit_pei
	--row_number() over(order by sum(a.sales_value) desc) rno
  from 
  (
    select *
    from csx_dw.dws_sale_r_d_detail
    where sdt>${hiveconf:before1_last_mon}
    --where sdt>${hiveconf:before1_last_mon_before7}
    and sdt<=${hiveconf:current_day}
    and channel_code in('1','7','9')
    and business_type_code='1'
    and dc_code='W0A2'
    and (return_flag<>'X' or return_flag is null)   --剔除退货单
    and substr(order_no,1,1)<>'R' --差异单补救单也会是R开头，剔除
    and sales_type<>'fanli'
    and customer_no not in('116932','117001','117464','117017','120830','120595','108102')  --战略客户
  )a 
  left join 
  (
    select customer_no,customer_name,second_category_code,second_category_name,third_category_code,third_category_name
    from csx_dw.dws_crm_w_a_customer
    where sdt='current'
  )b on a.customer_no=b.customer_no
  left join  
  (
    select goods_code,
	  sum(case when logistics_mode_code='1' then sales_value end) sales_value_zhi,
	  sum(case when logistics_mode_code='2' then sales_value end) sales_value_pei
    from csx_dw.dws_sale_r_d_detail
    where sdt>${hiveconf:before1_last_mon}
    --where sdt>${hiveconf:before1_last_mon_before7}
    and sdt<=${hiveconf:current_day}
    and channel_code in('1','7','9')
    and business_type_code='1'
    and dc_code='W0A2'
    and (return_flag<>'X' or return_flag is null)   --剔除退货单
    and substr(order_no,1,1)<>'R' --差异单补救单也会是R开头，剔除
    and sales_type<>'fanli'
    and customer_no not in('116932','117001','117464','117017','120830','120595','108102')  --战略客户
	group by goods_code
  )d on d.goods_code=a.goods_code
  group by a.customer_no,b.customer_name,b.second_category_code,b.second_category_name
)a  )a
where rno<=100 or (rno_zhi<=100 and sales_value_zhi>0);













