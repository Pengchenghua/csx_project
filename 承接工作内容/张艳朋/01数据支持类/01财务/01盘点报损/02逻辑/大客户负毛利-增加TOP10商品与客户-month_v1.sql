set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=2000;
set hive.groupby.skewindata=false;
set hive.map.aggr = true;
-- 增加reduce过程
set hive.optimize.sort.dynamic.partition=true;


--数据范围：当月月至今销售明细订单数据（截至昨日），大客户
--条件：当日负毛利金额在-500以外的客户，客户、订单、批次三个表
--退货冲销，如某客户1月1日销售额10000元，1月3日退货3000元（1月1日销售中产生的退货），则该客户1月1日的销售额与毛利额均需扣除退货部分，处理方式将退货单返回原始正向单的出库日期

set i_sdate =regexp_replace(date_sub(current_date,1),'-','');
set i_sdate_1 =regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','');
set i_sdate_2 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-2),'-','');
set i_sdate_3 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');

drop table csx_tmp.tmp_big_profit_day_001;
create table csx_tmp.tmp_big_profit_day_001
as
  select a.customer_no,a.customer_name,a.channel_name,a.province_name,a.is_factory_goods_name,
    coalesce(b.sdt,a.sdt) sales_date,
    goods_code,
    sum(a.sales_qty) sales_qty,
    sum(a.sales_value) sales_value,
    sum(a.profit) profit_d
  from
  (
    select channel_name,province_name,customer_no,customer_name,sdt,goods_code,origin_order_no,order_no,sales_qty,sales_value,profit,is_factory_goods_name
    from csx_dw.dws_sale_r_d_customer_sale 
    where sdt>=${hiveconf:i_sdate_2} 
    and sdt<=regexp_replace(date_sub(current_date(),1),"-","") 
    --and sales_type in ('qyg','gc','anhui','sc','bbc') 
    --and sales_type in ('sqpqyg','sapgc','qyg','sc','bbc') 
    and channel in('1','7','9')
  ) a
  left join 
  (
    select origin_order_no,min(sdt)sdt
    from csx_dw.dws_sale_r_d_customer_sale 
    where sdt>=${hiveconf:i_sdate_2} 
    and sdt<=regexp_replace(date_sub(current_date(),1),"-","")
    --and sales_type in ('qyg','gc','anhui','sc','bbc') 
    --and sales_type in ('sqpqyg','sapgc','qyg','sc','bbc')
    and channel in('1','7','9')
    and order_no not like 'RH%' and order_no not like 'OC%' and origin_order_no=order_no 
    group by origin_order_no
  ) b 
  on b.origin_order_no=a.origin_order_no
  where coalesce(b.sdt,a.sdt)>=${hiveconf:i_sdate_1}
  group by a.customer_no,a.customer_name,a.channel_name,a.province_name,a.is_factory_goods_name,coalesce(b.sdt,a.sdt),goods_code,a.is_factory_goods_name;


drop table csx_tmp.tmp_big_profit_day_002;
create table csx_tmp.tmp_big_profit_day_002
as
    select a.customer_no,coalesce(b.sdt,a.sdt) sdt,sum(a.profit) profit_d
  from
  (
    select customer_no,sdt,goods_code,origin_order_no,order_no,profit
    from csx_dw.dws_sale_r_d_customer_sale a
    where a.sdt>=${hiveconf:i_sdate_2} and a.sdt<=regexp_replace(date_sub(current_date(),1),"-","")
    --and sales_type in ('qyg','gc','anhui','sc','bbc') 
    --and sales_type in ('sqpqyg','sapgc','qyg','sc','bbc')
    and a.channel in('1','7','9')
  ) a
  left join 
  (
    select origin_order_no,min(sdt)sdt
    from csx_dw.dws_sale_r_d_customer_sale a
    where a.sdt>=${hiveconf:i_sdate_2} and a.sdt<=regexp_replace(date_sub(current_date(),1),"-","")
    --and sales_type in ('qyg','gc','anhui','sc','bbc') 
    --and sales_type in ('sqpqyg','sapgc','qyg','sc','bbc')
    and a.channel in('1','7','9')
    and order_no not like 'RH%' and order_no not like 'OC%' and origin_order_no=order_no 
    group by origin_order_no
  ) b 
  on b.origin_order_no=a.origin_order_no
  where coalesce(b.sdt,a.sdt)>=${hiveconf:i_sdate_1}
  group by a.customer_no,coalesce(b.sdt,a.sdt)
  having profit_d <-500;


-- 客户商品数据
drop table csx_tmp.tmp_profit_day_0;
create table csx_tmp.tmp_profit_day_0
as
select 
  a.sales_date,
  a.channel_name,a.province_name,
  --d.channel,
  --d.sales_province_code,----销售省区编码
  --d.sales_province, ----'销售省区'
  --d.first_category_code,----客户一级编码
  d.first_category, -----'客户一级分类'
  a.customer_no,
  a.customer_name,
  --a.order_kind,
  a.goods_code,
  b.goods_name,
  b.department_id, 
  b.department_name, 
  b.category_large_code, 
  b.category_large_name,
  a.is_factory_goods_name,
  sum(a.sales_value) sales_value,--销售额
  sum(a.profit_d) profit--毛利额
from 
(
  select * from csx_tmp.tmp_big_profit_day_001
  -- having profit_d <-10
) a
join --当日负毛利金额在-500以外的客户
(
 select * from csx_tmp.tmp_big_profit_day_002
)a1 
on a1.customer_no=a.customer_no and a1.sdt=a.sales_date
inner join  -----客户维度
(
  select distinct d.customer_no,d.channel,d.sales_province_code,d.sales_province,
    d.first_category_code,d.first_category 
  from csx_dw.dws_crm_w_a_customer_m_V1 d  
  where sdt=regexp_replace(date_sub(current_date(),1),"-","") and source<>'dev' and  customer_no<>''
) d on a.customer_no=d.customer_no 
left join  
  csx_dw.dws_basic_w_a_csx_product_m b on a.goods_code=b.goods_id and b.sdt='current'-----商品维度
group by a.sales_date,
a.channel_name,a.province_name,
--d.channel,
--d.sales_province_code,----销售省区编码
--d.sales_province, ----'销售省区'
--d.first_category_code,----客户一级编码
d.first_category, -----'客户一级分类'
a.customer_no,
a.customer_name,
--a.order_kind,
a.goods_code,
b.goods_name,
b.department_id, 
b.department_name, 
b.category_large_code, 
b.category_large_name,
a.is_factory_goods_name;





--各省区月至今 客户每天负毛利总额-500以上-订单明细
drop table csx_tmp.tmp_profit_day_1;
create table csx_tmp.tmp_profit_day_1
as
select 
a.sales_date,
a.origin_order_no,
a.order_no,
a.channel_name,
g.region_code,
g.region_name,
a.province_code,
a.province_name, ----'销售省区'
a.first_category, -----'客户一级分类'
a.customer_no,
a.customer_name,
a.goods_code,
regexp_replace(regexp_replace(b.goods_name,'\n',''),'\r','') as goods_name,
b.department_id, 
b.department_name, 
b.category_large_code, 
b.category_large_name,
a.is_factory_goods_name,
a.sales_qty sales_qty,--销售量
a.sales_value sales_value,--销售额
coalesce(f.order_entry_cost,e.order_entry_cost,a.cost_price) order_entry_cost, --`平均批次操作单价`
--a.cost_price order_entry_cost, --`平均批次操作单价` --老系统用商品进价
a.purchase_price purchase_price,--`采购报价`
a.middle_report_price middle_report_price,--`中台报价`
a.price price,--`售价`
a.profit,--毛利额
a.front_profit--`前端毛利`
--'' factory_share_adjust_cost,--`工厂月末分摊成本`
--'' negative_stock_adjust_cost,--`负库存调整成本`
--'' daily_stock_diff_adjust_cost,--`日常差异库存调整成本`
--'' finance_profit--`调整后毛利`
from(
select coalesce(b.sdt,a.sdt) sales_date,a.channel_name,a.province_code,a.province_name,
a.origin_order_no,a.order_no,a.customer_no,a.customer_name,a.goods_code,a.goods_name,a.is_factory_goods_name,a.sales_qty,a.sales_value,a.purchase_price,
a.middle_office_price middle_report_price,a.promotion_price price,a.profit,a.front_profit,a.sdt,a.cost_price,
d.channel,d.sales_province_code,d.sales_province,d.first_category
from 
(select * from csx_dw.dws_sale_r_d_customer_sale  
where sdt>=${hiveconf:i_sdate_2} and sdt<=regexp_replace(date_sub(current_date(),1),"-","")
--and sales_type in ('qyg','gc','anhui','sc','bbc')
and channel in('1','7','9')
--and ((order_no not like 'RH%' and profit<-50) or (order_no like 'RH%' and profit>0))
) a
left join 
(select origin_order_no,min(sdt)sdt
from csx_dw.dws_sale_r_d_customer_sale  
where sdt>=${hiveconf:i_sdate_2} and sdt<=regexp_replace(date_sub(current_date(),1),"-","")
--and sales_type in ('qyg','gc','anhui','sc','bbc')
and channel in('1','7','9')
and order_no not like 'RH%' and order_no not like 'OC%' and origin_order_no=order_no 
group by origin_order_no) b on b.origin_order_no=a.origin_order_no
inner join  -----客户维度
(select distinct d.customer_no,d.channel,d.sales_province_code,d.sales_province,
  d.first_category_code,d.first_category 
  from csx_dw.dws_crm_w_a_customer_m_V1 d  
  where sdt=regexp_replace(date_sub(current_date(),1),"-","") and source<>'dev' and  customer_no<>''
) d on a.customer_no=d.customer_no  
) a
join --当日负毛利金额在-500以外的客户
(select distinct customer_no,sales_date from csx_tmp.tmp_profit_day_0
)a1 on a1.customer_no=a.customer_no and a1.sales_date=a.sales_date
left join  (select * from csx_dw.dws_basic_w_a_csx_product_m where sdt='current')b on a.goods_code=b.goods_id -----商品维度
left join ----批次操作入库成本价
(select link_wms_order_no,goods_code,sum(qty) as qty,sum(amt) as amt,sum(amt)/sum(qty) order_entry_cost
  from csx_dw.dws_wms_r_d_accounting_stock_operation_item_m   
  where sdt>=${hiveconf:i_sdate_1} 
  and ((in_or_out=1 and move_type ='107A') or (in_or_out=0 and move_type='108A'))
--  and substr(link_wms_order_no,1,2) in('OH','OM','OS','OY','RH','TD')
  group by link_wms_order_no,goods_code  
)e on a.goods_code=e.goods_code and  a.order_no=e.link_wms_order_no
left join ----批次操作入库成本价
(select link_wms_order_no,goods_code,sum(qty) as qty,sum(amt) as amt,sum(amt)/sum(qty) order_entry_cost
  from csx_dw.dws_wms_r_d_accounting_stock_operation_item_m 
  where sdt>=${hiveconf:i_sdate_1}  
  and in_or_out=1 and move_type in ('114A')
--  and substr(link_wms_order_no,1,2) in('OH','OM','OS','OY','RH','TD')
  group by link_wms_order_no,goods_code  
)f on a.goods_code=f.goods_code and  a.order_no=f.link_wms_order_no
left join (select province_code,province_name,region_code,region_name from csx_dw.dim_area where area_rank='13')g on g.province_code=a.province_code
;


--批次明细
drop table csx_tmp.tmp_big_profit_day_2_00;
create table csx_tmp.tmp_big_profit_day_2_00
as
  select link_wms_order_no,goods_code,link_wms_batch_no,batch_no,link_wms_entry_order_type,
    round(sum(link_wms_entry_qty),6)link_wms_entry_qty,
    round(sum(link_wms_entry_amt),6)link_wms_entry_amt,
    round(sum(link_wms_entry_amt)/sum(link_wms_entry_qty),6) link_wms_entry_price,
    round(sum(qty),6)qty,
    round(sum(amt),6)amt,
    round(sum(amt)/sum(qty),6) price 
  from csx_dw.dws_wms_r_d_accounting_stock_operation_item_m 
  where sdt>=${hiveconf:i_sdate_1}  
  and in_or_out=1 and move_type='114A'
  --and substr(link_wms_order_no,1,2) in('OH','OM','OS','OY','RH','TD')
  group by link_wms_order_no,goods_code,link_wms_batch_no,batch_no,link_wms_entry_order_type

  union all
  select a.link_wms_order_no,a.goods_code,a.link_wms_batch_no,a.batch_no,a.link_wms_entry_order_type,
    round(sum(a.link_wms_entry_qty),6)link_wms_entry_qty,
    round(sum(a.link_wms_entry_amt),6)link_wms_entry_amt,
    round(sum(a.link_wms_entry_amt)/sum(a.link_wms_entry_qty),6) link_wms_entry_price,
    round(sum(a.qty),6)qty,
    round(sum(a.amt),6)amt,
    round(sum(a.amt)/sum(a.qty),6) price
  from csx_dw.dws_wms_r_d_accounting_stock_operation_item_m a
  left join 
  (
    select distinct link_wms_order_no,batch_no,goods_code
    from csx_dw.dws_wms_r_d_accounting_stock_operation_item_m
    where sdt>=${hiveconf:i_sdate_1} 
    and in_or_out=1 and move_type='114A' 
    --and substr(link_wms_order_no,1,2) in('OH','OM','OS','OY','RH','TD')
  )b on b.goods_code=a.goods_code and  b.link_wms_order_no=a.link_wms_order_no  
  where a.sdt>=${hiveconf:i_sdate_1}  
  and ((in_or_out=1 and move_type ='107A') or (in_or_out=0 and move_type='108A'))
  --and substr(a.link_wms_order_no,1,2) in('OH','OM','OS','OY','RH','TD')
  and b.link_wms_order_no is null
  group by a.link_wms_order_no,a.goods_code,a.link_wms_batch_no,a.batch_no,a.link_wms_entry_order_type;


drop table csx_tmp.tmp_profit_day_2;
create table csx_tmp.tmp_profit_day_2
as
select 
  a.sales_date,
  a.origin_order_no,
  a.order_no,
  a.channel_name,
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name, 
  a.first_category, -----'客户一级分类'
  a.customer_no,
  a.customer_name,
  a.goods_code,
  regexp_replace(regexp_replace(a.goods_name,'\n',''),'\r','') as goods_name,
  a.department_id, 
  a.department_name, 
  a.category_large_code, 
  a.category_large_name,
  a.is_factory_goods_name,
  --a.is_partner,
  --a.profit_d, 
  a.sales_qty,--销售量
  a.sales_value,--销售额
  a.order_entry_cost, --`平均批次操作单价`
  a.purchase_price,--`采购报价`
  a.middle_report_price,--`中台报价`
  a.price,--`售价`
  a.profit,--毛利额
  a.front_profit,--`前端毛利`
  --a.factory_share_adjust_cost,--`工厂月末分摊成本`
  --a.negative_stock_adjust_cost,--`负库存调整成本`
  --a.daily_stock_diff_adjust_cost,--`日常差异库存调整成本`
  --a.finance_profit,--`调整后毛利`

  b.link_wms_batch_no link_wms_entry_batch_no,
  b.batch_no,
  b.link_wms_entry_order_type, --批次入库
  b.link_wms_entry_qty,--批次入库 数量、金额、单价
  b.link_wms_entry_amt,
  b.link_wms_entry_price, 
  b.qty,  --订单批次出库 数量、金额，入库单价
  b.amt,
  b.amt/b.qty entry_price,
  (a.price-b.price)*b.qty profit1,
  concat(round(((a.price-b.price)/b.price)*100,2),'%') prorate
  --a.sales_date as sales_date  
from csx_tmp.tmp_profit_day_1 a
left join --移动批次采购数量、金额，批次对应订单出库数量、金额
(
  select distinct 
    link_wms_order_no,goods_code,link_wms_batch_no,batch_no,link_wms_entry_order_type,
  link_wms_entry_qty,link_wms_entry_amt,link_wms_entry_price,qty,amt,price 
  from
  (
    select * from csx_tmp.tmp_big_profit_day_2_00
  )b
) b on a.goods_code=b.goods_code and a.order_no=b.link_wms_order_no;


--TOP10需求新增表
-------------------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------------

--全国&大区-订单明细
drop table csx_tmp.tmp_profit_day_11;
create temporary table csx_tmp.tmp_profit_day_11
as
select 
sales_date,origin_order_no,order_no,channel_name,'0' region_code,'全国' region_name,province_code,province_name,
first_category,customer_no,customer_name,goods_code,goods_name,department_id,department_name,
category_large_code,category_large_name,is_factory_goods_name,sales_qty,sales_value,
order_entry_cost,purchase_price,middle_report_price,price,profit,front_profit
from csx_tmp.tmp_profit_day_1
union all
select 
sales_date,origin_order_no,order_no,channel_name,region_code,region_name,province_code,province_name,
first_category,customer_no,customer_name,goods_code,goods_name,department_id,department_name,
category_large_code,category_large_name,is_factory_goods_name,sales_qty,sales_value,
order_entry_cost,purchase_price,middle_report_price,price,profit,front_profit
from csx_tmp.tmp_profit_day_1;


insert overwrite table csx_tmp.ads_fr_profit_day  partition(smonth)
select 
sales_date,origin_order_no,order_no,channel_name,region_code,region_name,province_code,province_name,
first_category,customer_no,customer_name,goods_code,goods_name,department_id,department_name,
category_large_code,category_large_name,is_factory_goods_name,sales_qty,sales_value,
order_entry_cost,purchase_price,middle_report_price,price,profit,front_profit,
from_utc_timestamp(current_timestamp(),'GMT') write_time,
substr(${hiveconf:i_sdate},1,6) smonth
from csx_tmp.tmp_profit_day_1;





--商品TOP10-月至今
drop table csx_tmp.tmp_profit_day_goods_top_M;
create temporary table csx_tmp.tmp_profit_day_goods_top_M
as
select a.*
from
	(
	select region_code,region_name,goods_code,goods_name,
		sum(sales_qty) sales_qty,--销售量
		sum(sales_value) sales_value,--销售额
		sum(profit) profit,--毛利额
		sum(if(substr(order_no,1,2) in('RH','OC'),profit,if(profit<0,profit,0))) profit_f,--负毛利额
		sum(order_entry_cost*sales_qty)/sum(sales_qty) order_entry_cost, --`平均批次操作单价`
		sum(purchase_price*sales_qty)/sum(sales_qty) purchase_price,--`采购报价`
		sum(middle_report_price*sales_qty)/sum(sales_qty) middle_report_price,--`中台报价`
		sum(price*sales_qty)/sum(sales_qty) price, --`售价`
		row_number() over(partition by region_code,region_name order by sum(profit) asc) rno
	from csx_tmp.tmp_profit_day_11
	group by region_code,region_name,goods_code,goods_name
	having sum(sales_value)>0	
	)a
where rno<=10;

--商品TOP10-月至今：商品、商品省区、商品客户
drop table csx_tmp.tmp_profit_day_goods_top_M1;
create table csx_tmp.tmp_profit_day_goods_top_M1
as
select a.region_code,a.region_name,b.rno,a.goods_code,a.goods_name,
b.order_entry_cost,b.purchase_price,b.middle_report_price,b.price,
b.sales_qty,b.sales_value,b.profit,b.profit_f,
concat(a.province_name,':',round(a.profit_f/b.profit_f*100,0),'%') dist_rate,
concat(c.customer_no,'_',c.customer_name,':',round(c.profit_f/b.profit_f*100,0),'%') cust_rate
from
	(	
	select region_code,region_name,goods_code,goods_name,province_name,
		sum(sales_qty) sales_qty,--销售量
		sum(sales_value) sales_value,--销售额
		sum(profit) profit,--毛利额
		sum(if(substr(order_no,1,2) in('RH','OC'),profit,if(profit<0,profit,0))) profit_f,--负毛利额
		row_number() over(partition by region_code,region_name,goods_code,goods_name order by sum(profit) asc) rno
	from 
		(
		select a.*
		from csx_tmp.tmp_profit_day_11 a
		join csx_tmp.tmp_profit_day_goods_top_M b on b.goods_code=a.goods_code and b.region_code=a.region_code
		)a
	group by region_code,region_name,goods_code,goods_name,province_name
	--having sum(sales_value)>0
	)a
join csx_tmp.tmp_profit_day_goods_top_M b on b.goods_code=a.goods_code and b.region_code=a.region_code	
join 
	(	
	select region_code,region_name,goods_code,goods_name,customer_no,customer_name,
		sum(sales_qty) sales_qty,--销售量
		sum(sales_value) sales_value,--销售额
		sum(profit) profit,--毛利额
		sum(if(substr(order_no,1,2) in('RH','OC'),profit,if(profit<0,profit,0))) profit_f,--负毛利额
		row_number() over(partition by region_code,region_name,goods_code,goods_name order by sum(profit) asc) rno
	from 
		(
		select a.*
		from csx_tmp.tmp_profit_day_11 a
		join csx_tmp.tmp_profit_day_goods_top_M b on b.goods_code=a.goods_code and b.region_code=a.region_code
		)a
	group by region_code,region_name,goods_code,goods_name,customer_no,customer_name
	--having sum(sales_value)>0
	)c on c.goods_code=b.goods_code and c.region_code=a.region_code
where a.rno=1 and c.rno=1
order by a.region_code,b.rno;	



--商品TOP10-昨日
drop table csx_tmp.tmp_profit_day_goods_top_D;
create temporary table csx_tmp.tmp_profit_day_goods_top_D
as
select a.*
from
	(
	select region_code,region_name,goods_code,goods_name,
		sum(sales_qty) sales_qty,--销售量
		sum(sales_value) sales_value,--销售额
		sum(profit) profit,--毛利额
		sum(if(substr(order_no,1,2) in('RH','OC'),profit,if(profit<0,profit,0))) profit_f,--负毛利额
		sum(order_entry_cost*sales_qty)/sum(sales_qty) order_entry_cost, --`平均批次操作单价`
		sum(purchase_price*sales_qty)/sum(sales_qty) purchase_price,--`采购报价`
		sum(middle_report_price*sales_qty)/sum(sales_qty) middle_report_price,--`中台报价`
		sum(price*sales_qty)/sum(sales_qty) price, --`售价`
		row_number() over(partition by region_code,region_name order by sum(profit) asc) rno
	from csx_tmp.tmp_profit_day_11
	where sales_date=regexp_replace(date_sub(current_date,1),'-','')
	group by region_code,region_name,goods_code,goods_name
	having sum(sales_value)>0	
	)a
where rno<=10;

--商品TOP10-昨日：商品、商品省区、商品客户
drop table csx_tmp.tmp_profit_day_goods_top_D1;
create table csx_tmp.tmp_profit_day_goods_top_D1
as
select a.region_code,a.region_name,b.rno,a.goods_code,a.goods_name,
b.order_entry_cost,b.purchase_price,b.middle_report_price,b.price,
b.sales_qty,b.sales_value,b.profit,b.profit_f,
concat(a.province_name,':',round(a.profit_f/b.profit_f*100,0),'%') dist_rate,
concat(c.customer_no,'_',c.customer_name,':',round(c.profit_f/b.profit_f*100,0),'%') cust_rate
from
	(	
	select region_code,region_name,goods_code,goods_name,province_name,
		sum(sales_qty) sales_qty,--销售量
		sum(sales_value) sales_value,--销售额
		sum(profit) profit,--毛利额
		sum(if(substr(order_no,1,2) in('RH','OC'),profit,if(profit<0,profit,0))) profit_f,--负毛利额
		row_number() over(partition by region_code,region_name,goods_code,goods_name order by sum(profit) asc) rno
	from 
		(
		select a.*
		from csx_tmp.tmp_profit_day_11 a
		join csx_tmp.tmp_profit_day_goods_top_D b on b.goods_code=a.goods_code and b.region_code=a.region_code
		where sales_date=regexp_replace(date_sub(current_date,1),'-','')
		)a
	group by region_code,region_name,goods_code,goods_name,province_name
	--having sum(sales_value)>0
	)a
join csx_tmp.tmp_profit_day_goods_top_D b on b.goods_code=a.goods_code and b.region_code=a.region_code	
join 
	(	
	select region_code,region_name,goods_code,goods_name,customer_no,customer_name,
		sum(sales_qty) sales_qty,--销售量
		sum(sales_value) sales_value,--销售额
		sum(profit) profit,--毛利额
		sum(if(substr(order_no,1,2) in('RH','OC'),profit,if(profit<0,profit,0))) profit_f,--负毛利额
		row_number() over(partition by region_code,region_name,goods_code,goods_name order by sum(profit) asc) rno
	from 
		(
		select a.*
		from csx_tmp.tmp_profit_day_11 a
		join csx_tmp.tmp_profit_day_goods_top_D b on b.goods_code=a.goods_code and b.region_code=a.region_code
		where sales_date=regexp_replace(date_sub(current_date,1),'-','')
		)a
	group by region_code,region_name,goods_code,goods_name,customer_no,customer_name
	--having sum(sales_value)>0
	)c on c.goods_code=b.goods_code and c.region_code=a.region_code
where a.rno=1 and c.rno=1
order by a.region_code,b.rno;


---------------------------------------------------------------------------------------------------------


--客户TOP10-月至今
drop table csx_tmp.tmp_profit_day_cust_top_M;
create temporary table csx_tmp.tmp_profit_day_cust_top_M
as
select a.*
from
	(
	select region_code,region_name,customer_no,customer_name,
		sum(sales_value) sales_value,--销售额
		sum(profit) profit,--毛利额
		sum(if(substr(order_no,1,2) in('RH','OC'),profit,if(profit<0,profit,0))) profit_f,--负毛利额
		row_number() over(partition by region_code,region_name order by sum(profit) asc) rno
	from csx_tmp.tmp_profit_day_11
	group by region_code,region_name,customer_no,customer_name
	having sum(sales_value)>0	
	)a
where rno<=10;

--客户TOP10-月至今：商品、商品客户
drop table csx_tmp.tmp_profit_day_cust_top_M1;
create table csx_tmp.tmp_profit_day_cust_top_M1
as
select a.region_code,a.region_name,b.rno,a.customer_no,a.customer_name,a.province_name,
b.sales_value,b.profit,b.profit_f,
concat(c.goods_code,'_',c.goods_name,':',round(c.profit_f/b.profit_f*100,0),'%') cust_rate
from
	(	
	select region_code,region_name,customer_no,customer_name,province_name,
		sum(sales_value) sales_value,--销售额
		sum(profit) profit,--毛利额
		sum(if(substr(order_no,1,2) in('RH','OC'),profit,if(profit<0,profit,0))) profit_f,--负毛利额
		row_number() over(partition by region_code,region_name,customer_no,customer_name order by sum(profit) asc) rno
	from 
		(
		select a.*
		from csx_tmp.tmp_profit_day_11 a
		join csx_tmp.tmp_profit_day_cust_top_M b on b.customer_no=a.customer_no and b.region_code=a.region_code
		)a
	group by region_code,region_name,customer_no,customer_name,province_name
	--having sum(sales_value)>0
	)a
join csx_tmp.tmp_profit_day_cust_top_M b on b.customer_no=a.customer_no and b.region_code=a.region_code	
join 
	(	
	select region_code,region_name,customer_no,customer_name,goods_code,goods_name,
		sum(sales_value) sales_value,--销售额
		sum(profit) profit,--毛利额
		sum(if(substr(order_no,1,2) in('RH','OC'),profit,if(profit<0,profit,0))) profit_f,--负毛利额
		row_number() over(partition by region_code,region_name,customer_no,customer_name order by sum(profit) asc) rno
	from 
		(
		select a.*
		from csx_tmp.tmp_profit_day_11 a
		join csx_tmp.tmp_profit_day_cust_top_M b on b.customer_no=a.customer_no and b.region_code=a.region_code
		)a
	group by region_code,region_name,customer_no,customer_name,goods_code,goods_name
	--having sum(sales_value)>0
	)c on c.customer_no=b.customer_no and c.region_code=a.region_code
where a.rno=1 and c.rno=1
order by a.region_code,b.rno;	



--客户TOP10-昨日
drop table csx_tmp.tmp_profit_day_cust_top_D;
create temporary table csx_tmp.tmp_profit_day_cust_top_D
as
select a.*
from
	(
	select region_code,region_name,customer_no,customer_name,
		sum(sales_value) sales_value,--销售额
		sum(profit) profit,--毛利额
		sum(if(substr(order_no,1,2) in('RH','OC'),profit,if(profit<0,profit,0))) profit_f,--负毛利额
		row_number() over(partition by region_code,region_name order by sum(profit) asc) rno
	from csx_tmp.tmp_profit_day_11
	where sales_date=regexp_replace(date_sub(current_date,1),'-','')
	group by region_code,region_name,customer_no,customer_name
	having sum(sales_value)>0	
	)a
where rno<=10;

--客户TOP10-昨日：商品、商品省区、商品客户
drop table csx_tmp.tmp_profit_day_cust_top_D1;
create table csx_tmp.tmp_profit_day_cust_top_D1
as
select a.region_code,a.region_name,b.rno,a.customer_no,a.customer_name,a.province_name,
b.sales_value,b.profit,b.profit_f,
concat(c.goods_code,'_',c.goods_name,':',round(c.profit_f/b.profit_f*100,0),'%') cust_rate
from
	(	
	select region_code,region_name,customer_no,customer_name,province_name,
		sum(sales_value) sales_value,--销售额
		sum(profit) profit,--毛利额
		sum(if(substr(order_no,1,2) in('RH','OC'),profit,if(profit<0,profit,0))) profit_f,--负毛利额
		row_number() over(partition by region_code,region_name,customer_no,customer_name order by sum(profit) asc) rno
	from 
		(
		select a.*
		from csx_tmp.tmp_profit_day_11 a
		join csx_tmp.tmp_profit_day_cust_top_D b on b.customer_no=a.customer_no and b.region_code=a.region_code
		where sales_date=regexp_replace(date_sub(current_date,1),'-','')
		)a
	group by region_code,region_name,customer_no,customer_name,province_name
	--having sum(sales_value)>0
	)a
join csx_tmp.tmp_profit_day_cust_top_D b on b.customer_no=a.customer_no and b.region_code=a.region_code	
join 
	(	
	select region_code,region_name,customer_no,customer_name,goods_code,goods_name,
		sum(sales_value) sales_value,--销售额
		sum(profit) profit,--毛利额
		sum(if(substr(order_no,1,2) in('RH','OC'),profit,if(profit<0,profit,0))) profit_f,--负毛利额
		row_number() over(partition by region_code,region_name,customer_no,customer_name order by sum(profit) asc) rno
	from 
		(
		select a.*
		from csx_tmp.tmp_profit_day_11 a
		join csx_tmp.tmp_profit_day_cust_top_D b on b.customer_no=a.customer_no and b.region_code=a.region_code
		where sales_date=regexp_replace(date_sub(current_date,1),'-','')
		)a
	group by region_code,region_name,customer_no,customer_name,goods_code,goods_name
	--having sum(sales_value)>0
	)c on c.customer_no=b.customer_no and c.region_code=a.region_code
where a.rno=1 and c.rno=1
order by a.region_code,b.rno;






insert overwrite table csx_tmp.ads_fr_profit_day_goods_top_M  partition(smonth)
select 
region_code,region_name,rno,
goods_code,goods_name,
order_entry_cost,purchase_price,middle_report_price,price,
sales_qty,sales_value,profit,profit_f,
dist_rate,cust_rate,
from_utc_timestamp(current_timestamp(),'GMT') write_time,
substr(${hiveconf:i_sdate},1,6) smonth
from csx_tmp.tmp_profit_day_goods_top_M1;

insert overwrite table csx_tmp.ads_fr_profit_day_goods_top_D  partition(smonth)
select 
region_code,region_name,rno,
goods_code,goods_name,
order_entry_cost,purchase_price,middle_report_price,price,
sales_qty,sales_value,profit,profit_f,
dist_rate,cust_rate,
from_utc_timestamp(current_timestamp(),'GMT') write_time,
substr(${hiveconf:i_sdate},1,6) smonth
from csx_tmp.tmp_profit_day_goods_top_D1;

insert overwrite table csx_tmp.ads_fr_profit_day_cust_top_M  partition(smonth)
select 
region_code,region_name,rno,
customer_no,customer_name,province_name,
sales_value,profit,profit_f,cust_rate,
from_utc_timestamp(current_timestamp(),'GMT') write_time,
substr(${hiveconf:i_sdate},1,6) smonth
from csx_tmp.tmp_profit_day_cust_top_M1;

insert overwrite table csx_tmp.ads_fr_profit_day_cust_top_D  partition(smonth)
select 
region_code,region_name,rno,
customer_no,customer_name,province_name,
sales_value,profit,profit_f,cust_rate,
from_utc_timestamp(current_timestamp(),'GMT') write_time,
substr(${hiveconf:i_sdate},1,6) smonth
from csx_tmp.tmp_profit_day_cust_top_D1;


INVALIDATE METADATA csx_tmp.ads_fr_profit_day;
INVALIDATE METADATA csx_tmp.ads_fr_profit_day_goods_top_M;
INVALIDATE METADATA csx_tmp.ads_fr_profit_day_goods_top_D;
INVALIDATE METADATA csx_tmp.ads_fr_profit_day_cust_top_M;
INVALIDATE METADATA csx_tmp.ads_fr_profit_day_cust_top_D;


/*
---------------------------------------------------------------------------------------------------------
---------------------------------------------hive 建表语句-----------------------------------------------
--大客户负毛利订单明细 csx_tmp.ads_fr_profit_day
--大客户负毛利_商品TOP10-月至今 csx_tmp.ads_fr_profit_day_goods_top_M
--大客户负毛利_商品TOP10-昨日 csx_tmp.ads_fr_profit_day_goods_top_D
--大客户负毛利_客户TOP10-月至今 csx_tmp.ads_fr_profit_day_cust_top_M
--大客户负毛利_客户TOP10-昨日 csx_tmp.ads_fr_profit_day_cust_top_D

drop table if exists csx_tmp.ads_fr_profit_day;
create table csx_tmp.ads_fr_profit_day(
  `sales_date` string COMMENT '销售日期',
  `origin_order_no` string COMMENT '原始订单号',
  `order_no` string COMMENT '订单号',
  `channel_name` string COMMENT '渠道',
  `region_code` string COMMENT '大区编号',
  `region_name` string COMMENT '大区',
  `province_code` string COMMENT '省区编号',
  `province_name` string COMMENT '省区',
  `first_category` string COMMENT '客户一级分类',
  `customer_no` string COMMENT '客户编号',
  `customer_name` string COMMENT '客户名称',
  `goods_code` string COMMENT '商品编号',
  `goods_name` string COMMENT '商品名称',
  `department_id` string COMMENT '课组编号',
  `department_name` string COMMENT '课组名称',
  `category_large_code` string COMMENT '商品大类编码',
  `category_large_name` string COMMENT '商品大类名称',
  `is_factory_goods_name` string COMMENT '是否工厂商品',
  `sales_qty` decimal(26,6)  COMMENT '销售量',
  `sales_value` decimal(26,6)  COMMENT '销售额',
  `order_entry_cost` decimal(26,6)  COMMENT '成本价',
  `purchase_price` decimal(26,6)  COMMENT '采购报价',
  `middle_report_price` decimal(26,6)  COMMENT '中台报价',
  `price` decimal(26,6)  COMMENT '售价',
  `profit` decimal(26,6)  COMMENT '毛利额',
  `front_profit` decimal(26,6)  COMMENT '前端毛利', 
  `write_time` timestamp comment '更新时间'
) COMMENT '大客户负毛利订单明细'
PARTITIONED BY (smonth string COMMENT '日期分区')
STORED AS TEXTFILE;


drop table if exists csx_tmp.ads_fr_profit_day_goods_top_M;
create table csx_tmp.ads_fr_profit_day_goods_top_M(
  `region_code` string COMMENT '大区编号',
  `region_name` string COMMENT '大区',
  `rno` string COMMENT '排名',
  `goods_code` string COMMENT '商品编号',
  `goods_name` string COMMENT '商品名称',
  `order_entry_cost` decimal(26,6)  COMMENT '批次入库价',
  `purchase_price` decimal(26,6)  COMMENT ' 采购报价',
  `middle_report_price` decimal(26,6)  COMMENT ' 中台报价',
  `price` decimal(26,6)  COMMENT ' 售价',
  `sales_qty` decimal(26,6)  COMMENT '销售量',
  `sales_value` decimal(26,6)  COMMENT ' 销售额',
  `profit` decimal(26,6)  COMMENT ' 毛利额',
  `profit_f` decimal(26,6)  COMMENT '负毛利',
  `dist_rate` string COMMENT '主要省区负毛利占比',
  `cust_rate` string COMMENT '主要客户负毛利占比',
  `write_time` timestamp comment '更新时间'
) COMMENT '大客户负毛利_商品TOP10-月至今'
PARTITIONED BY (smonth string COMMENT '日期分区')
STORED AS TEXTFILE;

drop table if exists csx_tmp.ads_fr_profit_day_goods_top_D;
create table csx_tmp.ads_fr_profit_day_goods_top_D(
  `region_code` string COMMENT '大区编号',
  `region_name` string COMMENT '大区',
  `rno` string COMMENT '排名',
  `goods_code` string COMMENT '商品编号',
  `goods_name` string COMMENT '商品名称',
  `order_entry_cost` decimal(26,6)  COMMENT '批次入库价',
  `purchase_price` decimal(26,6)  COMMENT ' 采购报价',
  `middle_report_price` decimal(26,6)  COMMENT ' 中台报价',
  `price` decimal(26,6)  COMMENT ' 售价',
  `sales_qty` decimal(26,6)  COMMENT '销售量',
  `sales_value` decimal(26,6)  COMMENT ' 销售额',
  `profit` decimal(26,6)  COMMENT ' 毛利额',
  `profit_f` decimal(26,6)  COMMENT '负毛利',
  `dist_rate` string COMMENT '主要省区负毛利占比',
  `cust_rate` string COMMENT '主要客户负毛利占比',
  `write_time` timestamp comment '更新时间'
) COMMENT '大客户负毛利_商品TOP10-昨日'
PARTITIONED BY (smonth string COMMENT '日期分区')
STORED AS TEXTFILE;

drop table if exists csx_tmp.ads_fr_profit_day_cust_top_M;
create table csx_tmp.ads_fr_profit_day_cust_top_M(
  `region_code` string COMMENT '大区编号',
  `region_name` string COMMENT '大区',
  `rno` string COMMENT '排名',
  `customer_no` string COMMENT '客户编号',
  `customer_name` string COMMENT '客户名称',
  `province_name` string COMMENT '省区',
  `sales_value` decimal(26,6)  COMMENT '销售额',
  `profit` decimal(26,6)  COMMENT ' 毛利额',
  `profit_f` decimal(26,6)  COMMENT ' 负毛利',
  `cust_rate` string COMMENT '主要商品负毛利占比',
  `write_time` timestamp comment '更新时间'
) COMMENT '大客户负毛利_客户TOP10-月至今'
PARTITIONED BY (smonth string COMMENT '日期分区')
STORED AS TEXTFILE;

drop table if exists csx_tmp.ads_fr_profit_day_cust_top_D;
create table csx_tmp.ads_fr_profit_day_cust_top_D(
  `region_code` string COMMENT '大区编号',
  `region_name` string COMMENT '大区',
  `rno` string COMMENT '排名',
  `customer_no` string COMMENT '客户编号',
  `customer_name` string COMMENT '客户名称',
  `province_name` string COMMENT '省区',
  `sales_value` decimal(26,6)  COMMENT '销售额',
  `profit` decimal(26,6)  COMMENT ' 毛利额',
  `profit_f` decimal(26,6)  COMMENT ' 负毛利',
  `cust_rate` string COMMENT '主要商品负毛利占比',
  `write_time` timestamp comment '更新时间'
) COMMENT '大客户负毛利_客户TOP10-昨日'
PARTITIONED BY (smonth string COMMENT '日期分区')
STORED AS TEXTFILE;
*/
