set i_sdate_w11 =regexp_replace(date_sub(current_date,1),'-','');  ------昨日
set i_sdate_w12 =regexp_replace(date_sub(current_date,2),'-','');  ------前日
set i_sdate_w22 =regexp_replace(date_sub(current_date,30),'-','');  ------一个月前
set i_sdate_m =substr(regexp_replace(date_sub(current_date,1),'-',''),1,6);
--日维度

drop table csx_tmp.tmp_r_sales_top_customer_middle01;
create table csx_tmp.tmp_r_sales_top_customer_middle01
as
select
	province_code
	,province_name
	,city_group_code
	,city_group_name
	,customer_no
	,customer_name
	,kehu_sales_value
	,round((kehu_sales_value-kehusq_sales_value)/kehusq_sales_value,6) huanbi_sales_value
	,round(kehu_profit/abs(kehu_sales_value),6) profitlv
	,round(kehu_profit/abs(kehu_sales_value)-kehusq_profit/abs(kehusq_sales_value),6) huanbi_profitlv
	,classify_middle_code
	,classify_middle_name
	,sales_value
	,round(profit/abs(sales_value),6) profitlv_pinlei
	,round(profit/abs(sales_value),6)-round(zri_profit/abs(zri_sales_value),6) pl_huanbi_profitlv
	,round((sales_cost/sales_qty)/(zri_sales_cost/zri_sales_qty)-1,6) huanbi_cost_price
	,round((sales_value/sales_qty)/(zri_sales_value/zri_sales_qty)-1,6) huanbi_sales_price
from 
	(
	select
		a.province_code
		,a.province_name
		,a.city_group_code
		,a.city_group_name
		,a.customer_no
		,a.customer_name
		,classify_middle_code
		,classify_middle_name
		,sales_value
		,profit
		,zri_sales_value
		,zri_profit
		,sales_qty
		,sales_cost
		,zri_sales_qty
		,zri_sales_cost
		,sum(sales_value) over (partition by a.customer_no) as kehu_sales_value
		,sum(profit) over (partition by a.customer_no)  as kehu_profit
		,sum(zri_sales_value) over (partition by a.customer_no) as kehusq_sales_value 
		,sum(zri_profit) over (partition by a.customer_no)  as kehusq_profit
	from 
		(
		select 
			province_code
			,province_name
			,city_group_code
			,city_group_name
			,customer_no
			,customer_name
		from 
			csx_dw.report_sale_r_m_top_customer
		where 
			month=${hiveconf:i_sdate_m}
		)a
		join 
			(
			select
				customer_no,
				b.classify_middle_code,
				b.classify_middle_name,
				sum(if(sdt=${hiveconf:i_sdate_w11},sales_value,0)) sales_value,
				sum(if(sdt=${hiveconf:i_sdate_w11},profit,0)) profit,
				sum(if(sdt=${hiveconf:i_sdate_w12},sales_value,0))  zri_sales_value,
				sum(if(sdt=${hiveconf:i_sdate_w12},profit,0)) zri_profit,
				sum(if(sdt=${hiveconf:i_sdate_w11},sales_qty,0)) sales_qty,
				sum(if(sdt=${hiveconf:i_sdate_w11},sales_cost,0)) sales_cost,
				sum(if(sdt=${hiveconf:i_sdate_w12},sales_qty,0))  zri_sales_qty,
				sum(if(sdt=${hiveconf:i_sdate_w12},sales_cost,0)) zri_sales_cost	
			from 
				csx_dw.dws_sale_r_d_detail a
				left join 
					(
					select
						goods_id,
						classify_middle_code,
						classify_middle_name
					from  
						csx_dw.dws_basic_w_a_csx_product_m 
					where 
						sdt = 'current'
					) b on b.goods_id = a.goods_code
			where 
				sdt >=${hiveconf:i_sdate_w12} and sdt <=${hiveconf:i_sdate_w11}
				and channel_code in('1','9')
				and dc_code not in('W0K4','W0Z7')
				and business_type_code='1'
			group by 
				customer_no,
				b.classify_middle_code,
				b.classify_middle_name
			)b on a.customer_no=b.customer_no
	)a
;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table csx_tmp.tmp_r_sales_middle_unusual partition(sdt)
select
	province_code
	,province_name
	,city_group_code
	,city_group_name
	,customer_no
	,customer_name
	,kehu_sales_value
	,huanbi_sales_value
	,profitlv
	,huanbi_profitlv
	,classify_middle_code
	,classify_middle_name
	,sales_value
	,profitlv_pinlei
	,pl_huanbi_profitlv
	,huanbi_cost_price
	,huanbi_sales_price
	,${hiveconf:i_sdate_w11}
from 
	csx_tmp.tmp_r_sales_top_customer_middle01
where 
	(profitlv<=0.05 or huanbi_profitlv<0)
	AND (profitlv_pinlei<=0.05 or  pl_huanbi_profitlv<0);

insert overwrite table csx_tmp.tmp_r_sales_topgoods_unusual partition(sdt)
select
     a.province_code
    ,a.province_name
    ,a.city_group_code
    ,a.city_group_name
    ,a.customer_no
    ,a.customer_name
    ,b.goods_code
    ,b.goods_name
    ,b.classify_middle_code
    ,b.classify_middle_name
	,b.classify_small_code
	,b.classify_small_name
    ,b.sales_value
    ,b.profitlv
    ,b.huanbi_profitlv
	,b.fact_price
	,b.huanbi_fact_price
	,b.cost_price
    ,b.huanbi_cost_price
	,b.sales_price 
    ,b.huanbi_sales_price
	,${hiveconf:i_sdate_w11}
from(
select 
  province_code
 ,province_name
 ,city_group_code
 ,city_group_name
 ,customer_no
 ,customer_name
from csx_tmp.tmp_r_sales_top_customer_middle01
where (profitlv<=0.05 or huanbi_profitlv<0)
group by province_code
 ,province_name
 ,city_group_code
 ,city_group_name
 ,customer_no
 ,customer_name
  )a
join 
  (select *,
     row_number() over (partition by customer_no order by sales_value desc) as top_customer_goods_no
  from (select
     customer_no
    ,goods_code
    ,goods_name
    ,classify_middle_code
    ,classify_middle_name
	,classify_small_code
	,classify_small_name
    ,sales_value
    ,round(profit/abs(sales_value),6) profitlv
    ,round(profit/abs(sales_value),6)-round(zri_profit/abs(zri_sales_value),6) huanbi_profitlv
	,round(fact_price/sales_qty,6)  fact_price
	,round((fact_price/sales_qty)/(zri_fact_price/zri_sales_qty)-1,6) huanbi_fact_price
	,round((sales_cost/sales_qty),6) cost_price
    ,round((sales_cost/sales_qty)/(zri_sales_cost/zri_sales_qty)-1,6) huanbi_cost_price
	,round((sales_value/sales_qty),6) sales_price 
    ,round((sales_value/sales_qty)/(zri_sales_value/zri_sales_qty)-1,6) huanbi_sales_price
from (
   select
    a.customer_no,
	a.goods_code,
	b.goods_name,
	b.classify_middle_code,
    b.classify_middle_name,
	b.classify_small_code,
	b.classify_small_name,
    sum(if(sdt=${hiveconf:i_sdate_w11},sales_value,0)) sales_value,
	sum(if(sdt=${hiveconf:i_sdate_w11},profit,0)) profit,
    sum(if(sdt=${hiveconf:i_sdate_w12},sales_value,0))  zri_sales_value,
	sum(if(sdt=${hiveconf:i_sdate_w12},profit,0)) zri_profit,
	sum(if(sdt=${hiveconf:i_sdate_w11},sales_qty,0)) sales_qty,
	sum(if(sdt=${hiveconf:i_sdate_w11},sales_cost,0)) sales_cost,
	sum(if(sdt=${hiveconf:i_sdate_w12},sales_qty,0))  zri_sales_qty,
	sum(if(sdt=${hiveconf:i_sdate_w12},sales_cost,0)) zri_sales_cost,
	sum(if(sdt=${hiveconf:i_sdate_w11},fact_price*sales_qty,0)) fact_price,
	sum(if(sdt=${hiveconf:i_sdate_w12},fact_price*sales_qty,0))  zri_fact_price
  from csx_dw.dws_sale_r_d_detail a
  left join 
	(select
	  t2.goods_code,
	  t2.credential_no,
	  round(sum(t3.fact_price*t2.qty)/sum(case when t3.fact_price is not null then t2.qty end),6) fact_price --原料入库价
	from 
	(
	  select
	  	goods_code,
	  	credential_no,
	  	source_order_no,
		max(wms_order_time) wms_order_time,
		sum(amt) amt,
	  	sum(qty) as qty
	  from csx_dw.dws_wms_r_d_batch_detail
	  where  sdt >=${hiveconf:i_sdate_w22} and sdt<=${hiveconf:i_sdate_w11}	
	  and move_type in ('107A', '108A')
	  group by goods_code, credential_no, source_order_no
    )t2 
    left outer join 
    (
	
      select 
      	goods_code,
      	order_code,
		sdt,
        round(sum(fact_values)/sum(goods_reality_receive_qty),6) as fact_price
      from csx_dw.dws_mms_r_a_factory_order
      where sdt >=${hiveconf:i_sdate_w22} and sdt<=${hiveconf:i_sdate_w11} and mrp_prop_key in('3061','3010')
      group by goods_code, order_code,sdt
    )t3 on t2.source_order_no = t3.order_code and t2.goods_code = t3.goods_code				  
	group by t2.goods_code,
	  t2.credential_no
		)c on split(a.id,'&')[0]=c.credential_no and a.goods_code=c.goods_code
  left join 
  (
   select
      goods_id,
	  goods_name,
      classify_middle_code,
      classify_middle_name,
	  classify_small_code,
	  classify_small_name
  from  csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current'
  ) b 
  on b.goods_id = a.goods_code
  where sdt >=${hiveconf:i_sdate_w12} and sdt <=${hiveconf:i_sdate_w11}
  and channel_code in('1','9')
   and dc_code not in('W0K4','W0Z7')
  and business_type_code='1'
 group by a.customer_no,
	a.goods_code,
	b.goods_name,
	b.classify_middle_code,
    b.classify_middle_name,
	b.classify_small_code,
	b.classify_small_name)a
where (round(profit/abs(sales_value),6)<=0.05 or round(profit/abs(sales_value),6)-round(zri_profit/abs(zri_sales_value),6)<0)
  )b)b on a.customer_no=b.customer_no
where b.top_customer_goods_no<=50;
 /*  INVALIDATE METADATA csx_tmp.tmp_r_sales_middle_unusual; 

create table csx_tmp.tmp_r_sales_middle_unusual(
  `province_code` string comment '省区编号',
  `province_name` string comment '省区名称',
  `city_group_code` string comment '城市组编号',
  `city_group_name` string comment '城市组名称',
  `customer_no` string comment '客户编码', 
  `customer_name` string comment '客户名称', 
  `kehu_sales_value` decimal(19,6) comment '本期客户销售额',
  `huanbi_sales_value` decimal(19,6) comment '本期客户销售额环比',  
  `profitlv` decimal(19,6) comment '本期客户利率', 
  `huanbi_profitlv` decimal(19,6) comment '本期客户毛利率环比',    
  `classify_middle_code` string comment '管理中类编号',
  `classify_middle_name` string comment '管理中类名称',
  `sales_value` decimal(19,6) comment '品类销售额',
  `profitlv_pinlei` decimal(19,6) comment '品类毛利率',
  `pl_huanbi_profitlv` decimal(19,6) comment '品类毛利率环比',
  `huanbi_cost_price` decimal(19,6) comment '成本价环比',
  `huanbi_sales_price` decimal(19,6) comment '售价环比'
) comment '客户管理品类-异常汇总'
partitioned by (sdt string comment '日分区')
stored as textfile;

create table csx_tmp.tmp_r_sales_topgoods_unusual(
  `province_code` string comment '省区编号',
  `province_name` string comment '省区名称',
  `city_group_code` string comment '城市组编号',
  `city_group_name` string comment '城市组名称',
  `customer_no` string comment '客户编码', 
  `customer_name` string comment '客户名称', 
  `goods_code` string comment '商品编码', 
  `goods_name` string comment '商品名称', 
  `classify_middle_code` string comment '管理中类编号',
  `classify_middle_name` string comment '管理中类名称',
  `classify_small_code` string comment '管理小类编号',
  `classify_small_name` string comment '管理小类名称',  
  `sales_value` decimal(19,6) comment '销售额',
  `profitlv` decimal(19,6) comment '本期毛利率',  
  `huanbi_profitlv` decimal(19,6) comment '毛利率环比', 
  `fact_price` decimal(19,6) comment '原料价',  
  `huanbi_fact_price` decimal(19,6) comment '原料价环比',
  `cost_price` decimal(19,6) comment '成本价',
  `huanbi_cost_price` decimal(19,6) comment '成本价环比',
  `sales_price` decimal(19,6) comment '售价',
  `huanbi_sales_price` decimal(19,6) comment '售价环比'
) comment '客户商品-异常明细'
partitioned by (sdt string comment '日分区')
stored as textfile; */
 