--省区品类业绩与毛利跟踪报表、周维度报表；
--时间维度：周至今、环比上周周至今（其中周划分标准为上周六至本周五）；
--分区：周（202047）；

set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=2000;
set hive.groupby.skewindata=false;
set hive.map.aggr = true;
-- 增加reduce过程
set hive.optimize.sort.dynamic.partition=true;


--  本周六，昨日、上周六，上周昨日
--select ${hiveconf:i_sdate_w11},${hiveconf:i_sdate_w12},${hiveconf:i_sdate_w21},${hiveconf:i_sdate_w22},${hiveconf:i_sdate_dd};

set i_sdate_w11 =regexp_replace(DATE_SUB(date_sub(current_date,1),PMOD(DATEDIFF(date_sub(current_date,1),'2020-01-04'),7)),'-','');	------本周六
set i_sdate_w12 =regexp_replace(date_sub(current_date,1),'-','');	------昨日
set i_sdate_w21 =regexp_replace(DATE_SUB(date_sub(current_date,1),PMOD(DATEDIFF(date_sub(current_date,1),'2020-01-04'),7)+7),'-','');	 -----上周六
set i_sdate_w22 =regexp_replace(date_sub(current_date,1+7),'-','');	------上周昨日
set i_sdate_dd =from_utc_timestamp(current_timestamp(),'GMT');	------当前时间





--周维度
---------------------------------------------------------------------------------------------------------

--临时表2.1：各品类各省区的本期业绩、上期业绩、毛利率、省区间毛利率排名
drop table csx_tmp.tmp_w_sales_classify_middle_province_01;
create temporary table csx_tmp.tmp_w_sales_classify_middle_province_01
as
select 
  a.division,
  b.classify_middle_code,
  b.classify_middle_name, 		
  a.region_code,
  a.region_name,	
  a.province_code,    
  a.province_name, 
  --a.city_group_code,a.city_group_name,	
  sum(a.sales_value)/10000 sales_value,
  sum(a.profit)/10000 profit,
  sum(a.front_profit)/10000 front_profit,	
  sum(a.sq_sales_value)/10000 sq_sales_value,
  sum(a.sq_profit)/10000 sq_profit,
  sum(a.sq_front_profit)/10000 sq_front_profit	
from	
  (
  select 
    substr(sdt,1,6)smonth,
	region_code,
	region_name,
	channel_name,
	province_code,
	province_name,
	city_group_code,
	city_group_name,
	division_code,
	sdt,
    case when division_code in ('10','11') then '生鲜'
    	when division_code in ('12','13','15') then '食百'
    	else '' end as division,	
    customer_no,goods_code,
    sales_value,profit,front_profit,
    '' sq_sales_value,'' sq_profit,'' sq_front_profit
  from csx_dw.dws_sale_r_d_detail
  where sdt>= ${hiveconf:i_sdate_w11} 	--本周六
  and sdt<= ${hiveconf:i_sdate_w12} 	--昨日			
  and channel_code in('1','9')
  --仅包含业务类型为日配业务、福利业务数据
  and business_type_code in('1','2')
  and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
  				'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
  union all
  select 
    substr(sdt,1,6)smonth,
	region_code,
	region_name,
	channel_name,
	province_code,
	province_name,
	city_group_code,
	city_group_name,
	division_code,
	sdt,
    case when division_code in ('10','11') then '生鲜'
    	when division_code in ('12','13','15') then '食百'
    	else '' end as division,	
    customer_no,goods_code,
    '' sales_value,'' profit,'' front_profit,
    sales_value as sq_sales_value,profit as sq_profit,front_profit as sq_front_profit
  from csx_dw.dws_sale_r_d_detail
  where sdt>= ${hiveconf:i_sdate_w21} 	--上周六
  and sdt<= ${hiveconf:i_sdate_w22} 	--上周昨日			
  and channel_code in('1','9')
  --仅包含业务类型为日配业务、福利业务数据
  and business_type_code in('1','2')
  and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
  				'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
  )a
left join (select *  from  csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current') b on b.goods_id = a.goods_code	
group by 
  a.division,
  b.classify_middle_code,
  b.classify_middle_name, 
  a.region_code,
  a.region_name,	
  a.province_code,
  a.province_name
  --a.city_group_code,a.city_group_name
;

--结果表2：周度省区品类业绩毛利 csx_dw.report_sale_r_w_classify_middle
insert overwrite table csx_dw.report_sale_r_w_classify_middle  partition(week)
select 
  concat_ws('-',concat(substr(${hiveconf:i_sdate_w12},1,4),lpad(weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(${hiveconf:i_sdate_w12},'yyyyMMdd'))),-2)),2,'0')),
  			a.classify_middle_code,a.province_code) as biz_id,
  a.division,
  a.classify_middle_code,
  a.classify_middle_name,
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  c.total_sales_value,
  c.total_profit,
  c.total_prorate,
  a.rank_classify_sales,
  b.province_sales_value,
  a.rank_province_prorate,
  a.prorate_best,
  a.sales_value,
  a.profit,
  a.prorate,
  a.front_profit,
  a.middle_profit,
  a.sq_sales_value,
  a.sq_profit,
  a.sq_prorate,
  a.sq_front_profit,
  a.sq_middle_profit,
  concat_ws('-',${hiveconf:i_sdate_w11},${hiveconf:i_sdate_w12}) time_slot_bq,
  concat_ws('-',${hiveconf:i_sdate_w21},${hiveconf:i_sdate_w22})time_slot_sq,
  'raoyanhua' create_by,
  ${hiveconf:i_sdate_dd} create_time,
  ${hiveconf:i_sdate_dd} update_time,
  concat(substr(${hiveconf:i_sdate_w12},1,4),lpad(weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(${hiveconf:i_sdate_w12},'yyyyMMdd'))),-2)),2,'0')) week
from 
  (
  select 
    division,
    classify_middle_code,
    classify_middle_name,
    region_code,
	region_name,
	province_code,
	province_name,
    sum(sales_value) sales_value,
    sum(profit) profit,
    sum(profit)/abs(sum(sales_value)) prorate,
    sum(front_profit) front_profit,
    sum(profit)-sum(front_profit) as middle_profit,
    sum(sq_sales_value) sq_sales_value,
    sum(sq_profit) sq_profit,
    sum(sq_profit)/abs(sum(sq_sales_value)) sq_prorate,
    sum(sq_front_profit) sq_front_profit,
    sum(sq_profit)-sum(sq_front_profit) as sq_middle_profit,
    rank() over(partition by classify_middle_name order by round(sum(profit)/abs(sum(sales_value)),6) desc) rank_province_prorate,	 --本期省区毛利率排名	
    max(sum(profit)/abs(sum(sales_value))) over (partition by classify_middle_name) as prorate_best,	--最佳省区毛利率
    rank() over(partition by province_code order by sum(sales_value) desc) rank_classify_sales		--品类全国销售额排名
  from csx_tmp.tmp_w_sales_classify_middle_province_01
  group by division,classify_middle_code,classify_middle_name,region_code,region_name,province_code,province_name
  union all
  select 
    coalesce(division,'总计') as division,
    '合计' classify_middle_code,
    '合计' classify_middle_name,
    region_code,region_name,province_code,province_name,
    sum(sales_value) sales_value,
    sum(profit) profit,
    sum(profit)/abs(sum(sales_value)) prorate,
    sum(front_profit) front_profit,
    sum(profit)-sum(front_profit) as middle_profit,
    sum(sq_sales_value) sq_sales_value,
    sum(sq_profit) sq_profit,
    sum(sq_profit)/abs(sum(sq_sales_value)) sq_prorate,
    sum(sq_front_profit) sq_front_profit,
    sum(sq_profit)-sum(sq_front_profit) as sq_middle_profit,
    '' rank_province_prorate,	 --本期省区毛利率排名	
    '' prorate_best,	--最佳省区毛利率
    '' rank_classify_sales		--品类全国销售额排名
  from csx_tmp.tmp_w_sales_classify_middle_province_01
  group by division,region_code,region_name,province_code,province_name
  grouping sets((division,region_code,region_name,province_code,province_name),
  		(region_code,region_name,province_code,province_name))		
  )a
left join 
  (
  select 
    province_code,
    sum(sales_value) province_sales_value,
    sum(profit) province_profit
  from csx_tmp.tmp_w_sales_classify_middle_province_01
  group by province_code
  )b on b.province_code=a.province_code
left join 
  (
  select 
    classify_middle_code,
    sum(sales_value) total_sales_value,
    sum(profit) total_profit,
    sum(profit)/abs(sum(sales_value)) total_prorate	
  from csx_tmp.tmp_w_sales_classify_middle_province_01
  group by classify_middle_code
  )c on c.classify_middle_code=a.classify_middle_code
;
