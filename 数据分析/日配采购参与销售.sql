
-- 客户日配采购参与毛利率
select 
	business_type_name,
	performance_region_name,
	performance_province_name,
	performance_city_name,
	customer_code,
	customer_name,     --  客户名称
	second_category_name,     --  二级客户分类名称
	first_sale_date,
	bq_sale_amt,
	bq_profit,
	bq_profit/abs(bq_sale_amt) as bq_profit_rate,
	sq_sale_amt,
	sq_profit,
	sq_profit/abs(sq_sale_amt) as sq_profit_rate,
	
	bq_profit/abs(bq_sale_amt)-sq_profit/abs(sq_sale_amt) as hb_profit_rate,
	bq_sale_amt-sq_sale_amt as hb_sale_amt,
	
	

	-- 固定近4周
	w1_sale_amt,
	w1_profit,
	w1_profit/abs(w1_sale_amt) as w1_profit_rate,

	w2_sale_amt,
	w2_profit,
	w2_profit/abs(w2_sale_amt) as w2_profit_rate,

	w3_sale_amt,
	w3_profit,
	w3_profit/abs(w3_sale_amt) as w3_profit_rate,

	w4_sale_amt,
	w4_profit,
	w4_profit/abs(w4_sale_amt) as w4_profit_rate,
	w4_profit/abs(w4_sale_amt)-w3_profit/abs(w3_sale_amt) as w4_profit_rate_hb
from 
(
select 
	a.business_type_name,
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	a.customer_code,
	b.customer_name,     --  客户名称
	b.second_category_name,     --  二级客户分类名称
	c.first_sale_date,
	sum(case when a.sdt between regexp_replace(trunc('${i_sdate}','MM'),'-','') and regexp_replace(add_months('${i_sdate}',0),'-','') then sale_amt else 0 end) as bq_sale_amt,
	sum(case when a.sdt between regexp_replace(trunc('${i_sdate}','MM'),'-','') and regexp_replace(add_months('${i_sdate}',0),'-','') then profit else 0 end) as bq_profit,
	sum(case when a.sdt between regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and if('${i_sdate}'=last_day('${i_sdate}'),regexp_replace(last_day(add_months('${i_sdate}',-1)),'-',''),regexp_replace(add_months('${i_sdate}',-1),'-','')) then sale_amt else 0 end) as sq_sale_amt,
	sum(case when a.sdt between regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and if('${i_sdate}'=last_day('${i_sdate}'),regexp_replace(last_day(add_months('${i_sdate}',-1)),'-',''),regexp_replace(add_months('${i_sdate}',-1),'-','')) then profit else 0 end) as sq_profit,

	-- sum(case when a.sdt between regexp_replace(add_months(trunc('${i_sdate}','MM'),-12),'-','') and regexp_replace(add_months('${i_sdate}',-12),'-','') then sale_amt else 0 end) as tq_sale_amt,
	-- sum(case when a.sdt between regexp_replace(add_months(trunc('${i_sdate}','MM'),-12),'-','') and regexp_replace(add_months('${i_sdate}',-12),'-','') then profit else 0 end) as tq_profit,

	-- 固定近4周
	sum(case when a.week=weekofyear(date_sub('${sdt_yes_date}',3+21)) then a.sale_amt end) w1_sale_amt,
	sum(case when a.week=weekofyear(date_sub('${sdt_yes_date}',3+21)) then a.profit end  ) w1_profit,

	sum(case when a.week=weekofyear(date_sub('${sdt_yes_date}',3+14)) then a.sale_amt else 0 end) w2_sale_amt,
	sum(case when a.week=weekofyear(date_sub('${sdt_yes_date}',3+14)) then a.profit else 0 end) w2_profit,

	sum(case when a.week=weekofyear(date_sub('${sdt_yes_date}',3+7)) then a.sale_amt else 0 end) w3_sale_amt,
	sum(case when a.week=weekofyear(date_sub('${sdt_yes_date}',3+7)) then a.profit else 0 end) w3_profit,

	sum(case when a.week=weekofyear(date_sub('${sdt_yes_date}',3)) then a.sale_amt else 0 end) w4_sale_amt,
	sum(case when a.week=weekofyear(date_sub('${sdt_yes_date}',3)) then a.profit else 0 end) w4_profit
from
  ( 
  select 
  	inventory_dc_code,customer_code,delivery_type_name,direct_delivery_type,
	order_channel_code,refund_order_flag,delivery_type_code,second_category_name,
  	case when channel_code='2' then '商超' else 'B+BBC' end as channel_name,
  	performance_region_name,
  	performance_province_name,
  	-- case when performance_city_name in('上海松江','上海宝山','江苏苏州') then performance_city_name else '-' end as performance_city_name,
  	performance_city_name,	
  	weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)) week,	-- 周六到周五	
  	substr(sdt,1,6) smonth,sdt,
  	business_type_code,business_type_name,
  	sale_amt,
  	profit
  from csx_dws.csx_dws_sale_detail_di
  where sdt>=regexp_replace(add_months(trunc('${i_sdate}','MM'),-2),'-','')
  -- or sdt between regexp_replace(add_months(trunc('${i_sdate}','MM'),-12),'-','') and regexp_replace(add_months('${i_sdate}',-12),'-',''))
  and performance_province_name not like '平台%'
  -- and coalesce(channel_code,'0') not in('2')
  and business_type_code in('1')
  and shipper_code='YHCSX'
  )a
-- 直送类型 详细履约模式的码表
join 
  (
  select `code`,name,extra
  from csx_dim.csx_dim_basic_topic_dict_df
  where parent_code = 'direct_delivery_type'
  and extra='采购参与'
  )a2 on cast(a.direct_delivery_type as string)=a2.`code`
left join 
(
select dev_source_name,
	performance_region_name,     --  销售大区名称(业绩划分)
	performance_province_name,     --  销售归属省区名称
	performance_city_name,     --  城市组名称(业绩划分)
	customer_code,
	customer_name,     --  客户名称
	-- first_category_code,     --  一级客户分类编码
	first_category_name,     --  一级客户分类名称
	-- second_category_code,     --  二级客户分类编码
	second_category_name,     --  二级客户分类名称
	-- third_category_code,     --  三级客户分类编码
	third_category_name     --  三级客户分类名称
from csx_dim.csx_dim_crm_customer_info
where sdt='current'
and shipper_code='YHCSX'
-- and customer_type_code=4
)b on a.customer_code=b.customer_code 
left join 
(
  select 
    customer_code,first_sale_date,last_sale_date,sale_total_amt
  from csx_dws.csx_dws_crm_customer_active_di
  where sdt ='current' 
  and shipper_code='YHCSX'
)c on c.customer_code=a.customer_code 
group by 
		a.business_type_name,
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	a.customer_code,
	b.customer_name,     --  客户名称
	b.second_category_name,     --  二级客户分类名称	
	c.first_sale_date
)a 
;


-- 
with tmp_sale_de as (
-- 客户日配采购参与毛利率
select 
	business_type_name,
	performance_region_name,
	performance_province_name,
	performance_city_name,
-- 	classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
-- 	customer_code,
-- 	customer_name,     --  客户名称
-- 	second_category_name,     --  二级客户分类名称
-- 	first_sale_date,
	bq_sale_amt,
	bq_profit,
	bq_profit/abs(bq_sale_amt) as bq_profit_rate,
	sq_sale_amt,
	sq_profit,
	sq_profit/abs(sq_sale_amt) as sq_profit_rate,
	
	bq_profit/abs(bq_sale_amt)-sq_profit/abs(sq_sale_amt) as hb_profit_rate,
	bq_sale_amt-sq_sale_amt as hb_sale_amt,
	
    --省区品类毛利率
    sum(bq_profit)over(partition by classify_middle_name)/sum(bq_sale_amt)over(partition by classify_middle_name) as area_profit_rate,
     --省区品类毛利率
    sum(sq_profit)over(partition by classify_middle_name)/sum(sq_sale_amt)over(partition by classify_middle_name) as sq_area_profit_rate
from 
(
select 
	a.business_type_name,
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
-- 	classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
-- 	a.customer_code,
-- 	b.customer_name,     --  客户名称
-- 	b.second_category_name,     --  二级客户分类名称
-- 	c.first_sale_date,
	sum(case when a.sdt between regexp_replace(trunc('${i_sdate}','MM'),'-','') and regexp_replace(add_months('${i_sdate}',0),'-','') then sale_amt else 0 end) as bq_sale_amt,
	sum(case when a.sdt between regexp_replace(trunc('${i_sdate}','MM'),'-','') and regexp_replace(add_months('${i_sdate}',0),'-','') then profit else 0 end) as bq_profit,
	sum(case when a.sdt between regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and if('${i_sdate}'=last_day('${i_sdate}'),regexp_replace(last_day(add_months('${i_sdate}',-1)),'-',''),regexp_replace(add_months('${i_sdate}',-1),'-','')) then sale_amt else 0 end) as sq_sale_amt,
	sum(case when a.sdt between regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and if('${i_sdate}'=last_day('${i_sdate}'),regexp_replace(last_day(add_months('${i_sdate}',-1)),'-',''),regexp_replace(add_months('${i_sdate}',-1),'-','')) then profit else 0 end) as sq_profit

from
  ( 
  select 
  	inventory_dc_code,
  	customer_code,
  	delivery_type_name,
  	direct_delivery_type,
	order_channel_code,
	refund_order_flag,
	delivery_type_code,
	second_category_name,
  	case when channel_code='2' then '商超' else 'B+BBC' end as channel_name,
  	performance_region_name,
  	performance_province_name,
  	-- case when performance_city_name in('上海松江','上海宝山','江苏苏州') then performance_city_name else '-' end as performance_city_name,
  	performance_city_name,	
  	weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)) week,	-- 周六到周五	
  	substr(sdt,1,6) smonth,sdt,
  	business_type_code,
  	business_type_name,
  	b.classify_large_code,
    b.classify_large_name,
    b.classify_middle_code,
    b.classify_middle_name,
  	sale_amt,
  	profit
  from csx_dws.csx_dws_sale_detail_di a 
  left join 
  (select goods_code,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name
  from csx_dim.csx_dim_basic_goods where sdt='current') b on a.goods_code=b.goods_code
  where sdt>=regexp_replace(add_months(trunc('${i_sdate}','MM'),-2),'-','')
  -- or sdt between regexp_replace(add_months(trunc('${i_sdate}','MM'),-12),'-','') and regexp_replace(add_months('${i_sdate}',-12),'-',''))
  and performance_province_name not like '平台%'
  -- and coalesce(channel_code,'0') not in('2')
  and business_type_code in('1')
  and shipper_code='YHCSX'
  )a
-- 直送类型 详细履约模式的码表
join 
  (
  select `code`,name,extra
  from csx_dim.csx_dim_basic_topic_dict_df
  where parent_code = 'direct_delivery_type'
  and extra='采购参与'
  )a2 on cast(a.direct_delivery_type as string)=a2.`code`
left join 
(
select dev_source_name,
	performance_region_name,     --  销售大区名称(业绩划分)
	performance_province_name,     --  销售归属省区名称
	performance_city_name,     --  城市组名称(业绩划分)
	customer_code,
	customer_name,     --  客户名称
	-- first_category_code,     --  一级客户分类编码
	first_category_name,     --  一级客户分类名称
	-- second_category_code,     --  二级客户分类编码
	second_category_name,     --  二级客户分类名称
	-- third_category_code,     --  三级客户分类编码
	third_category_name     --  三级客户分类名称
from csx_dim.csx_dim_crm_customer_info
where sdt='current'
and shipper_code='YHCSX'
-- and customer_type_code=4
)b on a.customer_code=b.customer_code 
left join 
(
  select 
    customer_code,first_sale_date,last_sale_date,sale_total_amt
  from csx_dws.csx_dws_crm_customer_active_di
  where sdt ='current' 
  and shipper_code='YHCSX'
)c on c.customer_code=a.customer_code 
group by 
    a.business_type_name,
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
-- 	classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name
)a 
)select * from tmp_sale_de

 select 
	classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    sum(bq_sale_amt) as bq_sale_amt,
	sum(bq_profit) bq_profit,
	sum(bq_profit)/sum(bq_sale_amt) bq_profit_rate,
	sum(sq_sale_amt)sq_sale_amt,
	sum(sq_profit)sq_profit,
	sum(sq_profit)/sum(sq_sale_amt) sq_profit_rate,
	sum(bq_sale_amt)-sum(sq_sale_amt) as hb_sale_amt,
    --省区品类毛利率
    concat_ws(',',collect_set(if(profit_type=1,concat(performance_province_name,'(',bq_profit_rate,')'),''))) a ,
    concat_ws(',',collect_set(if(profit_type=0,concat(performance_province_name,'(',bq_profit_rate,')'),''))) b 
from
 (select performance_region_name,
	performance_province_name,
	classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
	bq_sale_amt,
	bq_profit,
	bq_profit_rate,
	sq_sale_amt,
	sq_profit,
	sq_profit_rate,
	hb_profit_rate,
	hb_sale_amt,
    --省区品类毛利率
   area_profit_rate,
     --省区品类毛利率
    sq_area_profit_rate,
    case when bq_profit_rate<area_profit_rate then 1
        when bq_profit_rate>area_profit_rate then 2
        else 0 end profit_type
from  tmp_sale_de
) a 
group by classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name
;