-- 午餐会业务类型AB客户环比情况
select *
from 
(
select 
	a.business_type_name,
	performance_region_name,
	performance_province_name,
	performance_city_name,
	a2.extra as direct_delivery_large_type,
	(case when a.inventory_dc_code in ('W0J2','W0AJ','W0G6','WB71','WC65','WD38','WD53') then '是' else '否' end) as `是否是监狱海军`,
	case 
	when order_channel_code=6 then '调价单'
	when order_channel_code=4 then '返利单'
	when refund_order_flag=1 then '退货单'
	when delivery_type_code=2 then '直送单'
	end as yc_flag,	
	customer_large_level,
  	customer_small_level,
	customer_code,
	customer_name,
	second_category_name,     --  二级客户分类名称
	sum(case when a.sdt between regexp_replace(trunc('${i_sdate}','MM'),'-','') and regexp_replace(add_months('${i_sdate}',0),'-','') then sale_amt else 0 end) as bq_sale_amt,
	sum(case when a.sdt between regexp_replace(trunc('${i_sdate}','MM'),'-','') and regexp_replace(add_months('${i_sdate}',0),'-','') then profit else 0 end) as bq_profit,
	sum(case when a.sdt between regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and if('${i_sdate}'=last_day('${i_sdate}'),regexp_replace(last_day(add_months('${i_sdate}',-1)),'-',''),regexp_replace(add_months('${i_sdate}',-1),'-','')) then sale_amt else 0 end) as sq_sale_amt,
	sum(case when a.sdt between regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and if('${i_sdate}'=last_day('${i_sdate}'),regexp_replace(last_day(add_months('${i_sdate}',-1)),'-',''),regexp_replace(add_months('${i_sdate}',-1),'-','')) then profit else 0 end) as sq_profit,

	-- sum(case when a.sdt between regexp_replace(add_months(trunc('${i_sdate}','MM'),-12),'-','') and regexp_replace(add_months('${i_sdate}',-12),'-','') then sale_amt else 0 end) as tq_sale_amt,
	-- sum(case when a.sdt between regexp_replace(add_months(trunc('${i_sdate}','MM'),-12),'-','') and regexp_replace(add_months('${i_sdate}',-12),'-','') then profit else 0 end) as tq_profit,

	-- 固定近4周
	sum(case when a.week=weekofyear(date_sub('${sdt_yes_date}',3+21)) then a.sale_amt end) w1_sales_value,
	sum(case when a.week=weekofyear(date_sub('${sdt_yes_date}',3+21)) then a.profit end  ) w1_profit,

	sum(case when a.week=weekofyear(date_sub('${sdt_yes_date}',3+14)) then a.sale_amt else 0 end) w2_sales_value,
	sum(case when a.week=weekofyear(date_sub('${sdt_yes_date}',3+14)) then a.profit else 0 end) w2_profit,

	sum(case when a.week=weekofyear(date_sub('${sdt_yes_date}',3+7)) then a.sale_amt else 0 end) w3_sales_value,
	sum(case when a.week=weekofyear(date_sub('${sdt_yes_date}',3+7)) then a.profit else 0 end) w3_profit,

	sum(case when a.week=weekofyear(date_sub('${sdt_yes_date}',3)) then a.sale_amt else 0 end) w4_sales_value,
	sum(case when a.week=weekofyear(date_sub('${sdt_yes_date}',3)) then a.profit else 0 end) w4_profit
from
  ( 
  select 
  	inventory_dc_code,
  	a.customer_code,
  	b.customer_name,
  	delivery_type_name,
  	direct_delivery_type,
	order_channel_code,refund_order_flag,
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
  	sale_amt,
  	profit,
  	customer_large_level,
  	customer_small_level
  from csx_dws.csx_dws_sale_detail_di  a 
  left  join 
  (select customer_no,customer_name,customer_large_level,customer_small_level from csx_analyse.csx_analyse_report_sale_customer_level_mf 
    where month='202506' 
    and customer_large_level in ('A','B')
    and tag=1) b on a.customer_code=b.customer_no
  where sdt>=regexp_replace(add_months(trunc('${i_sdate}','MM'),-2),'-','')
  -- or sdt between regexp_replace(add_months(trunc('${i_sdate}','MM'),-12),'-','') and regexp_replace(add_months('${i_sdate}',-12),'-',''))
  and performance_province_name not like '平台%'
  and coalesce(channel_code,'0') not in('2')
  and business_type_code in('1')
  and second_category_name<>'教育'
  and shipper_code='YHCSX'
  )a
-- 直送类型 详细履约模式的码表
left join 
  (
  select `code`,name,extra
  from csx_dim.csx_dim_basic_topic_dict_df
  where parent_code = 'direct_delivery_type'
  )a2 on cast(a.direct_delivery_type as string)=a2.`code`	
group by 
	a.business_type_name,
	performance_region_name,
	performance_province_name,
	performance_city_name,
	a2.extra,
	(case when a.inventory_dc_code in ('W0J2','W0AJ','W0G6','WB71','WC65','WD38','WD53') then '是' else '否' end),
	case 
	when order_channel_code=6 then '调价单'
	when order_channel_code=4 then '返利单'
	when refund_order_flag=1 then '退货单'
	when delivery_type_code=2 then '直送单'
	end,	
	customer_large_level,
  	customer_small_level,
	customer_code,
	customer_name,
	second_category_name     --  二级客户分类名称	
)a 
;