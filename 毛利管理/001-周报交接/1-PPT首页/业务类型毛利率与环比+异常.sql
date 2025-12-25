---------------------------- 表1：业务类型毛利率环比
-- B+BBC里各业务
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
	second_category_name,     --  二级客户分类名称
	sum(case when a.sdt between regexp_replace(trunc('${sdt_yes_date}','MM'),'-','') and regexp_replace(add_months('${sdt_yes_date}',0),'-','') then sale_amt else 0 end) as bq_sale_amt,
	sum(case when a.sdt between regexp_replace(trunc('${sdt_yes_date}','MM'),'-','') and regexp_replace(add_months('${sdt_yes_date}',0),'-','') then profit else 0 end) as bq_profit,
	sum(case when a.sdt between regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') and if('${sdt_yes_date}'=last_day('${sdt_yes_date}'),regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),regexp_replace(add_months('${sdt_yes_date}',-1),'-','')) then sale_amt else 0 end) as sq_sale_amt,
	sum(case when a.sdt between regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') and if('${sdt_yes_date}'=last_day('${sdt_yes_date}'),regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),regexp_replace(add_months('${sdt_yes_date}',-1),'-','')) then profit else 0 end) as sq_profit,

	-- sum(case when a.sdt between regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-12),'-','') and regexp_replace(add_months('${sdt_yes_date}',-12),'-','') then sale_amt else 0 end) as tq_sale_amt,
	-- sum(case when a.sdt between regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-12),'-','') and regexp_replace(add_months('${sdt_yes_date}',-12),'-','') then profit else 0 end) as tq_profit,

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
  where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-2),'-','')
  -- or sdt between regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-12),'-','') and regexp_replace(add_months('${sdt_yes_date}',-12),'-',''))
  and performance_province_name not like '平台%'
  and coalesce(channel_code,'0') not in('2')
  -- and business_type_code in('1')
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
	second_category_name     --  二级客户分类名称	
)a 
;








-- #################################################################################
---------------------------- 表2：异常相关
-- 日配整体
select 
	a. performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	sum(case when a.sdt >= regexp_replace(trunc('${sdt_yes_date}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${sdt_yes_date}',0),'-','') then a.sale_amt end) by_sale_amt,
	sum(case when a.sdt >= regexp_replace(trunc('${sdt_yes_date}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${sdt_yes_date}',0),'-','') then a.profit end) by_profit,	
	
	sum(case when a.sdt between regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') and if('${sdt_yes_date}'=last_day('${sdt_yes_date}'),regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),regexp_replace(add_months('${sdt_yes_date}',-1),'-','')) then a.sale_amt end) sy_sale_amt,
	sum(case when a.sdt between regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') and if('${sdt_yes_date}'=last_day('${sdt_yes_date}'),regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),regexp_replace(add_months('${sdt_yes_date}',-1),'-','')) then a.profit end) sy_profit,	
	
	sum(case when a.week=weekofyear(date_sub(current_date, 3)) then a.sale_amt end) bz_sale_amt,
	sum(case when a.week=weekofyear(date_sub(current_date, 3)) then a.profit end) bz_profit,
	
	sum(case when a.week=weekofyear(date_sub(current_date, 10)) then a.sale_amt end) sz_sale_amt,
	sum(case when a.week=weekofyear(date_sub(current_date, 10)) then a.profit end) sz_profit		
from 
(
	select * ,
		substr(sdt,1,6) smonth,
		weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)) week,
		if((order_channel_code in ('4','5','6') or refund_order_flag=1),original_order_code,order_code) as order_code_new	
	from csx_dws.csx_dws_sale_detail_di 
	where sdt >=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') 
	and sdt <= regexp_replace(add_months(date_sub(current_date,1),0),'-','') 
	and business_type_code in ('1') 
	-- and (delivery_type_code=2 -- '直送单'
	-- or order_channel_code=6 -- '调价单'
	-- or order_channel_code=4 -- '返利单'
	-- or refund_order_flag=1 -- '退货单'
	-- )
	-- and inventory_dc_code not in  ('W0J2','W0AJ','W0G6','WB71','WC65','WD38','WD53')
	and shipper_code='YHCSX'
) a 
group by 
	a. performance_region_name,
	a.performance_province_name,
	a.performance_city_name;
	
-- 异常	
select *
from 
(
select 
	a. performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	customer_code,
	customer_name,	
	yc_flag,
    new_direct_delivery_type, -- 新直送类型		
	direct_delivery_large_type, -- 新直送大类	
	-- 调价
	adjust_reason,
	-- 退补
	refund_reason,
	first_level_reason_name,
	second_level_reason_name,	
	sum(case when a.sdt >= regexp_replace(trunc('${sdt_yes_date}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${sdt_yes_date}',0),'-','') then a.sale_amt end) by_sale_amt,
	sum(case when a.sdt >= regexp_replace(trunc('${sdt_yes_date}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${sdt_yes_date}',0),'-','') then a.profit end) by_profit,	
	
	sum(case when a.sdt between regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') and if('${sdt_yes_date}'=last_day('${sdt_yes_date}'),regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),regexp_replace(add_months('${sdt_yes_date}',-1),'-','')) then a.sale_amt end) sy_sale_amt,
	sum(case when a.sdt between regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') and if('${sdt_yes_date}'=last_day('${sdt_yes_date}'),regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),regexp_replace(add_months('${sdt_yes_date}',-1),'-','')) then a.profit end) sy_profit,	
	
	sum(case when a.week=weekofyear(date_sub(current_date, 3)) then a.sale_amt end) bq_sale_amt,
	sum(case when a.week=weekofyear(date_sub(current_date, 3)) then a.profit end) bq_profit,
	
	sum(case when a.week=weekofyear(date_sub(current_date, 10)) then a.sale_amt end) sq_sale_amt,
	sum(case when a.week=weekofyear(date_sub(current_date, 10)) then a.profit end) sq_profit
from csx_analyse.csx_analyse_sale_order_detail_abnormal a
where sdt >=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') 
	and sdt <= regexp_replace(add_months(date_sub(current_date,1),0),'-','')
-- and inventory_dc_code not in  ('W0J2','W0AJ','W0G6','WB71','WC65','WD38','WD53')	
and yc_flag<>''
group by 
	a. performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	customer_code,
	customer_name,	
	yc_flag,
    new_direct_delivery_type, -- 新直送类型		
	direct_delivery_large_type, -- 新直送大类	
	-- 调价
	adjust_reason,
	-- 退补
	refund_reason,
	first_level_reason_name,
	second_level_reason_name
)a 
-- where (bq_sale_amt is not null or sq_sale_amt is not null)
;



