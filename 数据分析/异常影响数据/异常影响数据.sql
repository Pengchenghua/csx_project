-- 周报-异常
-- #################################################################################
-- 日配
-- 日配整体数据  -- XX剔除直送仓、剔除监狱海军仓 
-- 用途：求异常影响需要用到这部分数据

select 
	a. performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sale_amt end) by_sale_amt,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.profit end) by_profit,	
	
	sum(case when a.sdt between regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and if('${i_sdate}'=last_day('${i_sdate}'),regexp_replace(last_day(add_months('${i_sdate}',-1)),'-',''),regexp_replace(add_months('${i_sdate}',-1),'-','')) then a.sale_amt end) sy_sale_amt,
	sum(case when a.sdt between regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and if('${i_sdate}'=last_day('${i_sdate}'),regexp_replace(last_day(add_months('${i_sdate}',-1)),'-',''),regexp_replace(add_months('${i_sdate}',-1),'-','')) then a.profit end) sy_profit,	
	
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
	where sdt >=regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') 
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
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sale_amt end) by_sale_amt,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.profit end) by_profit,	
	
	sum(case when a.sdt between regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and if('${i_sdate}'=last_day('${i_sdate}'),regexp_replace(last_day(add_months('${i_sdate}',-1)),'-',''),regexp_replace(add_months('${i_sdate}',-1),'-','')) then a.sale_amt end) sy_sale_amt,
	sum(case when a.sdt between regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and if('${i_sdate}'=last_day('${i_sdate}'),regexp_replace(last_day(add_months('${i_sdate}',-1)),'-',''),regexp_replace(add_months('${i_sdate}',-1),'-','')) then a.profit end) sy_profit,	
	
	sum(case when a.week=weekofyear(date_sub(current_date, 3)) then a.sale_amt end) bq_sale_amt,
	sum(case when a.week=weekofyear(date_sub(current_date, 3)) then a.profit end) bq_profit,
	
	sum(case when a.week=weekofyear(date_sub(current_date, 10)) then a.sale_amt end) sq_sale_amt,
	sum(case when a.week=weekofyear(date_sub(current_date, 10)) then a.profit end) sq_profit
from csx_analyse.csx_analyse_sale_order_detail_abnormal a
where sdt >=regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') 
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


