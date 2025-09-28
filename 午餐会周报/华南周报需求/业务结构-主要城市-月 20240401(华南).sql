
-- 业务结构
with 
 province_business_sale as 
(
select 
	-- weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)) week,
	business_type_name,
	coalesce(performance_region_name,'全国') as performance_region_name,
	coalesce(performance_province_name,'全国') as performance_province_name,
	coalesce(performance_city_name,'全国') as performance_city_name,
	sum(case when a.smonth=substr(regexp_replace('${i_sdate}','-',''),1,6) then sale_amt else 0 end)/10000 as bz_sale_amt,
	sum(case when a.smonth=substr(regexp_replace('${i_sdate}','-',''),1,6) then profit else 0 end)/10000 as bz_profit,
	sum(case when a.smonth=substr(regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-',''),1,6) and sdt<=regexp_replace(add_months('${i_sdate}',-1),'-','') then sale_amt else 0 end)/10000 as sz_sale_amt,
	sum(case when a.smonth=substr(regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-',''),1,6) and sdt<=regexp_replace(add_months('${i_sdate}',-1),'-','') then profit else 0 end)/10000 as sz_profit
from
( 
select 
	case when channel_code='2' then '商超' else 'B+BBC' end as channel_name,
    performance_region_name,
	performance_province_name,
	performance_city_name,
	substr(sdt,1,6)smonth,
	sdt,
	weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)) week,
	business_type_name,
	sale_amt,
	profit
from csx_dws.csx_dws_sale_detail_di
where sdt>=regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','')
and sdt<=regexp_replace('${i_sdate}','-','')
-- and sdt <='20231208'
and performance_province_name not like '平台%'
and coalesce(channel_code,'0') not in('2')
and performance_region_name='华南大区'
)a
group by 
	-- weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)),
	business_type_name,
    performance_region_name,
	performance_province_name,
	performance_city_name
 grouping sets ((business_type_name),(business_type_name,performance_region_name,performance_province_name,performance_city_name)
 ,(business_type_name,performance_region_name,performance_province_name))
),

 province_sale as 
(
select
	performance_region_name,
	performance_province_name,
	performance_city_name,
	sum(bz_sale_amt) bz_sq_sale_amt,
	sum(bz_profit) as bz_sq_profit,
	sum(sz_sale_amt) sz_sq_sale_amt,
	sum(sz_profit) as sz_sq_profit	
from province_business_sale
group by performance_region_name,performance_province_name,performance_city_name
),

 province_business_sale_eff as 
(
select
	a.business_type_name,
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	a.bz_sale_amt,
	a.bz_profit,
	a.bz_profit/abs(a.bz_sale_amt) as bz_profit_rate,
	a.sz_sale_amt,
	a.sz_profit,
	a.sz_profit/abs(a.sz_sale_amt) as sz_profit_rate,
	b.bz_sq_sale_amt,
	b.bz_sq_profit,
	b.bz_sq_profit/abs(b.bz_sq_sale_amt) as bz_sq_profit_rate,
	b.sz_sq_sale_amt,
	b.sz_sq_profit,
	b.sz_sq_profit/abs(b.sz_sq_sale_amt) as sz_sq_profit_rate,
	-- 销售额占比
	a.bz_sale_amt/b.bz_sq_sale_amt as bz_sale_amt_zb,
	a.sz_sale_amt/b.sz_sq_sale_amt as sz_sale_amt_zb,
	a.bz_sale_amt/b.bz_sq_sale_amt - a.sz_sale_amt/b.sz_sq_sale_amt as bz_sale_amt_zb_hb,
	
	-- 毛利额占比
	a.bz_profit/b.bz_sq_profit as bz_profit_zb,
	a.sz_profit/b.sz_sq_profit as sz_profit_zb,
	a.bz_profit/b.bz_sq_profit - a.sz_profit/b.sz_sq_profit as bz_profit_zb_hb,	
	
	-- 毛利率环比
	a.bz_profit/abs(a.bz_sale_amt) - a.sz_profit/abs(a.sz_sale_amt) as bz_profit_rate_hb
from province_business_sale a 
left join province_sale b on a.performance_province_name=b.performance_province_name and a.performance_city_name=b.performance_city_name
)
select 
	business_type_name,
	performance_region_name,
	performance_province_name,
	performance_city_name,
	bz_sale_amt,
	bz_profit,
	bz_profit_rate,
	sz_sale_amt,
	sz_profit,
	sz_profit_rate,
	bz_sq_sale_amt,
	bz_sq_profit,
	bz_sq_profit_rate,
	sz_sq_sale_amt,
	sz_sq_profit,
	sz_sq_profit_rate,
	-- 销售额占比
	bz_sale_amt_zb,
	sz_sale_amt_zb,
	bz_sale_amt_zb_hb,
	
	-- 毛利额占比
	bz_profit_zb,
	sz_profit_zb,
	bz_profit_zb_hb,
	
	-- 毛利率环比
	bz_profit_rate_hb,
	-- 业绩变化影响 
	-- 本期省区毛利率-（本期省区毛利额-业务的销售额差异*本期业务毛利率）/本期省区剔除当前业务后的销售额
	if(round(bz_sale_amt,4)=0 or round(sz_sale_amt,4)=0,
		bz_sq_profit_rate-(bz_sq_sale_amt*bz_sq_profit_rate-if(round(bz_sale_amt,4)<>0,bz_sale_amt,-sz_sale_amt)*if(round(bz_sale_amt,4)<>0,bz_profit_rate,sz_profit_rate))/abs(bz_sq_sale_amt-if(bz_sale_amt<>0,bz_sale_amt,-sz_sale_amt)),
		bz_sq_profit_rate-(bz_sq_sale_amt*bz_sq_profit_rate-(bz_sale_amt-sz_sale_amt)*bz_profit_rate)/abs(bz_sq_sale_amt-(bz_sale_amt-sz_sale_amt))
		) as prorate_sale_eff,
	-- 毛利率波动影响
	-- 本期业务销售额*（本期业务毛利率-上期业务毛利率）/本期省区销售额
	bz_sale_amt*(bz_profit_rate-sz_profit_rate)/abs(bz_sq_sale_amt) as prorate_profit_eff,
	-- 剔除法影响值
	round(bz_sq_profit/abs(bz_sq_sale_amt)-(bz_sq_profit-bz_profit)/abs(bz_sq_sale_amt-bz_sale_amt),6) as bz_prorate_eff,
	round(sz_sq_profit/abs(sz_sq_sale_amt)-(sz_sq_profit-bz_profit)/abs(sz_sq_sale_amt-sz_sale_amt),6) as sz_prorate_eff
from province_business_sale_eff;




-- 日配-业务结构
with 
 province_business_sale as 
(
select 
	-- weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)) week,
	-- business_type_name,
	if( c.shop_code is null,'日配非直送仓','日配直送仓') is_zsc,
	coalesce(performance_region_name,'全国') as performance_region_name,
	coalesce(performance_province_name,'全国') as performance_province_name,
	coalesce(performance_city_name,'全国') as performance_city_name,
	sum(case when a.smonth=substr(regexp_replace('${i_sdate}','-',''),1,6) then sale_amt else 0 end)/10000 as bz_sale_amt,
	sum(case when a.smonth=substr(regexp_replace('${i_sdate}','-',''),1,6) then profit else 0 end)/10000 as bz_profit,
	sum(case when a.smonth=substr(regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-',''),1,6) and sdt<=regexp_replace(add_months('${i_sdate}',-1),'-','') then sale_amt else 0 end)/10000 as sz_sale_amt,
	sum(case when a.smonth=substr(regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-',''),1,6) and sdt<=regexp_replace(add_months('${i_sdate}',-1),'-','') then profit else 0 end)/10000 as sz_profit
from
	( 
	select 
		inventory_dc_code,
		case when channel_code='2' then '商超' else 'B+BBC' end as channel_name,
		performance_region_name,
		performance_province_name,
		performance_city_name,
		weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)) week,
		substr(sdt,1,6) smonth,
		sdt,
		business_type_name,
		sale_amt,
		profit
	from csx_dws.csx_dws_sale_detail_di
	where sdt>=regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','')
	-- and sdt <='20231208'
	and performance_province_name not like '平台%'
	and business_type_code in('1')
	and performance_region_name='华南大区'

	)a
left join 
	( 
    select
       distinct shop_code 
	from csx_dim.csx_dim_shop 
	where sdt='current' and shop_low_profit_flag=1  
	)c on a.inventory_dc_code = c.shop_code
group by 
	-- weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)),
	-- business_type_name,
	if( c.shop_code is null,'日配非直送仓','日配直送仓'),
    performance_region_name,
	performance_province_name,
	performance_city_name
 grouping sets ((if( c.shop_code is null,'日配非直送仓','日配直送仓')),
 (if( c.shop_code is null,'日配非直送仓','日配直送仓'),performance_region_name,performance_province_name,performance_city_name),
 (if( c.shop_code is null,'日配非直送仓','日配直送仓'),performance_region_name,performance_province_name))
),

 province_sale as 
(
select
	performance_province_name,
	performance_region_name,
	performance_city_name,
	sum(bz_sale_amt) bz_sq_sale_amt,
	sum(bz_profit) as bz_sq_profit,
	sum(sz_sale_amt) sz_sq_sale_amt,
	sum(sz_profit) as sz_sq_profit	
from province_business_sale
group by performance_region_name,performance_province_name,performance_city_name
),

 province_business_sale_eff as 
(
select
	a.is_zsc,
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	a.bz_sale_amt,
	a.bz_profit,
	a.bz_profit/abs(a.bz_sale_amt) as bz_profit_rate,
	a.sz_sale_amt,
	a.sz_profit,
	a.sz_profit/abs(a.sz_sale_amt) as sz_profit_rate,
	b.bz_sq_sale_amt,
	b.bz_sq_profit,
	b.bz_sq_profit/abs(b.bz_sq_sale_amt) as bz_sq_profit_rate,
	b.sz_sq_sale_amt,
	b.sz_sq_profit,
	b.sz_sq_profit/abs(b.sz_sq_sale_amt) as sz_sq_profit_rate,
	-- 销售额占比
	a.bz_sale_amt/b.bz_sq_sale_amt as bz_sale_amt_zb,
	a.sz_sale_amt/b.sz_sq_sale_amt as sz_sale_amt_zb,
	a.bz_sale_amt/b.bz_sq_sale_amt - a.sz_sale_amt/b.sz_sq_sale_amt as bz_sale_amt_zb_hb,
	
	-- 毛利额占比
	a.bz_profit/b.bz_sq_profit as bz_profit_zb,
	a.sz_profit/b.sz_sq_profit as sz_profit_zb,
	a.bz_profit/b.bz_sq_profit - a.sz_profit/b.sz_sq_profit as bz_profit_zb_hb,
	
	-- 毛利率环比
	a.bz_profit/abs(a.bz_sale_amt) - a.sz_profit/abs(a.sz_sale_amt) as bz_profit_rate_hb
from province_business_sale a 
left join province_sale b on a.performance_province_name=b.performance_province_name and a.performance_city_name=b.performance_city_name
)

select 
	is_zsc,
	performance_region_name,
	performance_province_name,
	performance_city_name,
	bz_sale_amt,
	bz_profit,
	bz_profit_rate,
	sz_sale_amt,
	sz_profit,
	sz_profit_rate,
	bz_sq_sale_amt,
	bz_sq_profit,
	bz_sq_profit_rate,
	sz_sq_sale_amt,
	sz_sq_profit,
	sz_sq_profit_rate,
	-- 销售额占比
	bz_sale_amt_zb,
	sz_sale_amt_zb,
	bz_sale_amt_zb_hb,
	
	-- 毛利额占比
	bz_profit_zb,
	sz_profit_zb,
	bz_profit_zb_hb,
	
	-- 毛利率环比
	bz_profit_rate_hb,
	-- 业绩变化影响 
	-- 本期省区毛利率-（本期省区毛利额-业务的销售额差异*本期业务毛利率）/本期省区剔除当前业务后的销售额
	if(round(bz_sale_amt,4)=0 or round(sz_sale_amt,4)=0,
		bz_sq_profit_rate-(bz_sq_sale_amt*bz_sq_profit_rate-if(round(bz_sale_amt,4)<>0,bz_sale_amt,-sz_sale_amt)*if(round(bz_sale_amt,4)<>0,bz_profit_rate,sz_profit_rate))/abs(bz_sq_sale_amt-if(bz_sale_amt<>0,bz_sale_amt,-sz_sale_amt)),
		bz_sq_profit_rate-(bz_sq_sale_amt*bz_sq_profit_rate-(bz_sale_amt-sz_sale_amt)*bz_profit_rate)/abs(bz_sq_sale_amt-(bz_sale_amt-sz_sale_amt))
		) as prorate_sale_eff,
	-- 毛利率波动影响
	-- 本期业务销售额*（本期业务毛利率-上期业务毛利率）/本期省区销售额
	bz_sale_amt*(bz_profit_rate-sz_profit_rate)/abs(bz_sq_sale_amt) as prorate_profit_eff,
	-- 剔除法影响值
	round(bz_sq_profit/abs(bz_sq_sale_amt)-(bz_sq_profit-bz_profit)/abs(bz_sq_sale_amt-bz_sale_amt),6) as bz_prorate_eff,
	round(sz_sq_profit/abs(sz_sq_sale_amt)-(sz_sq_profit-bz_profit)/abs(sz_sq_sale_amt-sz_sale_amt),6) as sz_prorate_eff
from province_business_sale_eff;







	