
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 

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
	sum(case when a.smonth=substr(regexp_replace('${i_sdate}','-',''),1,6)  then sale_amt else 0 end)/10000 as bz_sale_amt,
	sum(case when a.smonth=substr(regexp_replace('${i_sdate}','-',''),1,6)  then profit else 0 end)/10000 as bz_profit,
	sum(case when a.smonth=substr(regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-',''),1,6) then sale_amt else 0 end)/10000 as sz_sale_amt,
	sum(case when a.smonth=substr(regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-',''),1,6) then profit else 0 end)/10000 as sz_profit
from
( 
select 
	case when channel_code='2' then '商超' else 'B+BBC' end as channel_name,
    performance_region_name,
	performance_province_name,
	performance_city_name,
	weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)) week,
	substr(sdt,1,6) smonth,
	business_type_name,
	sale_amt,
	profit
from csx_dws.csx_dws_sale_detail_di
where sdt>=regexp_replace(add_months('${i_sdate}',-1),'-','')
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
	sum(case when a.smonth=substr(regexp_replace('${i_sdate}','-',''),1,6) then sale_amt else 0 end)/10000 as bq_sale_amt,
	sum(case when a.smonth=substr(regexp_replace('${i_sdate}','-',''),1,6) then profit else 0 end)/10000 as bq_profit,
	sum(case when a.smonth=substr(regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-',''),1,6) then sale_amt else 0 end)/10000 as sq_sale_amt,
	sum(case when a.smonth=substr(regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-',''),1,6) then profit else 0 end)/10000 as sq_profit
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
 grouping sets ((if( c.shop_code is null,'日配非直送仓','日配直送仓')),(if( c.shop_code is null,'日配非直送仓','日配直送仓'),performance_region_name,performance_province_name,performance_city_name))
),

 province_sale as 
(
select
	performance_region_name,
	performance_province_name,
	performance_city_name,
	sum(bq_sale_amt) bq_prov_sale_amt,
	sum(bq_profit) as bq_prov_profit,
	sum(sq_sale_amt) sq_prov_sale_amt,
	sum(sq_profit) as sq_prov_profit	
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
	a.bq_sale_amt,
	a.bq_profit,
	a.bq_profit/abs(a.bq_sale_amt) as bq_profit_rate,
	a.sq_sale_amt,
	a.sq_profit,
	a.sq_profit/abs(a.sq_sale_amt) as sq_profit_rate,
	b.bq_prov_sale_amt,
	b.bq_prov_profit,
	b.bq_prov_profit/abs(b.bq_prov_sale_amt) as bq_prov_profit_rate,
	b.sq_prov_sale_amt,
	b.sq_prov_profit,
	b.sq_prov_profit/abs(b.sq_prov_sale_amt) as sq_prov_profit_rate,
	-- 销售额占比
	a.bq_sale_amt/b.bq_prov_sale_amt as bq_sale_amt_zb,
	a.sq_sale_amt/b.sq_prov_sale_amt as sq_sale_amt_zb,
	a.bq_sale_amt/b.bq_prov_sale_amt - a.sq_sale_amt/b.sq_prov_sale_amt as bq_sale_amt_zb_hb,
	-- 毛利率环比
	a.bq_profit/abs(a.bq_sale_amt) - a.sq_profit/abs(a.sq_sale_amt) as bq_profit_rate_hb
from province_business_sale a 
left join province_sale b on a.performance_province_name=b.performance_province_name and a.performance_city_name=b.performance_city_name
)

select 
	is_zsc,
	performance_region_name,
	performance_province_name,
	performance_city_name,
	bq_sale_amt,
	bq_profit,
	bq_profit_rate,
	sq_sale_amt,
	sq_profit,
	sq_profit_rate,
	bq_prov_sale_amt,
	bq_prov_profit,
	bq_prov_profit_rate,
	sq_prov_sale_amt,
	sq_prov_profit,
	sq_prov_profit_rate,
	-- 销售额占比
	bq_sale_amt_zb,
	sq_sale_amt_zb,
	bq_sale_amt_zb_hb,
	-- 毛利率环比
	bq_profit_rate_hb,
	-- 业绩变化影响 
	-- 本期省区毛利率-（本期省区毛利额-业务的销售额差异*本期业务毛利率）/本期省区剔除当前业务后的销售额
	if(round(bq_sale_amt,4)=0 or round(sq_sale_amt,4)=0,
		bq_prov_profit_rate-(bq_prov_sale_amt*bq_prov_profit_rate-if(round(bq_sale_amt,4)<>0,bq_sale_amt,-sq_sale_amt)*if(round(bq_sale_amt,4)<>0,bq_profit_rate,sq_profit_rate))/abs(bq_prov_sale_amt-if(bq_sale_amt<>0,bq_sale_amt,-sq_sale_amt)),
		bq_prov_profit_rate-(bq_prov_sale_amt*bq_prov_profit_rate-(bq_sale_amt-sq_sale_amt)*bq_profit_rate)/abs(bq_prov_sale_amt-(bq_sale_amt-sq_sale_amt))
		) as prorate_sale_eff,
	-- 毛利率波动影响
	-- 本期业务销售额*（本期业务毛利率-上期业务毛利率）/本期省区销售额
	bq_sale_amt*(bq_profit_rate-sq_profit_rate)/abs(bq_prov_sale_amt) as prorate_profit_eff,
	-- 剔除法影响值
	round(bq_prov_profit/abs(bq_prov_sale_amt)-(bq_prov_profit-bq_profit)/abs(bq_prov_sale_amt-bq_sale_amt),6) as bz_prorate_eff,
	round(sq_prov_profit/abs(sq_prov_sale_amt)-(sq_prov_profit-bq_profit)/abs(sq_prov_sale_amt-sq_sale_amt),6) as sz_prorate_eff
from province_business_sale_eff;


-- 异常影响

-- 生鲜食百对毛利影响--------------------------------------
-- 备注：1、价格补救应该补救到原单上算毛利；
select 
	(case when a.performance_province_name='河南省' then '华北大区' 
	      when a.performance_province_name in ('安徽省','湖北省') then '华东大区' 
	else a.performance_region_name end) as `大区`,
	a.performance_province_name as `省区`,
	(case when a.performance_province_name in ('上海松江') then '上海松江' 
	      when a.performance_province_name in ('江苏苏州') then '江苏苏州'  
	else a.performance_city_name end) as `城市`,
	substr(c.business_division_name,1,2) as `生鲜or食百`,
	-- c.classify_middle_name as `管理中类`,
	-- c.classify_small_name as `管理小类`,
	-- a.customer_code as `客户编码`,
	-- max(a.customer_name) as `客户名称`,
	-- nvl(f.fir_price_type,e.price_type1) as `定价类型1`,
	-- nvl(f.sec_price_type,e.price_type2) as `定价类型2`,
	-- e.price_period_name as `报价周期`,
	-- e.price_date_name as `报价日期`,
	-- a.goods_code as `商品编码`,
	-- c.goods_name as `商品名称`,
	-- 直送>调价>返利，>退货
	case 
		when a.inventory_dc_code in ('W0J2') then '监狱仓'
		when a.inventory_dc_code in ('W0AJ','W0G6','WB71') then '海军仓'
		when a.delivery_type_code=2 then '直送'
		when a.order_channel_code in ('6')  then '调价'
		when a.order_channel_code in ('4')  then '返利'
		when a.refund_order_flag=1 then '退货'
		else '其他' end as group_1,
	case 
		when a.delivery_type_name<>'直送' then ''
		when a.direct_delivery_type=1 then 'R直送1'
		when a.direct_delivery_type=2 then 'Z直送2'
		when a.direct_delivery_type=11 then '临时加单'
		when a.direct_delivery_type=12 then '紧急补货'
		when a.direct_delivery_type=0 then '普通' else '普通' end as zhisong_flag,	
	nvl(sum(case when a.sdt >= regexp_replace(trunc('${yes_sdt_date}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${yes_sdt_date}',0),'-','') then a.sale_amt end),0) as `本月销售额`,
	nvl(sum(case when a.sdt >= regexp_replace(trunc('${yes_sdt_date}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${yes_sdt_date}',0),'-','') and d.original_order_code is not null then a.profit-d.sale_cost 
				 when a.sdt >= regexp_replace(trunc('${yes_sdt_date}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${yes_sdt_date}',0),'-','') then a.profit end),0) as `本月毛利额`,	
	nvl(sum(case when a.sdt >= regexp_replace(trunc('${yes_sdt_date}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${yes_sdt_date}',0),'-','') then a.sale_qty end),0) as `本月销量`,

	nvl(sum(case when a.sdt >= regexp_replace(add_months(trunc('${yes_sdt_date}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${yes_sdt_date}',-1),'-','') then a.sale_amt end),0) as `上月销售额`,
	nvl(sum(case when a.sdt >= regexp_replace(add_months(trunc('${yes_sdt_date}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${yes_sdt_date}',-1),'-','') and d.original_order_code is not null then a.profit-d.sale_cost 
				 when a.sdt >= regexp_replace(add_months(trunc('${yes_sdt_date}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${yes_sdt_date}',-1),'-','') then a.profit end),0) as `上月毛利额`,	
	nvl(sum(case when a.sdt >= regexp_replace(add_months(trunc('${yes_sdt_date}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${yes_sdt_date}',-1),'-','') then a.sale_qty end),0) as `上月销量`,
	
	nvl(sum(case when a.week=weekofyear(date_sub(current_date, 3)) then a.sale_amt end),0) as `本周销售额`,
	nvl(sum(case when a.week=weekofyear(date_sub(current_date, 3)) and d.original_order_code is not null then a.profit-d.sale_cost
				 when a.week=weekofyear(date_sub(current_date, 3)) then a.profit end),0) as `本周毛利额`,
	nvl(sum(case when a.week=weekofyear(date_sub(current_date, 3)) then a.sale_qty end),0) as `本周销量`,	
	nvl(sum(case when a.week=weekofyear(date_sub(current_date, 3+7)) then a.sale_amt end),0) as `上周销售额`,
	nvl(sum(case when a.week=weekofyear(date_sub(current_date, 3+7)) and d.original_order_code is not null then a.profit-d.sale_cost
				 when a.week=weekofyear(date_sub(current_date, 3+7)) then a.profit end),0) as `上周毛利额`,
	nvl(sum(case when a.week=weekofyear(date_sub(current_date, 3+7)) then a.sale_qty end),0) as `上周销量`
from 
	(select *,weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)) week
	from csx_dws.csx_dws_sale_detail_di 
	where sdt>=regexp_replace(add_months(trunc('${yes_sdt_date}','MM'),-1),'-','') 
	and sdt<=regexp_replace('${yes_sdt_date}','-','')   
	and business_type_code='1' 
	-- and order_channel_code not in ('4','6') 
	-- and delivery_type_code<>2 
	-- and refund_order_flag<>1 
	-- and inventory_dc_code not in ('W0J2','W0AJ','W0G6','WB71') 
	-- and (order_channel_detail_code<>26 or order_channel_detail_code is null) 
	-- and classify_middle_name in('蛋','米','食用油类','调味品类')
	) a 
	left join 
	(select * 
	from csx_dim.csx_dim_shop  
	where sdt='current' 
	and shop_low_profit_flag=1 
	) b 
	on a.inventory_dc_code=b.shop_code 
	left join 
	(select * 
	from csx_dim.csx_dim_basic_goods 
	where sdt='current'
	) c 
	on a.goods_code=c.goods_code 
	left join 
	(select * 
	from csx_dws.csx_dws_sale_detail_di 
	where sdt>=regexp_replace(add_months(trunc('${yes_sdt_date}','MM'),-1),'-','') 
	and sdt<=regexp_replace('${yes_sdt_date}','-','') 
	and business_type_code='1' 
	and order_channel_detail_code=26
	) d 
	on a.order_code=d.original_order_code and a.goods_code=d.goods_code 
	left join 
	csx_analyse_tmp.csx_analyse_tmp_customer_price_type_ky_tmp1 e 
	on a.customer_code=e.customer_code 
	-- left join 
	-- -- 线下表客户定价类型
	-- csx_ods.csx_ods_data_analysis_prd_cus_price_type_231206_df f  
	-- on a.customer_code=f.customer_code 
where b.shop_code is null 
group by 
	(case when a.performance_province_name='河南省' then '华北大区' 
	      when a.performance_province_name in ('安徽省','湖北省') then '华东大区' 
	else a.performance_region_name end),
	a.performance_province_name,
	(case when a.performance_province_name in ('上海松江') then '上海松江' 
	      when a.performance_province_name in ('江苏苏州') then '江苏苏州'  
	else a.performance_city_name end),
	substr(c.business_division_name,1,2),
	c.classify_middle_name,
	c.classify_small_name,
	-- a.goods_code,
	-- c.goods_name,
	-- a.customer_code,
	-- nvl(f.fir_price_type,e.price_type1),
	-- nvl(f.sec_price_type,e.price_type2),
	-- e.price_period_name,
	-- e.price_date_name
	case 
		when a.inventory_dc_code in ('W0J2') then '监狱仓'
		when a.inventory_dc_code in ('W0AJ','W0G6','WB71') then '海军仓'
		when a.delivery_type_code=2 then '直送'
		when a.order_channel_code in ('6')  then '调价'
		when a.order_channel_code in ('4')  then '返利'
		when a.refund_order_flag=1 then '退货'
		else '其他' end,
	case 
		when a.delivery_type_name<>'直送' then ''
		when a.direct_delivery_type=1 then 'R直送1'
		when a.direct_delivery_type=2 then 'Z直送2'
		when a.direct_delivery_type=11 then '临时加单'
		when a.direct_delivery_type=12 then '紧急补货'
		when a.direct_delivery_type=0 then '普通' else '普通' end	
;



-- 退货原因
-- 直送>调价>返利，>退货

select 
    a.sdt,
	a.performance_region_name,     --  销售大区名称(业绩划分)
	a.performance_province_name,     --  销售归属省区名称
	a.performance_city_name,     --  城市组名称(业绩划分)
	a.customer_code,
	c.customer_name,     --  客户名称	
	a.order_code,
	a.business_type_name,
	a.delivery_type_name,
	a.classify_middle_name,     --  管理中类名称
	a.goods_code,     --  商品编码
	a.goods_name,     --  商品名称
	a.sale_amt,     --  含税销售金额
	a.profit,     --  含税定价毛利额
	a.sale_qty,     --  销售数量
	b.source_biz_type_name,  -- 订单业务来源
	b.order_status_name,  -- 退货单状态
	b.has_goods_name,
	b.child_return_type_name,  -- 子退货单类型 ：0-父退货单 1-子退货单逆向 2-子退货单正向
	b.refund_order_type_name,	-- 退货单类型(0:差异单 1:退货单）
	b.first_level_reason_name,
	b.second_level_reason_name	
from 
	(select *,weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)) week
	from csx_dws.csx_dws_sale_detail_di 
	where sdt>=replace(date_sub(current_date,9),'-','')
	and sdt<=replace(date_sub(current_date,3),'-','') 
	and business_type_code='1' 
	and order_channel_code not in ('4','6') 
	and refund_order_flag=1 
	)a 
left join 
(
select
	inventory_dc_code,
	inventory_dc_name, 
	sdt,	
	refund_code,
	order_status_code,  -- 退货单状态: 10-差异待审(预留) 20-处理中 30-处理完成 -1-差异拒绝
	case order_status_code
	when -1 then '差异拒绝'
	when 10 then '差异待审'
	when 20 then '处理中'
	when 30 then '处理完成'
	else order_status_code end as order_status_name,  -- 退货单状态
	sale_order_code,
	customer_code,
	regexp_replace(regexp_replace(customer_name,'\n',''),'\r','') as customer_name,
	sub_customer_code,
	regexp_replace(regexp_replace(sub_customer_name,'\n',''),'\r','') as sub_customer_name,
	goods_code,
	regexp_replace(regexp_replace(goods_name,'\n',''),'\r','') as goods_name, 
	case source_biz_type
	when -1 then 'B端订单管理退货'
	when 0 then 'OMS物流审核'
	when 1 then '结算调整数量'
	when 2 then 'OMS调整数量'
	when 3 then 'CRM客诉退货'
	when 4 then 'CRM订单售后退货'
	when 5 then 'CRM预退货审核'
	when 6 then 'CRM签收'
	when 7 then '司机送达时差异'
	when 8 then '司机发起退货'
	when 9 then '实物退仓收货差异'
	when 10 then 'OMS签收'
	end as source_biz_type_name,  -- 订单业务来源（-1-B端订单管理退货 0-OMS物流审核 1-结算调整数量 2-OMS调整数量 3-CRM客诉退货 4-CRM订单售后退货 5-CRM预退货审核 6-CRM签收 7-司机送达时差异 8-司机发起退货 9-实物退仓收货差异 10-OMS签收）
	case refund_operation_type
	when -1 then '不处理'
	when 0 then '立即退'
	when 1 then '跟车退'
	end as refund_operation_type_name,  -- 退货处理方式 -1-不处理 0-立即退 1-跟车退
	case has_goods
	when 0 then '无实物'
	when 1 then '有实物'
	end as has_goods_name,	
	responsibility_reason,
	regexp_replace(regexp_replace(reason_detail,'\n',''),'\r','') as reason_detail,
	case source_type
	when 0 then '签收差异或退货'
	when 1 then '改单退货'
	end as source_type_name,  -- 订单来源(0-签收差异或退货 1-改单退货)
	case child_return_type_code
	when 0 then '父退货单'
	when 1 then '子退货单逆向'
	when 2 then '子退货单正向'
	end as child_return_type_name,  -- 子退货单类型 ：0-父退货单 1-子退货单逆向 2-子退货单正向
	case refund_order_type_code
	when 0 then '差异单'
	when 1 then '退货单'
	end as refund_order_type_name,	-- 退货单类型(0:差异单 1:退货单）
	refund_qty,
	sale_price,
	refund_total_amt,
	refund_scale_total_amt,
	first_level_reason_name,
	regexp_replace(regexp_replace(second_level_reason_name,'\n',''),'\r','') as second_level_reason_name
from csx_dwd.csx_dwd_oms_sale_refund_order_detail_di
where sdt>=replace(date_sub(current_date,40),'-','')
and child_return_type_code=1
and parent_refund_code<>''
)b on a.order_code=b.refund_code and a.goods_code=b.goods_code
left join 
(
	select  
		bloc_code,     --  集团编码
		bloc_name,     --  集团名称
		parent_id,customer_id,
		customer_code,
		customer_name,     --  客户名称
		first_category_name,     --  一级客户分类名称
		second_category_name,     --  二级客户分类名称
		performance_region_name,     --  销售大区名称(业绩划分)
		performance_province_name,     --  销售归属省区名称
		performance_city_name     --  城市组名称(业绩划分)
	from csx_dim.csx_dim_crm_customer_info
	where sdt='current'
	and customer_type_code=4
)c on a.customer_code=c.customer_code;












	