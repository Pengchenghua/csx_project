-- 福利BBC

-- BBC销售额 月同比增长率（农历） 先确定阳历24年6月，对应的阴历是4.25-5.25，找去年同期阴历对应的阳历是6.12-7.12
select
	substr(regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-',''),1,6) smonth,
	concat_ws('-',regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-',''),regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')) as date_bq,
	concat_ws('-',b.min_calday,b.max_calday) as date_sq,
	performance_region_name,
	performance_province_name,
	sale_amt_bq,
	sale_amt_sq,
	(sale_amt_bq-sale_amt_sq)/sale_amt_sq as sale_amt_tb_rate
from
( 
select
	performance_region_name,
	performance_province_name,
	sum(case when flag='本期' then sale_amt end) as sale_amt_bq,
	-- sum(case when flag='本期' then profit end) as profit_bq,
	sum(case when flag='上期' then sale_amt end) as sale_amt_sq
	-- sum(case when flag='上期' then profit end) as profit_sq
	-- (sum(case when flag='本期' then sale_amt end)-sum(case when flag='上期' then sale_amt end))/
	-- sum(case when flag='上期' then sale_amt end) sale_amt_tb_rate
from 
(
	select
		'本期' as flag,
		performance_region_name,
		performance_province_name,
		sum(sale_amt)/10000 as sale_amt,
		sum(profit)/10000 as profit
	from csx_dws.csx_dws_bbc_sale_detail_di 
	where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','')
	and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
	and business_type_code in(6)
	and operation_mode_code not in ('3','4')
	group by 
		performance_region_name,
		performance_province_name
	
	union all
	select
		'上期' as flag,
		performance_region_name,
		performance_province_name,
		sum(sale_amt)/10000 as sale_amt,
		sum(profit)/10000 as profit
	from
	(
	select *
	from 
	(
		select b.calday,b.calendar_lunar,
		-- 遇到闰月的时候可能需要手动调整 如同时出现二月廿三 和闰二月廿三，需要看实际应该第一个还是第二个有效，调整下方升序或降序
		row_number() over(partition by b.calendar_lunar order by b.calday desc) as rno
		from 
		(
			select 
			calday,calendar_lunar,
			regexp_replace(add_months(date_format(from_unixtime(unix_timestamp(calendar_lunar,'yyyyMMdd')), 'yyyy-MM-dd'),-12),'-','') as calendar_lunar_last
			from csx_dim.csx_dim_basic_date
			-- where calday>='20240401' and calday<'20240501'
			where calday>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','')
			and calday<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
		)a 
		left join 
		(
			select calday,calendar_lunar
			from csx_dim.csx_dim_basic_date
		)b on a.calendar_lunar_last=b.calendar_lunar
	)a 
	where rno=1
	)a 
	join 
	(
		select
			*
		from csx_dws.csx_dws_bbc_sale_detail_di 
		-- where sdt>='20230101' and sdt<'20240101'
		where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-15),'-','')
		and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-9)),'-','')		
		and operation_mode_code not in('3','4')
	)b on a.calday=b.sdt
	group by 
		performance_region_name,
		performance_province_name	
)a
group by 
	performance_region_name,
	performance_province_name
)a
left join 
(
	select 
	min(calday) as min_calday,
	max(calday) as max_calday,
	min(calendar_lunar) as min_calendar_lunar,
	max(calendar_lunar) as max_calendar_lunar
	from 
	(
		select b.calday,b.calendar_lunar,
		-- 遇到闰月的时候可能需要手动调整 如同时出现二月廿三 和闰二月廿三，需要看实际应该第一个还是第二个有效，调整下方升序或降序
		row_number() over(partition by b.calendar_lunar order by b.calday desc) as rno
		from 
		(
			select 
			calday,calendar_lunar,
			regexp_replace(add_months(date_format(from_unixtime(unix_timestamp(calendar_lunar,'yyyyMMdd')), 'yyyy-MM-dd'),-12),'-','') as calendar_lunar_last
			from csx_dim.csx_dim_basic_date
			-- where calday>='20240401' and calday<'20240501'
			where calday>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','')
			and calday<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
		)a 
		left join 
		(
			select calday,calendar_lunar
			from csx_dim.csx_dim_basic_date
		)b on a.calendar_lunar_last=b.calendar_lunar
	)a 
	where rno=1
)b on 1=1	
;

/*
-- 不可用 因为农历月有大小月天数不能按照阳历的来
select *
from 
(
select calday,calendar_lunar,
row_number() over(partition by 1 order by calday desc) as rno
from csx_dim.csx_dim_basic_date
where calendar_lunar in(
			select 
			-- calendar_lunar,
			regexp_replace(add_months(date_format(from_unixtime(unix_timestamp(calendar_lunar,'yyyyMMdd')), 'yyyy-MM-dd'),-12),'-','') as calendar_lunar_last
			from csx_dim.csx_dim_basic_date
			-- where calday>='20240401' and calday<'20240501'
			where calday>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','')
			and calday<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
)
)a 
-- 遇到闰月的时候需要手动调整
where rno<=(datediff(last_day(add_months('${sdt_yes_date}',-1)),add_months(trunc('${sdt_yes_date}','MM'),-1))+1)
*/





-- BBC毛利率 以去年同季度毛利率均值为目标
select
	substr(regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-',''),1,6) smonth,
	performance_region_name,
	performance_province_name,
	sum(case when flag='本期' then sale_amt end) as sale_amt_bq,
	sum(case when flag='本期' then profit end) as profit_bq,
	sum(case when flag='本期' then profit end)/abs(sum(case when flag='本期' then sale_amt end)) as profit_rate_bq,
	sum(case when flag='上期' then sale_amt end) as sale_amt_sq,
	sum(case when flag='上期' then profit end) as profit_sq,
	sum(case when flag='上期' then profit end)/abs(sum(case when flag='上期' then sale_amt end)) as profit_rate_sq
	-- (sum(case when flag='本期' then sale_amt end)-sum(case when flag='上期' then sale_amt end))/
	-- sum(case when flag='上期' then sale_amt end) sale_amt_tb_rate
from 
(
	select
		'本期' as flag,
		performance_region_name,
		performance_province_name,
		sum(sale_amt)/10000 as sale_amt,
		sum(profit)/10000 as profit
	from csx_dws.csx_dws_bbc_sale_detail_di 
	where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','')
	and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
	and operation_mode_code not in ('3','4')
	group by 
		performance_region_name,
		performance_province_name
	
	union all
	
	select
		'上期' as flag,
		performance_region_name,
		performance_province_name,
		sum(sale_amt)/10000 as sale_amt,
		sum(profit)/10000 as profit
	from csx_dws.csx_dws_bbc_sale_detail_di 
	where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-17),'-','')
	-- 去年同季度 昨日的上月再同季度
	and concat(substr(sdt,1,4),'Q',floor(substr(sdt,5,2)/3.1)+1)=concat(floor(substr(add_months('${sdt_yes_date}',-1),1,4)-1),'Q',floor(substr(add_months(trunc('${sdt_yes_date}','MM'),-1-12),6,2)/3.1)+1)
	and operation_mode_code not in ('3','4')
	group by 
		performance_region_name,
		performance_province_name	
)a
group by 
	performance_region_name,
	performance_province_name
;


-- BBC餐卡销售额 以去年销售额作为当月目标 达成率
select
	substr(regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-',''),1,6) smonth,
	performance_region_name,
	performance_province_name,
	sum(case when flag='本期' then sale_amt end) as sale_amt_bq,
	sum(case when flag='本期' then profit end) as profit_bq,
	sum(case when flag='本期' then profit end)/abs(sum(case when flag='本期' then sale_amt end)) as profit_rate_bq,
	sum(case when flag='上期' then sale_amt end) as sale_amt_sq,
	sum(case when flag='上期' then profit end) as profit_sq,
	sum(case when flag='上期' then profit end)/abs(sum(case when flag='上期' then sale_amt end)) as profit_rate_sq,
	sum(case when flag='本期' then sale_amt end)/sum(case when flag='上期' then sale_amt end) sale_amt_dc_rate
from 
(
	select
		'本期' as flag,
		performance_region_name,
		performance_province_name,
		sum(sale_amt)/10000 as sale_amt,
		sum(profit)/10000 as profit
	from csx_dws.csx_dws_bbc_sale_detail_di
	where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','')
	and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
	-- 餐卡
	and (credit_pay_type_name='餐卡' or credit_pay_type_code='F11')
	and operation_mode_code not in ('3','4')
	group by 
		performance_region_name,
		performance_province_name
	
	union all
	select
		'上期' as flag,
		performance_region_name,
		performance_province_name,
		sum(sale_amt)/10000 as sale_amt,
		sum(profit)/10000 as profit
	from csx_dws.csx_dws_bbc_sale_detail_di 
	-- 更改同期
	where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-13),'-','')
	and sdt<regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-12),'-','')
	-- 餐卡
	and (credit_pay_type_name='餐卡' or credit_pay_type_code='F11')
	and operation_mode_code not in ('3','4')
	group by 
		performance_region_name,
		performance_province_name	
)a
group by 
	performance_region_name,
	performance_province_name
;


--课组后台毛利 20250312
with tmp_bq_back as 
(select substr(regexp_replace(belong_date,'-',''),1,6) s_month,
	belong_province_code,
    belong_province_name,
    belong_city_code,
    belong_city_name,
case when substr(purchase_group_code,1,1) IN ('A','P') THEN '食百'
    when purchase_group_code in ('H01','H09','H10','H11','H90') or substr( purchase_group_code,1,1)='U' then '干货加工'
    when purchase_group_code in ('H04','H05','H06','H07','H08') then '肉禽水产'
    when purchase_group_code in ('H02','H03') then '蔬菜水果'
    end classify_large_name,
    purchase_group_name,
  sum(value_tax_total) back_profit
from
     csx_dwd.csx_dwd_pss_settle_settle_bill_di
where
  (  settlement_dc_name   like '%福利%'
  or settlement_dc_name   like '%BBC%'
  )
  and sdt>='20250101'  
  -- 增加归属日期
  and belong_date >= '2025-02-01' and belong_date <'2025-03-01'
group by
 case when substr(purchase_group_code,1,1) IN ('A','P') THEN '食百'
    when purchase_group_code in ('H01','H09','H10','H11','H90') or substr( purchase_group_code,1,1)='U' then '干货加工'
    when purchase_group_code in ('H04','H05','H06','H07','H08') then '肉禽水产'
    when purchase_group_code in ('H02','H03') then '蔬菜水果'
    end,
     belong_province_code,
    belong_province_name,
    belong_city_code,
    belong_city_name,
    purchase_group_name,
	substr(regexp_replace(belong_date,'-',''),1,6)
    )select * from tmp_bq_back

	;
-- 后台毛利

select belong_month,
	b.performance_region_name,
	-- b.performance_province_name,
	case when b.performance_city_name in('上海松江','上海宝山','江苏苏州') then b.performance_city_name else b.performance_province_name end as performance_province_name,
	sum(a.back_profit) back_profit
from 
(
	select
	    substr(regexp_replace(belong_date,'-',''),1,6) belong_month,
		settlement_dc_code,  -- 结算DC编码
		sum(value_tax_total)/10000 as back_profit   -- 价税合计
	from csx_dwd.csx_dwd_pss_settle_settle_bill_di
	where (settlement_dc_name like '%福利%' or settlement_dc_name like '%BBC%')
	and sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-2),'-','')
	-- 归属日期
	and belong_date >= add_months(trunc('${sdt_yes_date}','MM'),-1) 
	and belong_date <=last_day(add_months('${sdt_yes_date}',-1))

	group by settlement_dc_code,
	substr(regexp_replace(belong_date,'-',''),1,6) 
)a
left join (select * from csx_dim.csx_dim_shop where sdt='current')b on b.shop_code=a.settlement_dc_code
group by 
	b.performance_region_name,
	belong_month,
	-- b.performance_province_name
	case when b.performance_city_name in('上海松江','上海宝山','江苏苏州') then b.performance_city_name else b.performance_province_name end

; 

-- 采购-业绩综合毛利率
with sale as 
(-- 福利BBC销售额
select 
	substr(sdt,1,6) smonth,
	performance_region_name,
	-- performance_province_name,
	case when performance_city_name in('上海松江','上海宝山','江苏苏州') then performance_city_name else performance_province_name end as performance_province_name,
    sum(sale_amt)/10000 as sale_amt,
    sum(profit)/10000 as profit
from csx_dws.csx_dws_sale_detail_di a
where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','')
and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
and business_type_code in(2,6)
group by substr(sdt,1,6),
	performance_region_name,
	-- performance_province_name
	case when performance_city_name in('上海松江','上海宝山','江苏苏州') then performance_city_name else performance_province_name end
)
,
-- 后台返利
 back as 
(
select
	b.performance_region_name,
	-- b.performance_province_name,
	case when b.performance_city_name in('上海松江','上海宝山','江苏苏州') then b.performance_city_name else b.performance_province_name end as performance_province_name,
	sum(a.back_profit) back_profit
from 
(
	select
		settlement_dc_code,  -- 结算DC编码
		sum(value_tax_total)/10000 as back_profit   -- 价税合计
	from csx_dwd.csx_dwd_pss_settle_settle_bill_di
	where (settlement_dc_name like '%福利%' or settlement_dc_name like '%BBC%')
	and sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-2),'-','')
	-- 归属日期
	and belong_date >= add_months(trunc('${sdt_yes_date}','MM'),-1) 
	and belong_date <=last_day(add_months('${sdt_yes_date}',-1))

	group by settlement_dc_code
)a
left join (select * from csx_dim.csx_dim_shop where sdt='current')b on b.shop_code=a.settlement_dc_code
group by 
	b.performance_region_name,
	-- b.performance_province_name
	case when b.performance_city_name in('上海松江','上海宝山','江苏苏州') then b.performance_city_name else b.performance_province_name end
) 

select a.smonth,
	a.performance_region_name,
	a.performance_province_name,
	a.sale_amt,
	a.profit,
	b.back_profit 
from sale a
left join back b on a.performance_province_name=b.performance_province_name
;


-- 


-- 销售剔除永辉线下购、永辉生活
select 
	substr(a.sdt,1,6) smonth,
	performance_region_name,
	performance_province_name,
	performance_city_name,

	a.business_type_name,
	b.classify_large_name_new,
    b.classify_large_code,
    b.classify_large_name,
    b.classify_middle_code,
    b.classify_middle_name,	
    sum(a.sale_amt)/10000 as sale_amt,
    sum(a.profit)/10000 as profit,
	sum(a.profit)/abs(sum(a.sale_amt)) as profit_rate
from 
(	
select sdt,
	performance_region_name,
	performance_province_name,
	performance_city_name,
	operation_mode_name,
	business_type_name,
	goods_code,
	goods_name,
	sum(sale_amt) sale_amt,
	sum(profit) profit	
from csx_dws.csx_dws_sale_detail_di a
where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','')
and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
and business_type_code in(2)
and a.order_channel_detail_code not in (24,28)
	group by 
	 sdt,
	performance_region_name,
	performance_province_name,
	performance_city_name,
	operation_mode_name,
	business_type_name,
	goods_code,
	goods_name
union all 
select sdt,
	performance_region_name,
	performance_province_name,
	performance_city_name,
	operation_mode_name,
	'BBC'business_type_name,
	goods_code,
	goods_name,
	sum(sale_amt) sale_amt,
	sum(profit) profit	
from csx_dws.csx_dws_bbc_sale_detail_di  a
where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','')
and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
    and operation_mode_code  not in ('3','4') -- 剔除4永辉线下购\3永辉生活
	group by  sdt,
	performance_region_name,
	performance_province_name,
	performance_city_name,
	operation_mode_name,
	goods_code,
	goods_name
)a 
left join 
  (
    select
    goods_code,
    regexp_replace(regexp_replace(goods_name,'\n',''),'\r','') as goods_name,
    purchase_group_code as department_id,purchase_group_name as department_name,   
	case when classify_middle_name in ('水果','蔬菜','水产','牛羊','调理预制品','猪肉','家禽','米','蛋','熟食烘焙','干货') then classify_large_name
	else '食百' end as classify_large_name_new,	
    classify_large_code,classify_large_name, -- 管理大类
    classify_middle_code,classify_middle_name,-- 管理中类
    classify_small_code,classify_small_name-- 管理小类
    from csx_dim.csx_dim_basic_goods
    where sdt = 'current'
  )b on b.goods_code = a.goods_code
group by 
	substr(a.sdt,1,6),
	performance_region_name,
	performance_province_name,
	a.business_type_name,
	b.classify_large_name_new,
    b.classify_large_code,
    b.classify_large_name,	
    b.classify_middle_code,
    b.classify_middle_name,
	performance_city_name
order by a.business_type_name,b.classify_large_name_new,b.classify_large_code,b.classify_middle_code
;




-- 采购-全国品类业绩毛利率
select 
	substr(a.sdt,1,6) smonth,
	-- performance_region_name,
	-- performance_province_name,
	a.business_type_name,
	b.classify_large_name_new,
    b.classify_large_code,
    b.classify_large_name,
    b.classify_middle_code,
    b.classify_middle_name,	
    sum(a.sale_amt)/10000 as sale_amt,
    sum(a.profit)/10000 as profit,
	sum(a.profit)/abs(sum(a.sale_amt)) as profit_rate
from 
(	
select sdt,
	performance_region_name,
	performance_province_name,
	performance_city_name,
	operation_mode_name,
	business_type_name,
	goods_code,
	goods_name,
	sum(sale_amt) sale_amt,
	sum(profit) profit	
from csx_dws.csx_dws_sale_detail_di a
where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','')
and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
and business_type_code in(2)
	group by 
	 sdt,
	performance_region_name,
	performance_province_name,
	performance_city_name,
	operation_mode_name,
	business_type_name,
	goods_code,
	goods_name
union all 
select sdt,
	performance_region_name,
	performance_province_name,
	performance_city_name,
	operation_mode_name,
	'BBC'business_type_name,
	goods_code,
	goods_name,
	sum(sale_amt) sale_amt,
	sum(profit) profit	
from csx_dws.csx_dws_sale_detail_di a
where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','')
and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
	group by  sdt,
	performance_region_name,
	performance_province_name,
	performance_city_name,
	operation_mode_name,
	goods_code,
	goods_name
)a 
left join 
  (
    select
    goods_code,
    regexp_replace(regexp_replace(goods_name,'\n',''),'\r','') as goods_name,
    purchase_group_code as department_id,purchase_group_name as department_name,   
	case when classify_middle_name in ('水果','蔬菜','水产','牛羊','调理预制品','猪肉','家禽','米','蛋','熟食烘焙','干货') then classify_large_name
	else '食百' end as classify_large_name_new,	
    classify_large_code,classify_large_name, -- 管理大类
    classify_middle_code,classify_middle_name,-- 管理中类
    classify_small_code,classify_small_name-- 管理小类
    from csx_dim.csx_dim_basic_goods
    where sdt = 'current'
  )b on b.goods_code = a.goods_code
group by 
	substr(a.sdt,1,6),
	-- performance_region_name,
	-- performance_province_name,
	a.business_type_name,
	b.classify_large_name_new,
    b.classify_large_code,
    b.classify_large_name,	
    b.classify_middle_code,
    b.classify_middle_name
order by a.business_type_name,b.classify_large_name_new,b.classify_large_code,b.classify_middle_code
;




/*
-- 采购-周转 周转同承华因此未出数
select 
month_of_year `月份`,
-- performance_region_code	    as	`业绩归属大区编码`,
-- performance_region_name	    as	`业绩归属大区名称`,
-- performance_province_code	as	`绩效归属省区编码`,
-- performance_province_name	as	`绩效归属省区名称`,
-- performance_city_code	    as	`绩效归属城市编码`,
-- performance_city_name	    as	`绩效归属城市名称`,
 case when a.classify_middle_name in ('水果','蔬菜','水产','牛羊','调理预制品','猪肉','家禽','米','蛋','熟食烘焙','干货') then classify_large_name
 else '食百' end     as `管理大类合并`,
classify_large_code	as	`管理大类`,
classify_large_name	as	`管理大类名称`, 
classify_middle_code	as	`管理中类`,
classify_middle_name	as	`管理中类名称`,
sum(nearly30days_amt_no_tax)	as	`不含税近30天累计库存额`,
sum(nearly30days_sale_cost_no_tax)	as	`不含税近30天累计销售出库成本`,
--sum(out_nearly30days_province_transfer_cost) `跨省区出库成本`,
sum(nearly30days_amt_no_tax)/ sum(nearly30days_sale_cost_no_tax) as `近30周转`
from 
 csx_report.csx_report_cas_accounting_turnover_stock_cost_goods_detail_df_new a 
join 
(select distinct month_of_year, month_end from csx_dim.csx_dim_basic_date where calday=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','') ) b on a.sdt=b.month_end
 join 
 (select dc_code,regexp_replace(to_date(enable_time),'-','') enable_date 
 from csx_dim.csx_dim_csx_data_market_conf_supplychain_location 
 where sdt='current') c on a.dc_area_code=c.dc_code
 group by 
month_of_year  ,
-- performance_region_code	 ,
-- performance_region_name	 ,
-- performance_province_code,
-- performance_province_name,
-- performance_city_code	 ,
-- performance_city_name	 ,
  case when a.classify_middle_name in ('水果','蔬菜','水产','牛羊','调理预制品','猪肉','家禽','米','蛋','熟食烘焙','干货') then classify_large_name
 else '食百' end,
classify_large_code,
classify_large_name, 
classify_middle_code,
classify_middle_name	
























substr(regexp_replace(add_months(date_sub(current_date,1),-1),'-',''), 1, 6)
date_format(from_unixtime(unix_timestamp(calendar_lunar,'yyyyMMdd')), 'yyyy-MM-dd')


regexp_replace(add_months(date_format(from_unixtime(unix_timestamp(calendar_lunar,'yyyyMMdd')), 'yyyy-MM-dd'),-12),'-','')




