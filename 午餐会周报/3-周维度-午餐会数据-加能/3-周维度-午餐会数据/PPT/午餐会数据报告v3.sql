
-- 业绩环比 重点数据提醒（MTD、周维度）v1 日均业绩&成交客户数

select performance_region_code
,performance_region_name
,performance_province_code
,performance_province_name
,performance_city_code
,case when performance_city_name='宜宾' then '成都市'
		when performance_city_name in ('长寿区','石柱县')  then '重庆主城'
		when performance_province_name='东北' then '东北'
		when performance_city_name='阜阳市' then '合肥市'
		else performance_city_name end performance_city_name
,customer_code
,customer_name
,first_category_name
,second_category_name
,new_classify_name
,by_basic_days
,sy_basic_days
,by_work_days
,sy_work_days
,by_sale_amt
,by_profit
,by_sale_days
,sy_sale_amt
,sy_profit
,sy_sale_days
,by_rp_sale_amt
,by_rp_profit
,by_rp_sale_days
,sy_rp_sale_amt
,sy_rp_profit
,sy_rp_sale_days
,by_work_sale_amt
,by_work_profit
,by_work_sale_days
,sy_work_sale_amt
,sy_work_profit
,sy_work_sale_days
,by_rp_work_sale_amt
,by_rp_work_profit
,by_rp_work_sale_days
,sy_rp_work_sale_amt
,sy_rp_work_profit
,sy_rp_work_sale_days
,byrang
,syrang
,sdt
,month
 from csx_analyse.csx_analyse_fr_sale_r_m_customer_sales_avg_df
where month ='202502'

;
华南大区-华南大区年维度-日均业绩-B+BBC：
490.5万，同比上年：-0.5%(上周:-1.2%)—另24年：1.8%；
自营日配：282.4万，同比上年：4.6%(上周：4.6%)—另24年：11.5%；
日均成交客户数-B+BBC：551个，同比上年：14.4%；
自营日配：371个，同比上年：10.8%
华北大区-华北大区年维度-日均业绩-B+BBC：
468.7万，同比上年：16.2%(上周:15.1%)—另24年：30.2%；
自营日配：323.7万，同比上年：26.5%(上周：27.4%)—另24年：37.4%；
日均成交客户数-B+BBC：319个，同比上年：21%；
自营日配：222个，同比上年：17.6%
华西大区-华西大区年维度-日均业绩-B+BBC：
288.9万，同比上年：2.5%(上周:1%)—另24年：13.1%；
自营日配：217万，同比上年：18.7%(上周：16.9%)—另24年：17.9%；
日均成交客户数-B+BBC：548个，同比上年：-2.8%；
自营日配：443个，同比上年：-6.7%
华东大区-华中大区年维度-日均业绩-B+BBC：
491.9万，同比上年：-0.6%(上周:-2.3%)—另24年：-5.4%；
自营日配：295.3万，同比上年：-6.7%(上周：-8.6%)—另24年：-14.3%；
日均成交客户数-B+BBC：606个，同比上年：1.4%；
自营日配：467个，同比上年：2.3%
全国-全国年维度-日均业绩-B+BBC：
1740万，同比上年：4%(上周:2.7%)—另24年：7.7%；
自营日配：1118.4万，同比上年：9.1%(上周：8.3%)—另24年：9.5%；
日均成交客户数-B+BBC：2024个，同比上年：6.1%；
自营日配：1503个，同比上年：3.3%

华南大区-华南大区月维度-日均业绩-B+BBC：
429万，同比上年：4.9%(上周:2.5%)—另24年：1.8%；
自营日配：279万，同比上年：4%(上周：3.6%)—另24年：11.5%；
日均成交客户数-B+BBC：602个，同比上年：13.6%；
自营日配：408个，同比上年：7.6%
华北大区-华北大区月维度-日均业绩-B+BBC：
434.9万，同比上年：12.2%(上周:0.6%)—另24年：30.2%；
自营日配：323.7万，同比上年：7.4%(上周：-1.6%)—另24年：37.4%；
日均成交客户数-B+BBC：327个，同比上年：19.3%；
自营日配：239个，同比上年：17.5%
华西大区-华西大区月维度-日均业绩-B+BBC：
283.5万，同比上年：2.8%(上周:-9.1%)—另24年：13.1%；
自营日配：241.8万，同比上年：17.5%(上周：4.4%)—另24年：17.9%；
日均成交客户数-B+BBC：577个，同比上年：-8%；
自营日配：474个，同比上年：-10.9%
华东大区-华中大区月维度-日均业绩-B+BBC：
409.5万，同比上年：4.7%(上周:-8.4%)—另24年：-5.4%；
自营日配：302.6万，同比上年：8.4%(上周：4%)—另24年：-14.3%；
日均成交客户数-B+BBC：664个，同比上年：0%；
自营日配：535个，同比上年：2.2%
全国-全国月维度-日均业绩-B+BBC：
1556.9万，同比上年：6.4%(上周:-3.1%)—另24年：7.7%；
自营日配：1147.1万，同比上年：8.8%(上周：2.4%)—另24年：9.5%；
日均成交客户数-B+BBC：2169个，同比上年：3.6%；
自营日配：1655个，同比上年：1.1%


华南大区-华南大区年维度-日均业绩-B+BBC：
507.2万，同比上年：-1.6%(上周:-0.6%)—另24年：1.8%；
自营日配：283.3万，同比上年：4.8%(上周：-0.7%)—另24年：11.5%；
日均成交客户数-B+BBC：538个，同比上年：14.5%；
自营日配：361个，同比上年：11.8%
华北大区-华北大区年维度-日均业绩-B+BBC：
477.8万，同比上年：17.2%(上周:14.2%)—另24年：30.2%；
自营日配：323.6万，同比上年：32.8%(上周：28.6%)—另24年：37.4%；
日均成交客户数-B+BBC：316个，同比上年：21.4%；
自营日配：217个，同比上年：17.5%
华西大区-华西大区年维度-日均业绩-B+BBC：
290.3万，同比上年：2.5%(上周:-3.7%)—另24年：13.1%；
自营日配：210.3万，同比上年：19%(上周：13.2%)—另24年：17.9%；
日均成交客户数-B+BBC：540个，同比上年：-1.3%；
自营日配：434个，同比上年：-5.5%
华东大区-华中大区年维度-日均业绩-B+BBC：
514.2万，同比上年：-1.5%(上周:-4.3%)—另24年：-5.4%；
自营日配：293.2万，同比上年：-10.2%(上周：-12.8%)—另24年：-14.3%；
日均成交客户数-B+BBC：591个，同比上年：1.8%；
自营日配：449个，同比上年：2.2%
全国-全国年维度-日均业绩-B+BBC：
1789.5万，同比上年：3.5%(上周:1.2%)—另24年：7.7%；
自营日配：1110.5万，同比上年：9.2%(上周：4.9%)—另24年：9.5%；
日均成交客户数-B+BBC：1985个，同比上年：6.9%；
自营日配：1461个，同比上年：3.9%






--环比客户
select 
	d.performance_region_code,
	d.performance_region_name,
	d.performance_province_code,
	d.performance_province_name,
	d.performance_city_code,
	case when d.performance_city_name='宜宾' then '成都市'
		when d.performance_city_name in ('长寿区','石柱县') then '重庆主城'
		when d.performance_province_name='东北' then '东北'
		when d.performance_city_name='阜阳市' then '合肥市'
		else d.performance_city_name end as performance_city_name,
	a.customer_code,
	d.customer_name,
	d.first_category_name,
	d.second_category_name,
	e.new_classify_name,
	f.first_business_sale_date,
	d.sales_user_number,
	d.sales_user_name,
	d.supervisor_user_number,
	d.supervisor_user_name,	
	by_basic_days
	,sy_basic_days
	,by_work_days
	,sy_work_days
	,by_sale_amt
	,by_profit
	,by_profit_rate
	,by_sale_days
	,sy_sale_amt
	,sy_profit
	,sy_profit_rate	
	,sy_sale_days
	,by_rp_sale_amt
	,by_rp_profit
	,by_rp_profit_rate	
	,by_rp_sale_days
	,sy_rp_sale_amt
	,sy_rp_profit
	,sy_rp_profit_rate
	,sy_rp_sale_days
	,by_work_sale_amt
	,by_work_profit
	,by_work_profit_rate
	,by_work_sale_days
	,sy_work_sale_amt
	,sy_work_profit
	,sy_work_profit_rate
	,sy_work_sale_days
	,by_rp_work_sale_amt
	,by_rp_work_profit
	,by_rp_work_profit_rate
	,by_rp_work_sale_days
	,sy_rp_work_sale_amt
	,sy_rp_work_profit
	,sy_rp_work_profit_rate
	,sy_rp_work_sale_days 
	,sy_rp_sale_amt_all
	,sy_rp_profit_all
	,sy_rp_profit_rate_all
	,sy_rp_sale_days_all	
from 
 (select 
	customer_code
	,by_basic_days
	,sy_basic_days
	,by_work_days
	,sy_work_days
	,sum(by_sale_amt)/10000 by_sale_amt
	,sum(by_profit)/10000 by_profit
	,sum(by_profit)/abs(sum(by_sale_amt)) by_profit_rate
	,sum(by_sale_days) by_sale_days
	,sum(sy_sale_amt)/10000 sy_sale_amt
	,sum(sy_profit)/10000 sy_profit
	,sum(sy_profit)/abs(sum(sy_sale_amt)) sy_profit_rate	
	,sum(sy_sale_days) sy_sale_days
	,sum(by_rp_sale_amt)/10000 by_rp_sale_amt
	,sum(by_rp_profit)/10000 by_rp_profit
	,sum(by_rp_profit)/abs(sum(by_rp_sale_amt)) by_rp_profit_rate	
	,sum(by_rp_sale_days) by_rp_sale_days
	,sum(sy_rp_sale_amt)/10000 sy_rp_sale_amt
	,sum(sy_rp_profit)/10000 sy_rp_profit
	,sum(sy_rp_profit)/abs(sum(sy_rp_sale_amt)) sy_rp_profit_rate
	,sum(sy_rp_sale_days)sy_rp_sale_days
	,sum(by_work_sale_amt)/10000 by_work_sale_amt
	,sum(by_work_profit)/10000 by_work_profit
	,sum(by_work_profit)/abs(sum(by_work_sale_amt)) by_work_profit_rate
	,sum(by_work_sale_days) by_work_sale_days
	,sum(sy_work_sale_amt)/10000 sy_work_sale_amt
	,sum(sy_work_profit)/10000 sy_work_profit
	,sum(sy_work_profit)/abs(sum(sy_work_sale_amt)) sy_work_profit_rate
	,sum(sy_work_sale_days) sy_work_sale_days
	,sum(by_rp_work_sale_amt)/10000 by_rp_work_sale_amt
	,sum(by_rp_work_profit)/10000 by_rp_work_profit
	,sum(by_rp_work_profit)/abs(sum(by_rp_work_sale_amt)) by_rp_work_profit_rate
	,sum(by_rp_work_sale_days) by_rp_work_sale_days
	,sum(sy_rp_work_sale_amt)/10000 sy_rp_work_sale_amt
	,sum(sy_rp_work_profit)/10000 sy_rp_work_profit
	,sum(sy_rp_work_profit)/abs(sum(sy_rp_work_sale_amt)) sy_rp_work_profit_rate
	,sum(sy_rp_work_sale_days) sy_rp_work_sale_days 
  from csx_analyse.csx_analyse_fr_sale_r_m_customer_sales_avg_df 
  where month ='${month}'   --格式yyyymm '202401'
  group by 
	 customer_code
	,by_basic_days
	,sy_basic_days
	,by_work_days
	,sy_work_days
  )a
 left join
 (select customer_code	
    ,sum(sy_rp_sale_amt)/10000 sy_rp_sale_amt_all
	,sum(sy_rp_profit)/10000 sy_rp_profit_all
	,sum(sy_rp_profit)/abs(sum(sy_rp_sale_amt)) sy_rp_profit_rate_all
	,sum(sy_rp_sale_days)sy_rp_sale_days_all
  from csx_analyse.csx_analyse_fr_sale_r_m_lmall_customer_sales_avg_df 
  where month ='${month}' 
  group by customer_code
  )b on a.customer_code=b.customer_code  
left join
	(select * from csx_dim.csx_dim_crm_customer_info where sdt = 'current'	
	)d on d.customer_code=a.customer_code
left join csx_analyse.csx_analyse_fr_new_customer_classify_mf e on d.second_category_code = e.second_category_code	
left join 	---日配首次成交日期
	(select customer_code,business_type_code,first_business_sale_date
	 from csx_dws.csx_dws_crm_customer_business_active_di
     where sdt = 'current' and business_type_code=1
	)f on a.customer_code =f.customer_code
	;



========================================================同比
========================================================同比

-- 按照本期日期VS 同期日期 yyyymmdd
-- 需增加客户动态信息最近成交日期，判断断约、超过3个月没有履约客户过滤
select 
	a.performance_region_code,a.performance_region_name,
	a.performance_province_code,a.performance_province_name,
	a.performance_city_code,
	case when a.performance_city_name='宜宾' then '成都市'
		when a.performance_city_name in ('长寿区','石柱县')  then '重庆主城'
		when a.performance_province_name='东北' then '东北'
		when a.performance_city_name='阜阳市' then '合肥市'
		else a.performance_city_name end as performance_city_name,
	a.customer_code,
	d.customer_name,
	d.first_category_name,
	d.second_category_name,
	e.new_classify_name,
	d.sales_user_number,
	d.sales_user_name,
	d.supervisor_user_number,
	d.supervisor_user_name,	
	---大客户本月、上月业绩
	case when by_sale_amt<>0 then by_sale_amt end by_sale_amt,
	case when by_sale_amt<>0 then by_profit end by_profit,	
	case when by_sale_amt<>0 then by_sale_days end by_sale_days,
	case when sy_sale_amt<>0 then sy_sale_amt end sy_sale_amt,
	case when sy_sale_amt<>0 then sy_profit end sy_profit, 
	case when sy_sale_amt<>0 then sy_sale_days end sy_sale_days,
	---日配本月、上月业绩
	case when by_rp_sale_amt<>0 then by_rp_sale_amt end by_rp_sale_amt,
	case when by_rp_sale_amt<>0 then by_rp_profit end by_rp_profit,	
	case when by_rp_sale_amt<>0 then by_rp_sale_days end by_rp_sale_days,
	case when sy_rp_sale_amt<>0 then sy_rp_sale_amt end sy_rp_sale_amt,
	case when sy_rp_sale_amt<>0 then sy_rp_profit end sy_rp_profit, 
	case when sy_rp_sale_amt<>0 then sy_rp_sale_days end sy_rp_sale_days,
	last_business_sale_date,
	diff_days,
	case when last_business_sale_date is null or diff_days>90 then '断约'else '正常' end cust_status,
	sale_amt,
	profit,	
	sale_days,
	last_sale_amt,
	last_profit, 
	last_sale_days,
	rp_sale_amt,
	rp_profit,	
	rp_sale_days,
	last_rp_sale_amt,
	last_rp_profit, 
	last_rp_sale_days
from
	(select 
		regexp_replace(add_months(date_sub(current_date,1),0),'-','') as updatetime,
		performance_region_code,performance_region_name,
		performance_province_code,performance_province_name,
		performance_city_code,
		-- 按照旧城市
		performance_city_name,
		customer_code,	
		---大客户本月、上月业绩-自然日
		sum(case when sdt >= '${byc}' and sdt <= '${byz}' then sale_amt end) by_sale_amt,
		sum(case when sdt >= '${byc}' and sdt <= '${byz}' then profit end) by_profit,	
		count (distinct case when sdt >= '${byc}' and sdt <= '${byz}' then sdt end) by_sale_days,
		sum(case when sdt >= '${syc}' and sdt <= '${syz}' then sale_amt end) sy_sale_amt,
		sum(case when sdt >= '${syc}' and sdt <= '${syz}' then profit end) sy_profit, 
		count (distinct case when sdt >= '${syc}' and sdt <= '${syz}' then sdt end) sy_sale_days,
		---日配本月、上月业绩-自然日
		sum(case when sdt >= '${byc}' and sdt <= '${byz}' and business_type_code= 1 then sale_amt end) by_rp_sale_amt,
		sum(case when sdt >= '${byc}' and sdt <= '${byz}' and business_type_code= 1 then profit end) by_rp_profit,	
		count (distinct case when sdt >= '${byc}' and sdt <= '${byz}' and business_type_code= 1 then sdt end ) by_rp_sale_days,
		sum(case when sdt >= '${syc}' and sdt <= '${syz}' and business_type_code= 1 then sale_amt end) sy_rp_sale_amt,
		sum(case when sdt >= '${syc}' and sdt <= '${syz}' and business_type_code= 1 then profit end) sy_rp_profit, 
		count (distinct case when sdt >= '${syc}' and sdt <= '${syz}' and business_type_code= 1 then sdt end ) sy_rp_sale_days,
		-- 年至今
		sum(case when sdt >= '${y_byc}' and sdt <= '${byz}' then sale_amt end) sale_amt,
		sum(case when sdt >= '${y_byc}' and sdt <= '${byz}' then profit end)   profit,	
		count (distinct case when sdt >= '${y_byc}' and sdt <= '${byz}' then sdt end) sale_days,
		sum(case when sdt >= '${y_syc}' and sdt <= '${syz}' then sale_amt end) last_sale_amt,
		sum(case when sdt >= '${y_syc}' and sdt <= '${syz}' then profit end) last_profit, 
		count (distinct case when sdt >= '${y_syc}' and sdt <= '${syz}' then sdt end) last_sale_days,
		sum(case when sdt >= '${y_byc}' and sdt <= '${byz}' and business_type_code= 1 then sale_amt end) rp_sale_amt,
		sum(case when sdt >= '${y_byc}' and sdt <= '${byz}' and business_type_code= 1 then profit end) rp_profit,	
		count (distinct case when sdt >= '${y_byc}' and sdt <= '${byz}' and business_type_code= 1 then sdt end ) rp_sale_days,
		sum(case when sdt >= '${y_syc}' and sdt <= '${syz}' and business_type_code= 1 then sale_amt end) last_rp_sale_amt,
		sum(case when sdt >= '${y_syc}' and sdt <= '${syz}' and business_type_code= 1 then profit end) last_rp_profit, 
		count (distinct case when sdt >= '${y_syc}' and sdt <= '${syz}' and business_type_code= 1 then sdt end ) last_rp_sale_days
	 from 
		(
		 select ---每日业绩
			sdt,
			weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),0)) week, --周 常规周一-周日
			performance_region_code,performance_region_name,
			performance_province_code,performance_province_name,
			performance_city_code,performance_city_name,
			customer_code,
			business_type_code,business_type_name,	
			sum(sale_amt) sale_amt,
			sum(profit) profit
		 from csx_dws.csx_dws_sale_detail_di 
		 where ((sdt >= '${y_syc}' and sdt <= '${syz}') or (sdt >= '${y_byc}' and sdt <= '${byz}'))
			and channel_code in('1','7','9')
		 group by sdt,
			weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),0)),
			performance_region_code,performance_region_name,
			performance_province_code,performance_province_name,
			performance_city_code,performance_city_name,
			channel_code,channel_name,
			customer_code,
			business_type_code,business_type_name
		)a
		
	 group by 
		regexp_replace(add_months(date_sub(current_date,1),0),'-',''),  
		performance_region_code,performance_region_name,
		performance_province_code,performance_province_name,
		performance_city_code,performance_city_name,
		customer_code
	)a
left join
			(select *
			 from csx_dim.csx_dim_crm_customer_info
			 where sdt = 'current'	
			)d on d.customer_code=a.customer_code
		left join csx_analyse.csx_analyse_fr_new_customer_classify_mf e on d.second_category_code = e.second_category_code
left join 		
(select customer_code,business_type_code,first_business_sale_date,last_business_sale_date,
  datediff(date_sub(current_date(),1), from_unixtime(unix_timestamp(last_business_sale_date,'yyyyMMdd'),'yyyy-MM-dd')) diff_days	
	 from
	 csx_dws.csx_dws_crm_customer_business_active_di
     where sdt = 'current' and business_type_code=1
    ) f on a.customer_code =f.customer_code
	
	;
华南大区-华南大区年维度-日均业绩-B+BBC：
489.9万，同比上年：0.4%(上周:1%)—另24年：1.8%；
自营日配：289.7万，同比上年：6.1%(上周：6.8%)—另24年：11.5%；
日均成交客户数-B+BBC：565个，同比上年：15.4%；
自营日配：383个，同比上年：11.4%
华北大区-华北大区年维度-日均业绩-B+BBC：
470.4万，同比上年：16.6%(上周:16.1%)—另24年：30.2%；
自营日配：330.2万，同比上年：26.4%(上周：27.6%)—另24年：37.4%；
日均成交客户数-B+BBC：322个，同比上年：21.4%；
自营日配：226个，同比上年：17.8%
华西大区-华西大区年维度-日均业绩-B+BBC：
292.6万，同比上年：5.1%(上周:3.5%)—另24年：13.1%；
自营日配：225.4万，同比上年：19.9%(上周：18.8%)—另24年：17.9%；
日均成交客户数-B+BBC：557个，同比上年：-3.1%；
自营日配：453个，同比上年：-6.8%
华东大区-华中大区年维度-日均业绩-B+BBC：
483.6万，同比上年：0.8%(上周:0%)—另24年：-5.4%；
自营日配：302.1万，同比上年：-3%(上周：-4.8%)—另24年：-14.3%；
日均成交客户数-B+BBC：620个，同比上年：2.1%；
自营日配：482个，同比上年：3%
全国-全国年维度-日均业绩-B+BBC：
1736.6万，同比上年：5.3%(上周:4.8%)—另24年：7.7%；
自营日配：1147.4万，同比上年：11%(上周：10.6%)—另24年：9.5%；
日均成交客户数-B+BBC：2064个，同比上年：6.6%；
自营日配：1544个，同比上年：3.7%


华南大区-华南大区月维度-日均业绩-B+BBC：
456万，同比上年：5.2%(上周:10%)—另24年：1.8%；
自营日配：302.3万，同比上年：8.6%(上周：11.9%)—另24年：11.5%；
日均成交客户数-B+BBC：619个，同比上年：16.8%；
自营日配：426个，同比上年：10.6%
华北大区-华北大区月维度-日均业绩-B+BBC：
455.7万，同比上年：15.4%(上周:13%)—另24年：30.2%；
自营日配：343万，同比上年：15.7%(上周：16%)—另24年：37.4%；
日均成交客户数-B+BBC：332个，同比上年：21.3%；
自营日配：244个，同比上年：18.1%
华西大区-华西大区月维度-日均业绩-B+BBC：
297.2万，同比上年：10.5%(上周:6.4%)—另24年：13.1%；
自营日配：255.2万，同比上年：21.2%(上周：18.3%)—另24年：17.9%；
日均成交客户数-B+BBC：591个，同比上年：-6.3%；
自营日配：489个，同比上年：-9.1%
华东大区-华中大区月维度-日均业绩-B+BBC：
423.4万，同比上年：7.3%(上周:5.6%)—另24年：-5.4%；
自营日配：319.4万，同比上年：13.6%(上周：11.4%)—另24年：-14.3%；
日均成交客户数-B+BBC：678个，同比上年：2.3%；
自营日配：547个，同比上年：4.3%
全国-全国月维度-日均业绩-B+BBC：
1632.3万，同比上年：9.4%(上周:9%)—另24年：7.7%；
自营日配：1219.9万，同比上年：14.4%(上周：14.2%)—另24年：9.5%；
日均成交客户数-B+BBC：2220个，同比上年：5.9%；
自营日配：1706个，同比上年：3.1%

华南大区-华南大区月维度-日均业绩-B+BBC：
456万，环比上月：9.9%(上周:18.9%)
自营日配：302.3万，环比上月：22.3%(上周:38.9%)
日均成交客户数-B+BBC：619个，环比上月：10%；
自营日配：426个，环比上月：13.5%
华北大区-华北大区月维度-日均业绩-B+BBC：
455.7万，环比上月：7.2%(上周:16.6%)
自营日配：343万，环比上月：11.9%(上周:22.6%)
日均成交客户数-B+BBC：332个，环比上月：2.7%；
自营日配：244个，环比上月：11.1%
华西大区-华西大区月维度-日均业绩-B+BBC：
297.2万，环比上月：8%(上周:19%)
自营日配：255.2万，环比上月：12.9%(上周:22.4%)
日均成交客户数-B+BBC：591个，环比上月：7.3%；
自营日配：489个，环比上月：9.6%
华东大区-华东大区月维度-日均业绩-B+BBC：
423.4万，环比上月：4.4%(上周:17.6%)
自营日配：319.4万，环比上月：8.6%(上周:18.3%)
日均成交客户数-B+BBC：678个，环比上月：10.6%；
自营日配：547个，环比上月：15.2%
全国-全国月维度-日均业绩-B+BBC：
1632.3万，环比上月：7.3%(上周:17.9%)
自营日配：1219.9万，环比上月：13.6%(上周:25%)
日均成交客户数-B+BBC：2220个，环比上月：8.3%；
自营日配：1706个，环比上月：12.5%





-- ===业务类型同比
select 	

substr(sdt,1,4) month,
 
business_type_code,business_type_name,	
			sum(sale_amt)/10000 sale_amt,
			sum(profit)/10000 profit
 from csx_dws.csx_dws_sale_detail_di 
 where ((sdt >= '${syc}' and sdt <= '${syz}') or (sdt >= '${byc}' and sdt <= '${byz}'))
	and channel_code in('1','7','9')
	and performance_region_name like '%大区'
group by 
substr(sdt,1,4),business_type_code,business_type_name

;

-- 工作日统计
select substr(`date`,1,4), count(weekend_holiday_flag ),
		sum(if(weekend_holiday_flag=0,1,0)) as b
			 from csx_analyse.csx_analyse_date_weekend_holiday_yf
			 where(( `date` >= '20240101'
				and `date` <= '20241031'
			) or ( `date` >= '20250101'
				and `date` <= '20251031'
			)
			)
			-- and weekend_holiday_flag=0
			group by  substr(`date`,1,4)
			
;

select substr(`date`,1,6),count(`date`), 
		sum(if( weekend_holiday_flag=0 and substr(`date`,7,2) <='31' ,1,0)) as days,
		sum(if( weekend_holiday_flag=0  ,1,0)) all_days,
		sum(if( weekend_holiday_flag=0  ,1,0))-sum(if( weekend_holiday_flag=0 and substr(`date`,7,2) <='31' ,1,0))  diff_days
			 from csx_analyse.csx_analyse_date_weekend_holiday_yf
			 where(( `date` >= '20251001'
				and `date` <= '20251031'
			) or ( `date` >= '20240901'
				and `date` <= '20240930'
			))
			group by  substr(`date`,1,6)



select substr(`date`,1,6), count(weekend_holiday_flag )
			 from csx_analyse.csx_analyse_date_weekend_holiday_yf
			 where(( `date` >= '20240201'
				and `date` <= '20240229'
			) or ( `date` >= '20250201'
				and `date` <= '20250228'
			)
			)and weekend_holiday_flag=0
			group by  substr(`date`,1,6)
			
;


--业务同比

select 	

substr(sdt,1,4) month,
performance_region_code,
performance_region_name,
performance_province_code,
performance_province_name,
business_type_code,
business_type_name,	
customer_code,
customer_name,
second_category_name,
case when business_type_code=1 and shop_low_profit_flag=0 then '非直送' when business_type_code=1 and shop_low_profit_flag=1  then  '直送' end zs_type ,
sum(sale_amt)/10000 sale_amt,
sum(profit)/10000 profit
 from csx_dws.csx_dws_sale_detail_di a 
 left join 
(select shop_code,shop_low_profit_flag from csx_dim.csx_dim_shop where sdt='current') b on a.inventory_dc_code=b.shop_code
 where ((sdt >= '${syc}' and sdt <= '${syz}') or (sdt >= '${byc}' and sdt <= '${byz}'))
	and channel_code in('1','7','9')
	and performance_region_name like '%大区'
group by 
substr(sdt,1,4),case when business_type_code=1 and shop_low_profit_flag=0 then '非直送' when business_type_code=1 and shop_low_profit_flag=1  then  '直送' end 
,business_type_code,business_type_name,performance_region_code,
performance_region_name,
performance_province_code,
performance_province_name,
business_type_code,
customer_code,
customer_name,
second_category_name

;

-- TOP客户同比及环比分析 
CREATE table csx_analyse_tmp.csx_analyse_tmp_sale_r_m_customer_sales_avg_df_01 as
select 
	a.performance_region_code,a.performance_region_name,
	a.performance_province_code,a.performance_province_name,
	a.performance_city_code,a.performance_city_name,
	a.customer_code,d.customer_name,d.first_category_name,d.second_category_name,e.new_classify_name,
	by_basic_days,sy_basic_days,by_work_days,sy_work_days,
	---大客户本月、上月业绩
	case when by_sale_amt<>0 then by_sale_amt end by_sale_amt,
	case when by_sale_amt<>0 then by_profit end by_profit,	
	case when by_sale_amt<>0 then by_sale_days end by_sale_days,
	case when sy_sale_amt<>0 then sy_sale_amt end sy_sale_amt,
	case when sy_sale_amt<>0 then sy_profit end sy_profit, 
	case when sy_sale_amt<>0 then sy_sale_days end sy_sale_days,
	---日配本月、上月业绩
	case when by_rp_sale_amt<>0 then by_rp_sale_amt end by_rp_sale_amt,
	case when by_rp_sale_amt<>0 then by_rp_profit end by_rp_profit,	
	case when by_rp_sale_amt<>0 then by_rp_sale_days end by_rp_sale_days,
	case when sy_rp_sale_amt<>0 then sy_rp_sale_amt end sy_rp_sale_amt,
	case when sy_rp_sale_amt<>0 then sy_rp_profit end sy_rp_profit, 
	case when sy_rp_sale_amt<>0 then sy_rp_sale_days end sy_rp_sale_days,	
	---大客户本月、上月业绩-工作日
	case when by_work_sale_amt<>0 then by_work_sale_amt end by_work_sale_amt,
	case when by_work_sale_amt<>0 then by_work_profit end by_work_profit,	
	case when by_work_sale_amt<>0 then by_work_sale_days end by_work_sale_days,
	case when sy_work_sale_amt<>0 then sy_work_sale_amt end sy_work_sale_amt,
	case when sy_work_sale_amt<>0 then sy_work_profit end sy_work_profit, 
	case when sy_work_sale_amt<>0 then sy_work_sale_days end sy_work_sale_days,
	---日配本月、上月业绩-自然日
	case when by_rp_work_sale_amt<>0 then by_rp_work_sale_amt end by_rp_work_sale_amt,
	case when by_rp_work_sale_amt<>0 then by_rp_work_profit end by_rp_work_profit,	
	case when by_rp_work_sale_amt<>0 then by_rp_work_sale_days end by_rp_work_sale_days,
	case when sy_rp_work_sale_amt<>0 then sy_rp_work_sale_amt end sy_rp_work_sale_amt,
	case when sy_rp_work_sale_amt<>0 then sy_rp_work_profit end sy_rp_work_profit, 
	case when sy_rp_work_sale_amt<>0 then sy_rp_work_sale_days end sy_rp_work_sale_days,			
	concat(regexp_replace(trunc('${yesterday}','MM'),'-',''),'-',regexp_replace('${yesterday}','-','')) as byrang,
	concat(regexp_replace(add_months(trunc('${yesterday}','MM'),-12),'-',''),'-',regexp_replace(add_months('${yesterday}',-12),'-','')) as syrang,
	regexp_replace('${yesterday}','-','') as sdt,
	regexp_replace(substr('${yesterday}',1,7),'-','') as month
from
	( 
	 select 
		regexp_replace(add_months(date_sub(current_date,1),0),'-','') as updatetime,
		performance_region_code,performance_region_name,
		performance_province_code,performance_province_name,
		performance_city_code,performance_city_name,
		customer_code,	
		---大客户本月、上月业绩-自然日
		sum(case when sdt >= regexp_replace(trunc('${yesterday}','MM'),'-','') and sdt <= regexp_replace('${yesterday}','-','') then sale_amt end) by_sale_amt,
		sum(case when sdt >= regexp_replace(trunc('${yesterday}','MM'),'-','') and sdt <= regexp_replace('${yesterday}','-','') then profit end) by_profit,	
		count (distinct case when sdt >= regexp_replace(trunc('${yesterday}','MM'),'-','') and sdt <= regexp_replace('${yesterday}','-','') then sdt end) by_sale_days,
		sum(case when sdt >= regexp_replace(add_months(trunc('${yesterday}','MM'),-12),'-','') and sdt <= regexp_replace(add_months('${yesterday}',-12),'-','') then sale_amt end) sy_sale_amt,
		sum(case when sdt >= regexp_replace(add_months(trunc('${yesterday}','MM'),-12),'-','') and sdt <= regexp_replace(add_months('${yesterday}',-12),'-','') then profit end) sy_profit, 
		count (distinct case when sdt >= regexp_replace(add_months(trunc('${yesterday}','MM'),-12),'-','') and sdt <= regexp_replace(add_months('${yesterday}',-12),'-','') then sdt end) sy_sale_days,
		---日配本月、上月业绩-自然日
		sum(case when sdt >= regexp_replace(trunc('${yesterday}','MM'),'-','') and sdt <= regexp_replace('${yesterday}','-','') and business_type_code= 1 then sale_amt end) by_rp_sale_amt,
		sum(case when sdt >= regexp_replace(trunc('${yesterday}','MM'),'-','') and sdt <= regexp_replace('${yesterday}','-','') and business_type_code= 1 then profit end) by_rp_profit,	
		count (distinct case when sdt >= regexp_replace(trunc('${yesterday}','MM'),'-','') and sdt <= regexp_replace('${yesterday}','-','') and business_type_code= 1 then sdt end ) by_rp_sale_days,
		sum(case when sdt >= regexp_replace(add_months(trunc('${yesterday}','MM'),-12),'-','') and sdt <= regexp_replace(add_months('${yesterday}',-12),'-','') and business_type_code= 1 then sale_amt end) sy_rp_sale_amt,
		sum(case when sdt >= regexp_replace(add_months(trunc('${yesterday}','MM'),-12),'-','') and sdt <= regexp_replace(add_months('${yesterday}',-12),'-','') and business_type_code= 1 then profit end) sy_rp_profit, 
		count (distinct case when sdt >= regexp_replace(add_months(trunc('${yesterday}','MM'),-12),'-','') and sdt <= regexp_replace(add_months('${yesterday}',-12),'-','') and business_type_code= 1 then sdt end ) sy_rp_sale_days,
		
		---大客户本月、上月业绩-工作日
		sum(case when weekend_holiday_flag=0 and sdt >= regexp_replace(trunc('${yesterday}','MM'),'-','') and sdt <= regexp_replace('${yesterday}','-','') then sale_amt end) by_work_sale_amt,
		sum(case when weekend_holiday_flag=0 and sdt >= regexp_replace(trunc('${yesterday}','MM'),'-','') and sdt <= regexp_replace('${yesterday}','-','') then profit end) by_work_profit,	
		count (distinct case when weekend_holiday_flag=0 and sdt >= regexp_replace(trunc('${yesterday}','MM'),'-','') and sdt <= regexp_replace('${yesterday}','-','') then sdt end) by_work_sale_days,
		sum(case when weekend_holiday_flag=0 and sdt >= regexp_replace(add_months(trunc('${yesterday}','MM'),-12),'-','') and sdt <= regexp_replace(add_months('${yesterday}',-12),'-','') then sale_amt end) sy_work_sale_amt,
		sum(case when weekend_holiday_flag=0 and sdt >= regexp_replace(add_months(trunc('${yesterday}','MM'),-12),'-','') and sdt <= regexp_replace(add_months('${yesterday}',-12),'-','') then profit end) sy_work_profit, 
		count (distinct case when weekend_holiday_flag=0 and sdt >= regexp_replace(add_months(trunc('${yesterday}','MM'),-12),'-','') and sdt <= regexp_replace(add_months('${yesterday}',-12),'-','') then sdt end) sy_work_sale_days,
		---日配本月、上月业绩-工作日
		sum(case when weekend_holiday_flag=0 and sdt >= regexp_replace(trunc('${yesterday}','MM'),'-','') and sdt <= regexp_replace('${yesterday}','-','') and business_type_code= 1 then sale_amt end) by_rp_work_sale_amt,
		sum(case when weekend_holiday_flag=0 and sdt >= regexp_replace(trunc('${yesterday}','MM'),'-','') and sdt <= regexp_replace('${yesterday}','-','') and business_type_code= 1 then profit end) by_rp_work_profit,	
		count (distinct case when weekend_holiday_flag=0 and sdt >= regexp_replace(trunc('${yesterday}','MM'),'-','') and sdt <= regexp_replace('${yesterday}','-','') and business_type_code= 1 then sdt end ) by_rp_work_sale_days,
		sum(case when weekend_holiday_flag=0 and sdt >= regexp_replace(add_months(trunc('${yesterday}','MM'),-12),'-','') and sdt <= regexp_replace(add_months('${yesterday}',-12),'-','') and business_type_code= 1 then sale_amt end) sy_rp_work_sale_amt,
		sum(case when weekend_holiday_flag=0 and sdt >= regexp_replace(add_months(trunc('${yesterday}','MM'),-12),'-','') and sdt <= regexp_replace(add_months('${yesterday}',-12),'-','') and business_type_code= 1 then profit end) sy_rp_work_profit, 
		count (distinct case when weekend_holiday_flag=0 and sdt >= regexp_replace(add_months(trunc('${yesterday}','MM'),-12),'-','') and sdt <= regexp_replace(add_months('${yesterday}',-12),'-','') and business_type_code= 1 then sdt end ) sy_rp_work_sale_days	
	 from 
		(
		 select ---每日业绩
			sdt,
			weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),0)) week, --周 常规周一-周日
			performance_region_code,performance_region_name,
			performance_province_code,performance_province_name,
			performance_city_code,performance_city_name,
			customer_code,
			business_type_code,business_type_name,	
			sum(sale_amt) sale_amt,
			sum(profit) profit
		 from csx_dws.csx_dws_sale_detail_di 
		 where sdt >= regexp_replace(add_months(trunc('${yesterday}','MM'),-12),'-','') and sdt <= regexp_replace('${yesterday}','-','')
			and channel_code in('1','7','9') 
			and shipper_code='YHCSX'
-- 			and business_type_code=1
		 group by sdt,
			weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),0)),
			performance_region_code,performance_region_name,
			performance_province_code,performance_province_name,
			performance_city_code,performance_city_name,
			channel_code,channel_name,
			customer_code,
			business_type_code,
			business_type_name
		)a
		left join  ---日历-是否含周末与节假日，补班为工作日
			(select `date`,weekend_holiday_flag 
			 from csx_analyse.csx_analyse_date_weekend_holiday_yf
			 where `date` >= regexp_replace(add_months(trunc('${yesterday}','MM'),-12),'-','') 
				and `date` <= regexp_replace('${yesterday}','-','')
			)b on b.`date`=a.sdt	
	 group by 
		regexp_replace(add_months(date_sub(current_date,1),0),'-',''),  
		performance_region_code,performance_region_name,
		performance_province_code,performance_province_name,
		performance_city_code,performance_city_name,
		customer_code
	)a
 left join 
	(
	 select 
		regexp_replace(add_months(date_sub(current_date,1),0),'-','') as updatetime,
		count(case when `date` >= regexp_replace(trunc('${yesterday}','MM'),'-','') and `date` <= regexp_replace('${yesterday}','-','') then `date` end) by_basic_days,
		count(case when `date` >= regexp_replace(add_months(trunc('${yesterday}','MM'),-12),'-','') and `date` <= regexp_replace(add_months('${yesterday}',-12),'-','') then `date` end) sy_basic_days,
		count(case when weekend_holiday_flag=0 and `date` >= regexp_replace(trunc('${yesterday}','MM'),'-','') and `date` <= regexp_replace('${yesterday}','-','') then `date` end) by_work_days,
		count(case when weekend_holiday_flag=0 and `date` >= regexp_replace(add_months(trunc('${yesterday}','MM'),-12),'-','') and `date` <= regexp_replace(add_months('${yesterday}',-12),'-','') then `date` end) sy_work_days
	 from csx_analyse.csx_analyse_date_weekend_holiday_yf
	 where `date` >= regexp_replace(add_months(trunc('${yesterday}','MM'),-12),'-','') and `date` <= regexp_replace('${yesterday}','-','') 
	 group by regexp_replace(add_months(date_sub(current_date,1),0),'-','')
	)c on a.updatetime = c.updatetime
left join
	(
	select *
	from csx_dim.csx_dim_crm_customer_info
	where sdt = 'current' 
	and shipper_code='YHCSX'
	)d on d.customer_code=a.customer_code
left join csx_analyse.csx_analyse_fr_new_customer_classify_mf e on d.second_category_code = e.second_category_code	
where by_sale_amt<>0 or sy_sale_amt<>0;


select *,
dense_rank()over(partition by performance_province_name  order by coalesce(by_rp_sale_amt,0) desc  ) as by_rn,
dense_rank()over(partition by performance_province_name  order by coalesce(sy_rp_sale_amt,0) desc  ) as sy_rn

from csx_analyse_tmp.csx_analyse_tmp_sale_r_m_customer_sales_avg_df_01
where by_rp_sale_amt is not null or  sy_rp_sale_amt is not null 