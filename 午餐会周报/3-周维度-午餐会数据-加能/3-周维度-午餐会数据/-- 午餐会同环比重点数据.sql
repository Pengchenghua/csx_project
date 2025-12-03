-- 午餐会重点数据
select performance_region_code
,performance_region_name
,performance_province_code
,performance_province_name
,performance_city_code
,performance_city_name
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
where month ='202511'
;





--环比客户
select 
	d.performance_region_code,
	d.performance_region_name,
	d.performance_province_code,
	d.performance_province_name,
	d.performance_city_code,
	performance_city_name,
	a.customer_code,
	if(d.second_category_name='部队', regexp_replace(
        d.customer_name, 
        '(\\d)(\\d*)', 
        concat('$1', repeat('**', length('$2')))
    ),d.customer_name) new_customer_name ,
    d.customer_name ,
	d.first_category_name,
	d.second_category_name,
	e.new_classify_name,
	f.first_business_sale_date,
	f.last_business_sale_date,
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
	(select customer_code,business_type_code,first_business_sale_date,last_business_sale_date
	 from csx_dws.csx_dws_crm_customer_business_active_di
     where sdt = 'current' and business_type_code=1
	)f on a.customer_code =f.customer_code
	;





-- 按照本期日期VS 同期日期 yyyymmdd
-- 需增加客户动态信息最近成交日期，判断断约、超过3个月没有履约客户过滤
select 
	a.performance_region_code,a.performance_region_name,
	a.performance_province_code,a.performance_province_name,
	a.performance_city_code,
-- 	case when a.performance_city_name='宜宾' then '成都市'
-- 		when a.performance_city_name in ('长寿区','石柱县')  then '重庆主城'
-- 		when a.performance_province_name='东北' then '东北'
-- 		when a.performance_city_name='阜阳市' then '合肥市'
-- 		else a.performance_city_name end as performance_city_name,
    a.performance_city_name,
	a.customer_code,
	if(d.second_category_name='部队', regexp_replace(
        d.customer_name, 
        '(\\d)(\\d*)', 
        concat('$1', repeat('**', length('$2')))
    ),d.customer_name) new_customer_name,
    d.customer_name,
	d.first_category_name,
	d.second_category_name,
	e.new_classify_name,
	first_business_sale_date,
		last_business_sale_date,

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
	case when by_rp_sale_amt<>0 then by_rp_profit end/	case when by_rp_sale_amt<>0 then by_rp_sale_amt end as by_rp_profit_rate,
	case when by_rp_sale_amt<>0 then by_rp_sale_days end by_rp_sale_days,
	case when sy_rp_sale_amt<>0 then sy_rp_sale_amt end sy_rp_sale_amt,
	case when sy_rp_sale_amt<>0 then sy_rp_profit end sy_rp_profit, 
	case when sy_rp_sale_amt<>0 then sy_rp_profit end/	case when sy_rp_sale_amt<>0 then sy_rp_sale_amt end as sy_rp_profit_rate,
	case when sy_rp_sale_amt<>0 then sy_rp_sale_days end sy_rp_sale_days,
	coalesce(by_rp_sale_amt,0)- coalesce(sy_rp_sale_amt,0)    as diff_rp_sale_amt,

	diff_days,
	case when last_business_sale_date is null or diff_days>90 then '断约'else '正常' end cust_status,
	by_rp_work_sale_amt,
	by_rp_work_profit,
	by_rp_work_profit/by_rp_work_sale_amt as by_rp_work_profit_rate,
	by_rp_work_sale_days,
	sy_rp_work_sale_amt,
	sy_rp_work_profit, 
	sy_rp_work_profit/sy_rp_work_sale_amt as sy_rp_work_profit_rate,
	sy_rp_work_sale_days,
	row_number()over(partition by a.performance_province_name order by  coalesce(by_rp_work_sale_amt,0) desc ) as work_rn,
	row_number()over(partition by a.performance_province_name order by  coalesce(by_rp_sale_amt,0) desc ) as rp_rn,
	-- 日配非直送
	by_rp_no_dirt_sale_amt,
	by_rp_no_dirt_profit,	
	by_rp_no_dirt_profit/ by_rp_no_dirt_sale_amt as by_rp_no_dirt_profit_rate,
	by_rp_no_dirt_sale_days,
	sy_rp_no_dirt_sale_amt,
	sy_rp_no_dirt_profit,
	sy_rp_no_dirt_profit/sy_rp_no_dirt_sale_amt as sy_rp_no_dirt_profit_rate,
	sy_rp_no_dirt_sale_days,
	by_rp_no_dirt_sale_amt- sy_rp_no_dirt_sale_amt diff_rp_no_dirt_sale_amt,
		---日配本月、上月业绩-工作日 剔除直送仓
	by_rp_no_dirt_work_sale_amt,
	by_rp_no_dirt_work_profit,	
	by_rp_no_dirt_work_sale_days,
	sy_rp_no_dirt_work_sale_amt,
	sy_rp_no_dirt_work_profit, 
	sy_rp_no_dirt_work_sale_days,
	
	-- 年累计
	sale_amt,
	profit,	
	sale_days,
	last_sale_amt,
	last_profit, 
	last_sale_days,
	rp_sale_amt,
	rp_profit,
	rp_profit/rp_sale_amt as rp_profit_rate,
	rp_sale_days,
	last_rp_sale_amt,
	last_rp_profit, 
	last_rp_profit/last_rp_sale_amt last_rp_profit_rate,
	last_rp_sale_days,
	rp_no_dirt_sale_amt,
	rp_no_dirt_profit,	
	rp_sale_no_dirt_days,
	last_rp_no_dirt_sale_amt,
	last_rp_no_dirt_profit, 
	last_rp_no_dirt_sale_days
from
	(select 
		regexp_replace(add_months(date_sub(current_date,1),0),'-','') as updatetime,
		performance_region_code,
		performance_region_name,
		performance_province_code,
		performance_province_name,
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
		

		
		---日配本月、上月业绩-工作日
		sum(case when weekend_holiday_flag=0 and sdt >= '${byc}' and sdt <= '${byz}' and business_type_code= 1 then sale_amt end) by_rp_work_sale_amt,
		sum(case when weekend_holiday_flag=0 and sdt >= '${byc}' and sdt <= '${byz}' and business_type_code= 1 then profit end) by_rp_work_profit,	
		count (distinct case when weekend_holiday_flag=0 and sdt >= '${byc}' and sdt <= '${byz}' and business_type_code= 1 then sdt end ) by_rp_work_sale_days,
		sum(case when weekend_holiday_flag=0 and sdt >= '${syc}' and sdt <= '${syz}' and business_type_code= 1 then sale_amt end) sy_rp_work_sale_amt,
		sum(case when weekend_holiday_flag=0 and sdt >= '${syc}' and sdt <= '${syz}'  and business_type_code= 1 then profit end) sy_rp_work_profit, 
		count (distinct case when weekend_holiday_flag=0 and  sdt >= '${syc}' and sdt <= '${syz}'  and business_type_code= 1 then sdt end ) sy_rp_work_sale_days,	 
        ---日配本月、上月业绩-自然日 剔除直送仓
		sum(case when sdt >= '${byc}' and sdt <= '${byz}' and business_type_code= 1 and  partner_type_code not in (1,3) then sale_amt end) by_rp_no_dirt_sale_amt,
		sum(case when sdt >= '${byc}' and sdt <= '${byz}' and business_type_code= 1 and  partner_type_code not in (1,3) then profit end) by_rp_no_dirt_profit,	
		count (distinct case when sdt >= '${byc}' and sdt <= '${byz}' and business_type_code= 1 and  partner_type_code not in (1,3) then sdt end ) by_rp_no_dirt_sale_days,
		sum(case when sdt >= '${syc}' and sdt <= '${syz}' and business_type_code= 1 and  partner_type_code not in (1,3) then sale_amt end) sy_rp_no_dirt_sale_amt,
		sum(case when sdt >= '${syc}' and sdt <= '${syz}' and business_type_code= 1 and  partner_type_code not in (1,3) then profit end) sy_rp_no_dirt_profit, 
		count (distinct case when sdt >= '${syc}' and sdt <= '${syz}' and business_type_code= 1 and  partner_type_code not in (1,3) then sdt end ) sy_rp_no_dirt_sale_days,
		
		---日配本月、上月业绩-工作日 剔除直送仓
		sum(case when weekend_holiday_flag=0 and  partner_type_code not in (1,3) and sdt >= '${byc}' and sdt <= '${byz}' and business_type_code= 1 then sale_amt end) by_rp_no_dirt_work_sale_amt,
		sum(case when weekend_holiday_flag=0 and  partner_type_code not in (1,3) and sdt >= '${byc}' and sdt <= '${byz}' and business_type_code= 1 then profit end) by_rp_no_dirt_work_profit,	
		count (distinct case when weekend_holiday_flag=0 and  partner_type_code not in (1,3) and sdt >= '${byc}' and sdt <= '${byz}' and business_type_code= 1 then sdt end ) by_rp_no_dirt_work_sale_days,
		sum(case when weekend_holiday_flag=0 and  partner_type_code not in (1,3) and sdt >= '${syc}' and sdt <= '${syz}' and business_type_code= 1 then sale_amt end) sy_rp_no_dirt_work_sale_amt,
		sum(case when weekend_holiday_flag=0 and  partner_type_code not in (1,3) and sdt >= '${syc}' and sdt <= '${syz}'  and business_type_code= 1 then profit end) sy_rp_no_dirt_work_profit, 
		count (distinct case when weekend_holiday_flag=0 and  partner_type_code not in (1,3) and  sdt >= '${syc}' and sdt <= '${syz}'  and business_type_code= 1 then sdt end ) sy_rp_no_dirt_work_sale_days,	
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
		count (distinct case when sdt >= '${y_syc}' and sdt <= '${syz}' and business_type_code= 1 then sdt end ) last_rp_sale_days,
		sum(case when sdt >= '${y_byc}' and sdt <= '${byz}' and business_type_code= 1 and  partner_type_code not in (1,3)  then sale_amt end) rp_no_dirt_sale_amt,
		sum(case when sdt >= '${y_byc}' and sdt <= '${byz}' and business_type_code= 1 and  partner_type_code not in (1,3)  then profit end) rp_no_dirt_profit,	
		count (distinct case when sdt >= '${y_byc}' and sdt <= '${byz}' and business_type_code= 1 and  partner_type_code not in (1,3)  then sdt end ) rp_sale_no_dirt_days,
		sum(case when sdt >= '${y_syc}' and sdt <= '${syz}' and business_type_code= 1 and  partner_type_code not in (1,3)  then sale_amt end) last_rp_no_dirt_sale_amt,
		sum(case when sdt >= '${y_syc}' and sdt <= '${syz}' and business_type_code= 1 and  partner_type_code not in (1,3)  then profit end) last_rp_no_dirt_profit, 
		count (distinct case when sdt >= '${y_syc}' and sdt <= '${syz}' and business_type_code= 1 and  partner_type_code not in (1,3)  then sdt end ) last_rp_no_dirt_sale_days
	 from 
		(
		 select ---每日业绩
			sdt,
			weekend_holiday_flag,
			weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),0)) week, --周 常规周一-周日
			a.performance_region_code,
			a.performance_region_name,
			a.performance_province_code,
			a.performance_province_name,
			a.performance_city_code,
			a.performance_city_name,
			a.customer_code,
			partner_type_code,
			case   when  c.order_code is not null then new_business_type_code 
		        else a.business_type_code
		        end business_type_code,
            sum(sale_amt) sale_amt,
			sum(profit) profit
		 from csx_dws.csx_dws_sale_detail_di a 
        left join 		
        (select
          customer_code ,
          order_code ,
          business_type_code,
          business_type_name,
          new_business_type_code ,
          new_business_type_name 
        from
          csx_report.csx_report_sale_fujian_prison_business_type_adjust_df
        ) c on a.order_code=c.order_code 
        		 left join  ---日历-是否含周末与节假日，补班为工作日
			(select `date`,weekend_holiday_flag 
			 from csx_analyse.csx_analyse_date_weekend_holiday_yf
			 where `date` >= '${y_syc}'
				and `date` <=  '${byz}'
			)b on b.`date`=a.sdt	
		 
		 where ((sdt >= '${y_syc}' and sdt <= '${syz}') or (sdt >= '${y_byc}' and sdt <= '${byz}'))
			and channel_code in('1','7','9')
		 group by sdt,
			weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),0)),
			a.performance_region_code,
			a.performance_region_name,
			a.performance_province_code,
			a.performance_province_name,
			a.performance_city_code,
			a.performance_city_name,
			a.channel_code,
			a.channel_name,
			a.customer_code,
			weekend_holiday_flag,
			partner_type_code ,
			case   when  c.order_code is not null then new_business_type_code 
		        else a.business_type_code
		        end
			
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