drop table if exists csx_analyse_tmp.tmp01_ghm; 
create table if not exists csx_analyse_tmp.tmp01_ghm as 
select 
	a.performance_region_name,
	a.performance_province_name,
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
	by_sale_amt,
	by_profit,	
	by_profit/abs(by_sale_amt) as by_profit_rate,	
	by_sale_days,
	sy_sale_amt,
	sy_profit, 
	sy_profit/abs(sy_sale_amt)  as sy_profit_rate,
	sy_sale_days,
	by_sale_amt-sy_sale_amt as diff_sale_amt,
	
	---日配本月、上月业绩
	by_rp_sale_amt,
	by_rp_profit,	
	by_rp_profit/abs(by_rp_sale_amt)  as by_rp_profit_rate,
	by_rp_sale_days ,
	sy_rp_sale_amt,
	sy_rp_profit, 
	sy_rp_profit/abs(sy_rp_sale_amt)  as sy_rp_profit_rate,
	sy_rp_sale_days,
	by_rp_sale_amt - sy_rp_sale_amt as diff_rp_sale_amt,

	diff_days,
	case when last_business_sale_date is null or diff_days>90 then '断约'else '正常' end cust_status,

	-- 日配非直送
	by_rp_no_dirt_sale_amt,
	by_rp_no_dirt_profit,	
	by_rp_no_dirt_profit/abs(by_rp_no_dirt_sale_amt)  as by_rp_no_dirt_profit_rate,
	by_rp_no_dirt_sale_days,
	
	sy_rp_no_dirt_sale_amt,
	sy_rp_no_dirt_profit,
	sy_rp_no_dirt_profit/abs(sy_rp_no_dirt_sale_amt)  as sy_rp_no_dirt_profit_rate,
	sy_rp_no_dirt_sale_days,
	by_rp_no_dirt_sale_amt-sy_rp_no_dirt_sale_amt as diff_rp_no_dirt_sale_amt,

	
	-- 年累计：B+BBC
	sale_amt,
	profit,	
	profit/abs(sale_amt) as profit_rate,	
	sale_days,
		
	last_sale_amt,
	last_profit, 
	last_profit/abs(last_sale_amt) as last_profit_rate,
	last_sale_days,
	sale_amt - last_sale_amt as diff_year_sale_amt,
	
	
	-- 年累计：日配
	rp_sale_amt,
	rp_profit,
	rp_profit/abs(rp_sale_amt) as rp_profit_rate,
	rp_sale_days,
	
	last_rp_sale_amt,
	last_rp_profit, 
	last_rp_profit/abs(last_rp_sale_amt) as last_rp_profit_rate,
	last_rp_sale_days,
	
	rp_sale_amt-last_rp_sale_amt as diff_year_rp_sale_amt,
	
	rp_no_dirt_sale_amt,
	rp_no_dirt_profit,	
	rp_no_dirt_profit/abs(rp_no_dirt_sale_amt) as rp_no_dirt_profit_rate,
	rp_sale_no_dirt_days,
	
	last_rp_no_dirt_sale_amt,
	last_rp_no_dirt_profit, 
	last_rp_no_dirt_profit/abs(last_rp_no_dirt_sale_amt) as last_rp_no_dirt_profit_rate,	
	last_rp_no_dirt_sale_days,
	
	rp_no_dirt_sale_amt-last_rp_no_dirt_sale_amt as diff_year_rp_no_dirt_sale_amt
from
	(select 
		regexp_replace(add_months(date_sub(current_date,1),0),'-','') as updatetime,
		performance_region_code,
		performance_region_name,
		performance_province_code,
		performance_province_name,
		performance_city_code,
		performance_city_name,
		customer_code,	
		-- 月同比：B+BBC
		sum(case when sdt >= '${byc}' and sdt <= '${byz}' then sale_amt end) by_sale_amt,
		sum(case when sdt >= '${byc}' and sdt <= '${byz}' then profit end) by_profit,	
		count (distinct case when sdt >= '${byc}' and sdt <= '${byz}' then sdt end) by_sale_days,
		sum(case when sdt >= '${syc}' and sdt <= '${syz}' then sale_amt end) sy_sale_amt,
		sum(case when sdt >= '${syc}' and sdt <= '${syz}' then profit end) sy_profit, 
		count (distinct case when sdt >= '${syc}' and sdt <= '${syz}' then sdt end) sy_sale_days,
		
		-- 月同比：日配
		sum(case when sdt >= '${byc}' and sdt <= '${byz}' and business_type_code= 1 then sale_amt end) by_rp_sale_amt,
		sum(case when sdt >= '${byc}' and sdt <= '${byz}' and business_type_code= 1 then profit end) by_rp_profit,	
		count (distinct case when sdt >= '${byc}' and sdt <= '${byz}' and business_type_code= 1 then sdt end ) by_rp_sale_days,
		sum(case when sdt >= '${syc}' and sdt <= '${syz}' and business_type_code= 1 then sale_amt end) sy_rp_sale_amt,
		sum(case when sdt >= '${syc}' and sdt <= '${syz}' and business_type_code= 1 then profit end) sy_rp_profit, 
		count (distinct case when sdt >= '${syc}' and sdt <= '${syz}' and business_type_code= 1 then sdt end ) sy_rp_sale_days,			
		
        --- 月同比：日配主仓
		sum(case when sdt >= '${byc}' and sdt <= '${byz}' and business_type_code= 1 and  partner_type_code not in (1,3) then sale_amt end) by_rp_no_dirt_sale_amt,
		sum(case when sdt >= '${byc}' and sdt <= '${byz}' and business_type_code= 1 and  partner_type_code not in (1,3) then profit end) by_rp_no_dirt_profit,	
		count (distinct case when sdt >= '${byc}' and sdt <= '${byz}' and business_type_code= 1 and  partner_type_code not in (1,3) then sdt end ) by_rp_no_dirt_sale_days,
		sum(case when sdt >= '${syc}' and sdt <= '${syz}' and business_type_code= 1 and  partner_type_code not in (1,3) then sale_amt end) sy_rp_no_dirt_sale_amt,
		sum(case when sdt >= '${syc}' and sdt <= '${syz}' and business_type_code= 1 and  partner_type_code not in (1,3) then profit end) sy_rp_no_dirt_profit, 
		count (distinct case when sdt >= '${syc}' and sdt <= '${syz}' and business_type_code= 1 and  partner_type_code not in (1,3) then sdt end ) sy_rp_no_dirt_sale_days,
		
		-- 年同比：B+BBC
		sum(case when sdt >= '${y_byc}' and sdt <= '${byz}' then sale_amt end) sale_amt,
		sum(case when sdt >= '${y_byc}' and sdt <= '${byz}' then profit end)   profit,	
		count (distinct case when sdt >= '${y_byc}' and sdt <= '${byz}' then sdt end) sale_days,
		sum(case when sdt >= '${y_syc}' and sdt <= '${syz}' then sale_amt end) last_sale_amt,
		sum(case when sdt >= '${y_syc}' and sdt <= '${syz}' then profit end) last_profit, 
		count (distinct case when sdt >= '${y_syc}' and sdt <= '${syz}' then sdt end) last_sale_days,
		
		-- 年同比：日配
		sum(case when sdt >= '${y_byc}' and sdt <= '${byz}' and business_type_code= 1 then sale_amt end) rp_sale_amt,
		sum(case when sdt >= '${y_byc}' and sdt <= '${byz}' and business_type_code= 1 then profit end) rp_profit,	
		count (distinct case when sdt >= '${y_byc}' and sdt <= '${byz}' and business_type_code= 1 then sdt end ) rp_sale_days,
		sum(case when sdt >= '${y_syc}' and sdt <= '${syz}' and business_type_code= 1 then sale_amt end) last_rp_sale_amt,
		sum(case when sdt >= '${y_syc}' and sdt <= '${syz}' and business_type_code= 1 then profit end) last_rp_profit, 
		count (distinct case when sdt >= '${y_syc}' and sdt <= '${syz}' and business_type_code= 1 then sdt end ) last_rp_sale_days,
		
		-- 年同比：日配主仓
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
			case when  c.order_code is not null then new_business_type_code 
		        else a.business_type_code
		        end business_type_code,
            sum(sale_amt)/10000 sale_amt,
			sum(profit)/10000 profit
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
			and channel_code in ('1','7','9')
			and performance_region_name like '%大区'
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
    ) f on a.customer_code =f.customer_code;
    
    
    
select * from csx_analyse_tmp.tmp01_ghm