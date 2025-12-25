-- 月环比业绩：
select 
	performance_region_name
	,performance_province_name
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
	,by_sale_amt/10000 as  by_sale_amt
	,by_profit/10000 as by_profit
	,by_sale_days
	,sy_sale_amt/10000 as sy_sale_amt
	,sy_profit/10000 as sy_profit
	,sy_sale_days
	,by_rp_sale_amt/10000 as by_rp_sale_amt
	,by_rp_profit/10000 as by_rp_profit
	,by_rp_sale_days
	,sy_rp_sale_amt/10000 as sy_rp_sale_amt
	,sy_rp_profit/10000 as sy_rp_profit
	,sy_rp_sale_days
	,by_work_sale_amt/10000 as by_work_sale_amt
	,by_work_profit/10000 as by_work_profit
	,by_work_sale_days
	,sy_work_sale_amt/10000 as sy_work_sale_amt
	,sy_work_profit/10000 as sy_work_profit
	,sy_work_sale_days
	,by_rp_work_sale_amt/10000 as by_rp_work_sale_amt
	,by_rp_work_profit/10000 as by_rp_work_profit
	,by_rp_work_sale_days
	,sy_rp_work_sale_amt/10000 as sy_rp_work_sale_amt
	,sy_rp_work_profit/10000 as sy_rp_work_profit
	,sy_rp_work_sale_days
	,byrang
	,syrang
	,sdt
	,month
from csx_analyse.csx_analyse_fr_sale_r_m_customer_sales_avg_df
where month ='202512'
;



--月环比客户
select 
	d.performance_region_name,
	d.performance_province_name,
	d.performance_city_name,
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

	
-- 工作日统计


-- 年同比工作日统计			
SELECT 
    SUBSTR(`date`, 1, 4) as year,
    COUNT(weekend_holiday_flag) as total_records,
    SUM(IF(weekend_holiday_flag = 0, 1, 0)) AS b
FROM 
    csx_analyse.csx_analyse_date_weekend_holiday_yf
WHERE 
    ((`date` >= '20240101' AND `date` <= '20241214')
     OR (`date` >= '20250101' AND `date` <= '20251214'))
GROUP BY 
    SUBSTR(`date`, 1, 4)
ORDER BY 
    year;



-- 月同比工作日统计
SELECT 
    SUBSTR(`date`, 1, 6) as month_year,
    COUNT(`date`) as total_days,
    SUM(IF(weekend_holiday_flag = 0 AND SUBSTR(`date`, 7, 2) <= '14', 1, 0)) AS days,
    SUM(IF(weekend_holiday_flag = 0, 1, 0)) AS all_days,
    SUM(IF(weekend_holiday_flag = 0, 1, 0)) - SUM(IF(weekend_holiday_flag = 0 AND SUBSTR(`date`, 7, 2) <= '14', 1, 0)) AS diff_days
FROM 
    csx_analyse.csx_analyse_date_weekend_holiday_yf
WHERE 
    ((`date` >= '20251201' AND `date` <= '20251231')
     OR (`date` >= '20241201' AND `date` <= '20241231'))
GROUP BY 
    SUBSTR(`date`, 1, 6)
ORDER BY 
    month_year, 
    total_days;




-- 月环比工作日统计
SELECT 
    SUBSTR(`date`, 1, 6) as month_year,
    COUNT(`date`) as total_days,
    SUM(IF(weekend_holiday_flag = 0 AND SUBSTR(`date`, 7, 2) <= '14', 1, 0)) AS days,
    SUM(IF(weekend_holiday_flag = 0, 1, 0)) AS all_days,
    SUM(IF(weekend_holiday_flag = 0, 1, 0)) - SUM(IF(weekend_holiday_flag = 0 AND SUBSTR(`date`, 7, 2) <= '14', 1, 0)) AS diff_days
FROM 
    csx_analyse.csx_analyse_date_weekend_holiday_yf
WHERE 
    ((`date` >= '20251201' AND `date` <= '20251231')
     OR (`date` >= '20251101' AND `date` <= '20251130'))
GROUP BY 
    SUBSTR(`date`, 1, 6)
ORDER BY 
    month_year, 
    total_days;
