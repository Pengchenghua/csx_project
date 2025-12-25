
-- ★★★★★★★★★★★★★★★★★★★★注意： ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
-- 结果表1中周日期需要手动调整
-- ★★★★★★★★★★★★★★★★★★★★注意： ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
-- 数据源 客户标签+每天销售 
drop table if exists csx_analyse_tmp.r_city_customer_sale_sdt;
create table  csx_analyse_tmp.r_city_customer_sale_sdt
as 
select
	a.sdt,
	a.week,
	a.smonth,
	a.performance_region_name as region_name,  
	a.performance_province_name as province_name, 
	a.performance_city_name as city_group_name, 
	a.business_type_name,
	a.customer_code,
	d.customer_name,
	d.second_category_name,
	c.first_sales_date,
	-- 两个新老客标签2选1 因为次新客在上月新客本月老客则有两个标签，会出现两条数据
	if(c.first_sales_date >=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-0),'-',''),'新客','老客') as xinlaok_original,
	if(substr(c.first_sales_date,1,6)>=substr(regexp_replace(add_months(from_unixtime(unix_timestamp(a.sdt,'yyyyMMdd'),'yyyy-MM-dd'),-0),'-',''),1,6),'新客','老客') as xinlaok,
	sum(sale_amt)as sale_amt,
	sum(profit)as profit,
	sum(if(a.order_channel_detail_code=26,0,a.sale_qty)) as sale_qty
from
	(
	select *,
	weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)) week,
	  substr(sdt,1,6) smonth  
	from csx_dws.csx_dws_sale_detail_di
	where sdt >= regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-2),'-','') 
		and sdt <= regexp_replace(add_months('${sdt_yes_date}',0),'-','') 
		and business_type_code=1  
		and shipper_code='YHCSX' 
	)a 	
	left join  -- 首单日期
	(
	select customer_code,min(first_business_sale_date) first_sales_date
	from csx_dws.csx_dws_crm_customer_business_active_di
	where sdt ='current' and business_type_code=1
	group by customer_code
	)c on c.customer_code=a.customer_code 	
    -- -----客户数据
	left join 
	(select * 
	from csx_dim.csx_dim_crm_customer_info 
	where sdt='current' 
	and shipper_code='YHCSX'
	)d on a.customer_code=d.customer_code 	
	left join 
    (select
        code as type,
        max(name) as name,
        max(extra) as extra 
    from csx_dim.csx_dim_basic_topic_dict_df
    where parent_code = 'direct_delivery_type' 
    group by code 
    )h on a.direct_delivery_type=h.type 
    where h.extra='采购参与'		
group by  
	a.sdt,
	a.week,
	a.smonth,
	a.performance_region_name,
	a.performance_province_name,    
	a.performance_city_name,
	a.business_type_name,
	a.customer_code,
	d.customer_name,
	d.second_category_name,
	c.first_sales_date

-- 数据源1：	
select
	region_name,
	province_name,
	city_group_name,
	xinlaok_original,	
	count(distinct case when sdt>='20251101' and sdt<='20251114' then customer_code end) sy_cust,
	count(distinct case when sdt>='20251201' and sdt<='20251214' then customer_code end) by_cust,	
	
	sum(case when sdt>='20251101' and sdt<='20251114' then sale_amt end)/10000 as sy_sale_amt,
	sum(case when sdt>='20251101' and sdt<='20251114' then profit end)/10000 as sy_profit,
	
	sum(case when sdt>='20251201' and sdt<='20251214' then sale_amt end)/10000 as by_sale_amt,
	sum(case when sdt>='20251201' and sdt<='20251214' then profit end)/10000 as by_profit
	
	-- sum(case when sdt>='20251004' and sdt<='20251010' then sale_amt end)/10000 w2_sale_amt,
	-- sum(case when sdt>='20251004' and sdt<='20251010' then profit end)/10000 w2_profit,
    -- 
	-- sum(case when sdt>='20251011' and sdt<='20251017' then sale_amt end)/10000 w3_sale_amt,
	-- sum(case when sdt>='20251011' and sdt<='20251017' then profit end)/10000 w3_profit,
	-- 
	-- sum(case when sdt>='20251018' and sdt<='20251024' then sale_amt end)/10000 w4_sale_amt,
	-- sum(case when sdt>='20251018' and sdt<='20251024' then profit end)/10000 w4_profit,
    -- 
	-- sum(case when sdt>='20251025' and sdt<='20251031' then sale_amt end)/10000 w5_sale_amt,
	-- sum(case when sdt>='20251025' and sdt<='20251031' then profit end)/10000 w5_profit,
    -- 
	-- sum(case when sdt>='20251101' and sdt<='20251107' then sale_amt end)/10000 w1_sale_amt,
	-- sum(case when sdt>='20251101' and sdt<='20251107' then profit end)/10000 w1_profit
from csx_analyse_tmp.r_city_customer_sale_sdt a
group by  
	a.region_name,
	a.province_name,
	a.city_group_name,
	a.xinlaok_original;	
	


-- 数据源2：	
select
	region_name,
	province_name,
	city_group_name,
	business_type_name,
	customer_code,
	customer_name,
	second_category_name,
	first_sales_date,
	xinlaok_original,
	
	sum(case when sdt>='20251101' and sdt<='20251114' then sale_amt end)/10000 as sy_sale_amt,
	sum(case when sdt>='20251101' and sdt<='20251114' then profit end)/10000 as sy_profit,
	sum(case when sdt>='20251101' and sdt<='20251114' then profit end)/abs(sum(case when sdt>='20251101' and sdt<='20251111' then sale_amt end)) sy_profit_rate,	
		
	sum(case when sdt>='20251201' and sdt<='20251214' then sale_amt end)/10000 as by_sale_amt,
	sum(case when sdt>='20251201' and sdt<='20251214' then profit end)/10000 as by_profit,
	sum(case when sdt>='20251201' and sdt<='20251214' then profit end)/abs(sum(case when sdt>='20251201' and sdt<='20251211' then sale_amt end)) by_profit_rate,
	
	sum(case when sdt>='20251201' and sdt<='20251214' then profit end)/abs(sum(case when sdt>='20251201' and sdt<='20251211' then sale_amt end))-sum(case when sdt>='20251001' and sdt<='20251031' then profit end)/abs(sum(case when sdt>='20251001' and sdt<='20251031' then sale_amt end)) as diff_profit_rate

	
	
	-- sum(case when sdt>='20251004' and sdt<='20251010' then profit end)/abs(sum(case when sdt>='20251004' and sdt<='20251010' then sale_amt end)) w2_profit_rate,
	-- sum(case when sdt>='20251011' and sdt<='20251017' then profit end)/abs(sum(case when sdt>='20251011' and sdt<='20251017' then sale_amt end)) w3_profit_rate,
	-- sum(case when sdt>='20251018' and sdt<='20251024' then profit end)/abs(sum(case when sdt>='20251018' and sdt<='20251024' then sale_amt end)) w4_profit_rate,
	-- sum(case when sdt>='20251025' and sdt<='20251031' then profit end)/abs(sum(case when sdt>='20251025' and sdt<='20251031' then sale_amt end)) w5_profit_rate,
	-- sum(case when sdt>='20251101' and sdt<='20251107' then profit end)/abs(sum(case when sdt>='20251101' and sdt<='20251107' then sale_amt end)) w1_profit_rate
	
	
from csx_analyse_tmp.r_city_customer_sale_sdt 
group by  
	region_name,
	province_name,
	city_group_name,
	business_type_name,
	customer_code,
	customer_name,
	second_category_name,
	first_sales_date,
	xinlaok_original;	
	
		
	
/*	
-- 中间表1 表格大数：客户维度 老客影响正向负向汇总
drop table if exists csx_analyse_tmp.r_city_old_cust_eff;
create table  csx_analyse_tmp.r_city_old_cust_eff
as 
with customer_sale_month_week_0 as
( 
select
	a.region_name,
	a.province_name,
	a.city_group_name,
	a.business_type_name,
	a.customer_code,
	a.customer_name,
	a.second_category_name,
	a.first_sales_date,
	xinlaok,
	sum(case when a.sdt between regexp_replace(trunc('${sdt_yes_date}','MM'),'-','') and regexp_replace(add_months('${sdt_yes_date}',0),'-','') then a.sale_amt end) by_sale_amt,
	sum(case when a.sdt between regexp_replace(trunc('${sdt_yes_date}','MM'),'-','') and regexp_replace(add_months('${sdt_yes_date}',0),'-','') then a.sale_qty end) by_sale_qty,
	sum(case when a.sdt between regexp_replace(trunc('${sdt_yes_date}','MM'),'-','') and regexp_replace(add_months('${sdt_yes_date}',0),'-','') then a.profit end) by_profit,	
	sum(case when a.sdt between regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') and regexp_replace(add_months(last_day(date_sub('${sdt_yes_date}',0)),-1),'-','') then a.sale_amt end) sy_sale_amt,
	sum(case when a.sdt between regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') and regexp_replace(add_months(last_day(date_sub('${sdt_yes_date}',0)),-1),'-','') then a.sale_qty end) sy_sale_qty,
	sum(case when a.sdt between regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') and regexp_replace(add_months(last_day(date_sub('${sdt_yes_date}',0)),-1),'-','') then a.profit end) sy_profit,	
	sum(case when a.week=weekofyear(date_sub(current_date, 3)) then a.sale_amt end) bz_sale_amt,
	sum(case when a.week=weekofyear(date_sub(current_date, 3)) then a.sale_qty end) bz_sale_qty,
	sum(case when a.week=weekofyear(date_sub(current_date, 3)) then a.profit end) bz_profit,	
	sum(case when a.week=weekofyear(date_sub(current_date, 10)) then a.sale_amt end) sz_sale_amt,
	sum(case when a.week=weekofyear(date_sub(current_date, 10)) then a.sale_qty end) sz_sale_qty,
	sum(case when a.week=weekofyear(date_sub(current_date, 10)) then a.profit end) sz_profit	
from csx_analyse_tmp.r_city_customer_sale_sdt a
where xinlaok='老客'
group by  
	a.region_name,
	a.province_name,
	a.city_group_name,
	a.business_type_name,
	a.customer_code,
	a.customer_name,
	a.second_category_name,
	a.first_sales_date,
	xinlaok
),

customer_sale_month_week as
(
select 
	province_name ,
	city_group_name,
	business_type_name,
	customer_code,
	customer_name,
	second_category_name,
	first_sales_date,	
	nvl(sum(by_sale_amt),0) by_sale_amt,
	nvl(sum(by_sale_qty),0) by_sale_qty,
	nvl(sum(by_profit),0) by_profit,

	nvl(sum(sy_sale_amt),0) sy_sale_amt,
	nvl(sum(sy_sale_qty),0) sy_sale_qty,
	nvl(sum(sy_profit),0) sy_profit	
from customer_sale_month_week_0
group by 
	province_name ,
	city_group_name,
	business_type_name,
	customer_code,
	customer_name,
	second_category_name,
	first_sales_date
),

city_sale_month_week as  
(
select 
	city_group_name,
	sum(by_sale_amt)by_sale_amt_city,
	sum(by_profit) by_profit_city,
	sum(by_profit)/abs(sum(by_sale_amt)) as by_profit_rate_city,
	sum(sy_sale_amt)sy_sale_amt_city,
	sum(sy_profit) sy_profit_city,
	sum(sy_profit)/abs(sum(sy_sale_amt)) as sy_profit_rate_city	

	-- sum(bz_sale_amt)bz_sale_amt_city,
	-- sum(bz_profit) bz_profit_city,
	-- sum(bz_profit)/abs(sum(bz_sale_amt)) as bz_profit_rate_city,
	-- sum(sz_sale_amt)sz_sale_amt_city,
	-- sum(sz_profit) sz_profit_city,
	-- sum(sz_profit)/abs(sum(sz_sale_amt)) as sz_profit_rate_city
from customer_sale_month_week
group by city_group_name
),

customer_sale_month_week_prorate_eff as
(
select 
	a.*,
	-- 对城市毛利率影响-月
	-- (客户上期销售额*（客户本期毛利率-客户上期毛利率）+(客户本期销售额-客户上期销售额)*(客户本期毛利率-整体本期毛利额/整体本期销售额))/整体上期销售额
	(sy_sale_amt*(nvl(by_profit/by_sale_amt,0)-nvl(sy_profit/sy_sale_amt,0))+(by_sale_amt-sy_sale_amt)*(nvl(by_profit/by_sale_amt,0)-by_profit_city/by_sale_amt_city))
	  /sy_sale_amt_city as y_prorate_eff 
from customer_sale_month_week a 
left join city_sale_month_week b on a.city_group_name=b.city_group_name
)

select *,
if(y_prorate_eff<0,'负向','正向') as eff_flag,
row_number() over(partition by city_group_name order by y_prorate_eff asc ) as num_y
from customer_sale_month_week_prorate_eff
;




-- 结果表1：表格大数：全国新老客毛利（月至今）----------------------
drop table if exists csx_analyse_tmp.r_city_new_old_eff;
create table  csx_analyse_tmp.r_city_new_old_eff
as 
select a.*,
	b.count_cust_eff_f,
	b.count_cust_eff_z,
	b.y_prorate_eff_f,
	b.y_prorate_eff_z,
	from_utc_timestamp(current_timestamp(),'GMT') update_time
from
(
select
	a.region_name,
	a.province_name,
	a.city_group_name,
	a.xinlaok,
	count(distinct case when a.sdt between regexp_replace(trunc('${sdt_yes_date}','MM'),'-','') and regexp_replace(add_months('${sdt_yes_date}',0),'-','') then a.customer_code end) by_cust,	
	count(distinct case when a.sdt between regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') and regexp_replace(add_months(last_day(date_sub('${sdt_yes_date}',0)),-1),'-','') then a.customer_code end) sy_cust,
	
	sum(case when a.sdt between regexp_replace(trunc('${sdt_yes_date}','MM'),'-','') and regexp_replace(add_months('${sdt_yes_date}',0),'-','') then a.sale_amt end) by_sale_amt, 	
    sum(case when a.sdt between regexp_replace(trunc('${sdt_yes_date}','MM'),'-','') and regexp_replace(add_months('${sdt_yes_date}',0),'-','') then a.profit end) by_profit,	 	
    sum(case when a.sdt between regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') and regexp_replace(add_months(last_day(date_sub('${sdt_yes_date}',0)),-1),'-','') then a.sale_amt end) sy_sale_amt, 	
    sum(case when a.sdt between regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') and regexp_replace(add_months(last_day(date_sub('${sdt_yes_date}',0)),-1),'-','') then a.profit end) sy_profit,	
	
	sum(case when a.sdt >='20250605'  and a.sdt <= '20250611' then a.sale_amt end) w1_sale_amt,
	sum(case when a.sdt >='20250605'  and a.sdt <= '20250611' then a.profit end  ) w1_profit,

	sum(case when a.sdt >='20250605'  and a.sdt <= '20250611' then a.sale_amt end) w2_sale_amt,
	sum(case when a.sdt >='20250605'  and a.sdt <= '20250611' then a.profit end  ) w2_profit,

	sum(case when a.sdt >='20250612'  and a.sdt <= '20250618' then a.sale_amt else 0 end) w3_sale_amt,
	sum(case when a.sdt >='20250612'  and a.sdt <= '20250618' then a.profit else 0 end) w3_profit,

	sum(case when a.sdt >='20250419'  and a.sdt <= '20250425' then a.sale_amt else 0 end) w4_sale_amt,
	sum(case when a.sdt >='20250419'  and a.sdt <= '20250425' then a.profit else 0 end) w4_profit,

	sum(case when a.sdt >='20250426'  and a.sdt <= '20250502' then a.sale_amt else 0 end) w5_sale_amt,
	sum(case when a.sdt >='20250426'  and a.sdt <= '20250502' then a.profit else 0 end) w5_profit
from csx_analyse_tmp.r_city_customer_sale_sdt a
group by  
	a.region_name,
	a.province_name,
	a.city_group_name,
	a.xinlaok
)a 
left join
(
select city_group_name,
count(distinct case when eff_flag='负向' then customer_code end) as count_cust_eff_f,
count(distinct case when eff_flag='正向' then customer_code end) as count_cust_eff_z,

sum(if(eff_flag='负向',y_prorate_eff,0)) as y_prorate_eff_f,
sum(if(eff_flag='正向',y_prorate_eff,0)) as y_prorate_eff_z
from csx_analyse_tmp.r_city_old_cust_eff 
group by city_group_name
)b on a.city_group_name=b.city_group_name and a.xinlaok='老客'
;


-- 结果表2：筛选影响top客户 客户对城市影响  客户维度
drop table if exists csx_analyse_tmp.r_cust_eff_city_month_week;
create table  csx_analyse_tmp.r_cust_eff_city_month_week
as 
with customer_sale_month_week_0 as
( 
select
	a.region_name,
	a.province_name,
	a.city_group_name,
	a.business_type_name,
	a.customer_code,
	a.customer_name,
	a.second_category_name,
	a.first_sales_date,
	xinlaok_original,
	sum(case when a.sdt between regexp_replace(trunc('${sdt_yes_date}','MM'),'-','') and regexp_replace(add_months('${sdt_yes_date}',0),'-','') then a.sale_amt end) by_sale_amt,
	sum(case when a.sdt between regexp_replace(trunc('${sdt_yes_date}','MM'),'-','') and regexp_replace(add_months('${sdt_yes_date}',0),'-','') then a.sale_qty end) by_sale_qty,
	sum(case when a.sdt between regexp_replace(trunc('${sdt_yes_date}','MM'),'-','') and regexp_replace(add_months('${sdt_yes_date}',0),'-','') then a.profit end) by_profit,	
	sum(case when a.sdt between regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') and regexp_replace(add_months(last_day(date_sub('${sdt_yes_date}',0)),-1),'-','') then a.sale_amt end) sy_sale_amt,
	sum(case when a.sdt between regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') and regexp_replace(add_months(last_day(date_sub('${sdt_yes_date}',0)),-1),'-','') then a.sale_qty end) sy_sale_qty,
	sum(case when a.sdt between regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') and regexp_replace(add_months(last_day(date_sub('${sdt_yes_date}',0)),-1),'-','') then a.profit end) sy_profit,

	sum(case when a.sdt between regexp_replace(trunc('${sdt_yes_date}','MM'),'-','') and regexp_replace(date_add('${sdt_yes_date}',-7),'-','') then a.sale_amt end) as bysz_sale_amt,
	sum(case when a.sdt between regexp_replace(trunc('${sdt_yes_date}','MM'),'-','') and regexp_replace(date_add('${sdt_yes_date}',-7),'-','') then a.profit end) as bysz_profit,
	sum(case when a.sdt between regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') and regexp_replace(add_months(date_add('${sdt_yes_date}',-7),-1),'-','') then a.sale_amt end) sysz_sale_amt,
	sum(case when a.sdt between regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') and regexp_replace(add_months(date_add('${sdt_yes_date}',-7),-1),'-','') then a.profit end) sysz_profit,
		
	sum(case when a.week=weekofyear(date_sub(current_date, 3)) then a.sale_amt end) bz_sale_amt,
	sum(case when a.week=weekofyear(date_sub(current_date, 3)) then a.sale_qty end) bz_sale_qty,
	sum(case when a.week=weekofyear(date_sub(current_date, 3)) then a.profit end) bz_profit,	
	sum(case when a.week=weekofyear(date_sub(current_date, 10)) then a.sale_amt end) sz_sale_amt,
	sum(case when a.week=weekofyear(date_sub(current_date, 10)) then a.sale_qty end) sz_sale_qty,
	sum(case when a.week=weekofyear(date_sub(current_date, 10)) then a.profit end) sz_profit	
from csx_analyse_tmp.r_city_customer_sale_sdt a
group by  
	a.region_name,
	a.province_name,
	a.city_group_name,
	a.business_type_name,
	a.customer_code,
	a.customer_name,
	a.second_category_name,
	a.first_sales_date,
	xinlaok_original
),

customer_sale_month_week as
(
select 
	-- '' region_name ,
	province_name ,
	city_group_name,
	business_type_name,
	customer_code,
	customer_name,
	-- '' first_category_name,
	second_category_name,
	-- '' third_category_name,
	-- '' first_sales_date ,
	first_sales_date,
	xinlaok_original,
	nvl(sum(by_sale_amt),0) by_sale_amt,
	nvl(sum(by_sale_qty),0) by_sale_qty,
	nvl(sum(by_profit),0) by_profit,

	nvl(sum(sy_sale_amt),0) sy_sale_amt,
	nvl(sum(sy_sale_qty),0) sy_sale_qty,
	nvl(sum(sy_profit),0) sy_profit,

	nvl(sum(bysz_sale_amt),0) bysz_sale_amt,
	nvl(sum(bysz_profit),0) bysz_profit,	
	nvl(sum(sysz_sale_amt),0) sysz_sale_amt,
	nvl(sum(sysz_profit),0) sysz_profit,
	
	nvl(sum(bz_sale_amt),0) bz_sale_amt,
	nvl(sum(bz_sale_qty),0) bz_sale_qty,
	nvl(sum(bz_profit),0)  bz_profit,	

	nvl(sum(sz_sale_amt),0) sz_sale_amt,
	nvl(sum(sz_sale_qty),0) sz_sale_qty,
	nvl(sum(sz_profit),0)  sz_profit	
from customer_sale_month_week_0
group by 
	-- region_name ,
	province_name ,
	city_group_name,
	business_type_name,
	customer_code,
	customer_name,
	second_category_name,
	first_sales_date,
	xinlaok_original
),

city_sale_month_week as  
(
select 
	-- province_name,
	city_group_name,
	sum(by_sale_amt) by_sale_amt_city,
	sum(by_profit) by_profit_city,
	sum(by_profit)/abs(sum(by_sale_amt)) as by_profit_rate_city,
	sum(sy_sale_amt) sy_sale_amt_city,
	sum(sy_profit) sy_profit_city,
	sum(sy_profit)/abs(sum(sy_sale_amt)) as sy_profit_rate_city,
	
	sum(bysz_sale_amt) bysz_sale_amt_city,
	sum(bysz_profit) bysz_profit_city,
	sum(bysz_profit)/abs(sum(bysz_sale_amt)) as bysz_profit_rate_city,
	sum(sysz_sale_amt) sysz_sale_amt_city,
	sum(sysz_profit) sysz_profit_city,
	sum(sysz_profit)/abs(sum(sysz_sale_amt)) as sysz_profit_rate_city,	

	sum(bz_sale_amt) bz_sale_amt_city,
	sum(bz_profit) bz_profit_city,
	sum(bz_profit)/abs(sum(bz_sale_amt)) as bz_profit_rate_city,
	sum(sz_sale_amt) sz_sale_amt_city,
	sum(sz_profit) sz_profit_city,
	sum(sz_profit)/abs(sum(sz_sale_amt)) as sz_profit_rate_city
from customer_sale_month_week
group by city_group_name
),
prov_sale_month_week as  
(
select 
	province_name,
	sum(by_sale_amt)by_sale_amt_prov,
	sum(by_profit) by_profit_prov,
	sum(by_profit)/abs(sum(by_sale_amt)) as by_profit_rate_prov,
	sum(sy_sale_amt)sy_sale_amt_prov,
	sum(sy_profit) sy_profit_prov,
	sum(sy_profit)/abs(sum(sy_sale_amt)) as sy_profit_rate_prov,	

	sum(bz_sale_amt)bz_sale_amt_prov,
	sum(bz_profit) bz_profit_prov,
	sum(bz_profit)/abs(sum(bz_sale_amt)) as bz_profit_rate_prov,
	sum(sz_sale_amt)sz_sale_amt_prov,
	sum(sz_profit) sz_profit_prov,
	sum(sz_profit)/abs(sum(sz_sale_amt)) as sz_profit_rate_prov
from customer_sale_month_week
group by province_name
),
customer_sale_month_week_prorate_eff as
(
select 
	a.*,
	-- 对省区毛利率影响-月
	-- (客户上期销售额*（客户本期毛利率-客户上期毛利率）+(客户本期销售额-客户上期销售额)*(客户本期毛利率-整体本期毛利额/整体本期销售额))/整体上期销售额
	(sy_sale_amt*(nvl(by_profit/by_sale_amt,0)-nvl(sy_profit/sy_sale_amt,0))+(by_sale_amt-sy_sale_amt)*(nvl(by_profit/by_sale_amt,0)-by_profit_prov/by_sale_amt_prov))
	  /sy_sale_amt_prov as y_prorate_eff_prov,
	  
	-- 对城市毛利率影响-周
	(sz_sale_amt*(nvl(bz_profit/bz_sale_amt,0)-nvl(sz_profit/sz_sale_amt,0))+(bz_sale_amt-sz_sale_amt)*(nvl(bz_profit/bz_sale_amt,0)-bz_profit_prov/bz_sale_amt_prov))
	  /sz_sale_amt_prov as z_prorate_eff_prov,

	-- 对城市毛利率影响-月
	-- (客户上期销售额*（客户本期毛利率-客户上期毛利率）+(客户本期销售额-客户上期销售额)*(客户本期毛利率-整体本期毛利额/整体本期销售额))/整体上期销售额
	(sy_sale_amt*(nvl(by_profit/by_sale_amt,0)-nvl(sy_profit/sy_sale_amt,0))+(by_sale_amt-sy_sale_amt)*(nvl(by_profit/by_sale_amt,0)-by_profit_city/by_sale_amt_city))
	  /sy_sale_amt_city as y_prorate_eff,

	-- 对城市毛利率影响-月 月上周
	-- (客户上期销售额*（客户本期毛利率-客户上期毛利率）+(客户本期销售额-客户上期销售额)*(客户本期毛利率-整体本期毛利额/整体本期销售额))/整体上期销售额
	(sysz_sale_amt*(nvl(bysz_profit/bysz_sale_amt,0)-nvl(sysz_profit/sysz_sale_amt,0))+(bysz_sale_amt-sysz_sale_amt)*(nvl(bysz_profit/bysz_sale_amt,0)-bysz_profit_city/bysz_sale_amt_city))
	  /sysz_sale_amt_city as ysz_prorate_eff,
	  
	-- 对城市毛利率影响-周
	(sz_sale_amt*(nvl(bz_profit/bz_sale_amt,0)-nvl(sz_profit/sz_sale_amt,0))+(bz_sale_amt-sz_sale_amt)*(nvl(bz_profit/bz_sale_amt,0)-bz_profit_city/bz_sale_amt_city))
	  /sz_sale_amt_city as z_prorate_eff	  
from customer_sale_month_week a 
left join city_sale_month_week b on a.city_group_name=b.city_group_name
left join prov_sale_month_week c on a.province_name=c.province_name
)

select *,
row_number() over(partition by city_group_name order by y_prorate_eff asc ) as num_y,
row_number() over(partition by city_group_name order by z_prorate_eff asc ) as num_z,
from_utc_timestamp(current_timestamp(),'GMT') update_time
from customer_sale_month_week_prorate_eff
;




-- 结果表查询
/*
------------------------------------------------------------------------------------------------------------------------

新老客PPT

select *
from csx_analyse_tmp.r_city_new_old_eff;

客户对城市毛利影响
select *
from csx_analyse_tmp.r_cust_eff_city_month_week;









