-- --------------------------------------------------------------------------
-- ------------新签数据
drop table if exists csx_analyse_tmp.new_sign_customer;
create table if not exists csx_analyse_tmp.new_sign_customer as 
select
	a.sign_month,
	-- b.performance_province_name,
	-- b.performance_city_name,
	b.customer_code,
	max(b.customer_name) as customer_name,
	max(b.new_or_old_customer_mark) as new_or_old_customer_mark,
	max(b.first_sign_date) as first_sign_date,
	max(a.maxSignDate) as max_sign_date,
	max(a.signAmount) as sign_amount,
	max(b.first_category_name) as first_category_name,
	max(b.second_category_name) as second_category_name,
	max(b.third_category_name) as third_category_name,
	max(b.sales_user_number) as sales_user_number,
	max(b.sales_user_name) as sales_user_name,
	max(b.supervisor_user_number) as supervisor_user_number,
	max(b.supervisor_user_name) as supervisor_user_name,
	max(b.city_manager_user_number) as city_manager_user_number,
	max(b.city_manager_user_name) as city_manager_user_name,
	max(b.province_manager_user_number) as province_manager_user_number,
	max(b.province_manager_user_name) as province_manager_user_name  
from
(
    select
        customer_code,
        regexp_replace(substr(cast(business_sign_time as string),1,7),'-','') as sign_month,
        SUM(CAST(estimate_contract_amount AS DECIMAL(20,4))) as signAmount,
        count(business_number) as newSignBizOppCount,
        to_date(max(business_sign_time)) as maxSignDate,
        concat_ws(',', collect_set(cast(business_attribute_code as string))) as attribute,
        concat_ws(',', collect_set(business_attribute_name)) as bizOppAttributeName
    from csx_dim.csx_dim_crm_business_info  
    where sdt='current'
    and business_stage = 5 and business_attribute_code in (1,2,5) 
    and substr(cast(business_sign_time as string),1,7)>='2025-04'    
    and status=1 
    group by customer_code,regexp_replace(substr(cast(business_sign_time as string),1,7),'-','') 
)a
join 
csx_ads.csx_ads_sale_new_sign_customer_detail_1m b
on a.customer_code =b.customer_code and a.sign_month=b.month 
where b.performance_region_name='华东大区' 
and b.performance_province_name='上海' 
and a.sign_month=substr(b.first_sign_date,1,6) -- 限定是新签客户 
group by a.sign_month,b.customer_code
order by a.sign_month,max(b.first_sign_date),max(a.maxSignDate),b.customer_code
;

-- ------------新履约数据
drop table if exists csx_analyse_tmp.sale_detail_customer;
create table if not exists csx_analyse_tmp.sale_detail_customer as 
select
	substr(a.sdt,1,6) as month,
	-- a.performance_region_name,
	-- a.performance_province_name,	
	-- a.performance_city_name,
	-- a.business_type_name,
	a.customer_code,
	sum(a.sale_amt)/10000 sale_amt,
	sum(a.profit)/10000 profit		
from 
	(select 
		* 
	from csx_dws.csx_dws_sale_detail_di 
	where sdt >='20250401' and sdt <= '20250630' 
	and performance_region_name='华东大区' 
	and performance_province_name='上海'
	) a
	left join  -- 首单日期
	(select 
	    customer_code,
		business_type_code,
		min(first_business_sale_date) first_sales_date
	from csx_dws.csx_dws_crm_customer_business_active_di
	where sdt ='current' 
	group by customer_code,business_type_code
	)c 
	on c.customer_code=a.customer_code and c.business_type_code=a.business_type_code 
where substr(a.sdt,1,6)=substr(c.first_sales_date,1,6) 
group by 
	substr(a.sdt,1,6),
	-- a.performance_region_name,
	-- a.performance_province_name,	
	-- a.performance_city_name,
	-- a.business_type_name,
	a.customer_code
;

-- 所有客户履约数据
drop table if exists csx_analyse_tmp.sale_detail_customer_all;
create table if not exists csx_analyse_tmp.sale_detail_customer_all as 
select
	substr(a.sdt,1,6) as month,
	-- a.performance_region_name,
	-- a.performance_province_name,	
	-- a.performance_city_name,
	a.business_type_name,
	a.customer_code,
	sum(a.sale_amt)/10000 sale_amt,
	sum(a.profit)/10000 profit		
from 
	(select 
		* 
	from csx_dws.csx_dws_sale_detail_di 
	where sdt >='20250401' and sdt <= '20250630' 
	and performance_region_name='华东大区' 
	and performance_province_name='上海'
	) a
	left join  -- 首单日期
	(select 
	    customer_code,
		business_type_code,
		min(first_business_sale_date) first_sales_date
	from csx_dws.csx_dws_crm_customer_business_active_di
	where sdt ='current' 
	group by customer_code,business_type_code
	)c 
	on c.customer_code=a.customer_code and c.business_type_code=a.business_type_code 
-- where substr(a.sdt,1,6)=substr(b.first_sign_date,1,6) 
group by 
	substr(a.sdt,1,6),
	-- a.performance_region_name,
	-- a.performance_province_name,	
	-- a.performance_city_name,
	a.business_type_name,
	a.customer_code
;

-- 最终数据表
drop table if exists csx_analyse_tmp.new_sign_customer_final;
create table if not exists csx_analyse_tmp.new_sign_customer_final as 
select 
	'销售' as type,
	a.month,
	a.performance_province_name,
	a.performance_city_name,
	a.customer_code,
	a.customer_name,
	a.sales_user_name,
	a.supervisor_user_name,
	
	b.customer_code as sign_cus,
	c.customer_code as ly_cus,
	c.sale_amt,
	c.profit 
from 
	(select 
		*,
		substr(sdt,1,6) as month 
	from csx_dim.csx_dim_crm_customer_info 
	where sdt in ('20250430','20250531','20250630') 
	and performance_province_name='上海'
	) a 
	left join 
	csx_analyse_tmp.new_sign_customer b 
	on a.month=b.sign_month and a.customer_code=b.customer_code 
	left join 
	csx_analyse_tmp.sale_detail_customer c 
	on a.month=c.month and a.customer_code=c.customer_code 
union all 
select 
	'服务管家' as type,
	a1.month,
	a3.performance_province_name,
	a3.performance_city_name,
	a1.customer_code,
	a3.customer_name,
	(case when a1.business_type_name='日配业务' then a2.rp_service_user_name_new 
		  when a1.business_type_name='福利业务' then a2.fl_service_user_name 
		  when a1.business_type_name='BBC' then a2.bbc_service_user_name 
		  when a1.business_type_name='批发内购' then a2.ng_service_user_name   
	end) as sales_user_name,
	a1.customer_code as sign_cus,
	a1.customer_code as ly_cus,
	a1.sale_amt,
	a1.profit  
from 
	csx_analyse_tmp.sale_detail_customer_all a1 
	left join 
	(select 
		*,
		substr(sdt,1,6) as month 
	from csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df 
	where sdt in ('20250430','20250531','20250630')  
	) a2 
	on a1.customer_code=a2.customer_no and a1.month=a2.month 
	left join 
	(select 
		*,
		substr(sdt,1,6) as month 
	from csx_dim.csx_dim_crm_customer_info 
	where sdt in ('20250430','20250531','20250630') 
	and performance_province_name='上海'
	) a3  
	on a1.customer_code=a3.customer_code and a1.month=a3.month 
;


-- --------------------------------------------------------------------------
-- ------------服务管家Q2每个月维护的客户数及业绩数据
select 
	type as `数据类型`,
	month as `月份`,
	performance_province_name as `省区`,
	performance_city_name as `城市`,
	customer_code as `客户编码`,
	customer_name as `客户名称`,
	sales_user_name as `销售员`,
	sign_cus as `是否是新签约客户`,
	ly_cus as `是否是新履约客户`,
	sale_amt as `销售额`,
	profit as `毛利额`  
from csx_analyse_tmp.new_sign_customer_final 
