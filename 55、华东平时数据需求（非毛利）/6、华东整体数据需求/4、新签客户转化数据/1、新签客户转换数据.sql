drop table if exists csx_analyse_tmp.new_sign_customer;
create table if not exists csx_analyse_tmp.new_sign_customer as 
select
	a.sign_month,
	b.performance_province_name,
	b.performance_city_name,
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
    and substr(cast(business_sign_time as string),1,7)>='2025-01' 
    and status=1 
    group by customer_code,regexp_replace(substr(cast(business_sign_time as string),1,7),'-','') 
)a
join 
csx_ads.csx_ads_sale_new_sign_customer_detail_1m b
on a.customer_code =b.customer_code and a.sign_month=b.month 
where b.performance_region_name='华东大区' 
group by a.sign_month,b.performance_province_name,b.performance_city_name,b.customer_code
order by a.sign_month,b.performance_province_name,b.performance_city_name,max(b.first_sign_date),max(a.maxSignDate),b.customer_code
;



drop table if exists csx_analyse_tmp.sale_detail_customer;
create table if not exists csx_analyse_tmp.sale_detail_customer as 
select 
	t.*,
	min(t.sale_month)over(partition by t.customer_code) as min_sale_month
from 
(select 
	customer_code,
	substr(sdt,1,6) as sale_month,
	sum(sale_amt) as sale_amt 
from csx_dws.csx_dws_sale_detail_di 
where sdt>='20250101' 
and performance_region_name='华东大区' 
group by 
	customer_code,
	substr(sdt,1,6)
) t 
;



drop table if exists csx_analyse_tmp.new_sign_customer_final;
create table if not exists csx_analyse_tmp.new_sign_customer_final as 
select 
	a.*,
	b.min_sale_month,

	b1.min_sale_month as one_min_sale_month,
	b1.sale_month as one_sale_month,
	b1.sale_amt as one_sale_amt,

	b2.min_sale_month as two_min_sale_month,
	b2.sale_month as two_sale_month,
	b2.sale_amt as two_sale_amt,

	b3.min_sale_month as three_min_sale_month,
	b3.sale_month as three_sale_month,
	b3.sale_amt as three_sale_amt,

	b4.min_sale_month as four_min_sale_month,
	b4.sale_month as four_sale_month,
	b4.sale_amt as four_sale_amt,

	b5.min_sale_month as five_min_sale_month,
	b5.sale_month as five_sale_month,
	b5.sale_amt as five_sale_amt   
from 
	(select 
		*,
		substr(first_sign_date,1,6) as first_sign_month  
	from csx_analyse_tmp.new_sign_customer 
	where substr(first_sign_date,1,6)>='202501' 
	) a 
	left join 
	(select * 
	from csx_analyse_tmp.sale_detail_customer 
	where sale_month='202501'
	) b1 
	on a.customer_code=b1.customer_code 
	left join 
	(select * 
	from csx_analyse_tmp.sale_detail_customer 
	where sale_month='202502'
	) b2 
	on a.customer_code=b2.customer_code 
	left join 
	(select * 
	from csx_analyse_tmp.sale_detail_customer 
	where sale_month='202503'
	) b3 
	on a.customer_code=b3.customer_code 
	left join 
	(select * 
	from csx_analyse_tmp.sale_detail_customer 
	where sale_month='202504'
	) b4 
	on a.customer_code=b4.customer_code 
	left join 
	(select * 
	from csx_analyse_tmp.sale_detail_customer 
	where sale_month='202505'
	) b5 
	on a.customer_code=b5.customer_code 
	left join 
	(select customer_code,min(sale_month) as min_sale_month  
	from csx_analyse_tmp.sale_detail_customer 
	group by customer_code 
	) b 
	on a.customer_code=b.customer_code 
;


select 
	first_sign_month as `首次签约月份`,
	performance_province_name as `省区`,
	performance_city_name as `城市`,
	customer_code as `客户编码`,
	customer_name as `客户名称`,
	new_or_old_customer_mark as `新老客`,
	first_sign_date as `首次签约日期`,
	max_sign_date as `最晚签约日期`,
	sign_amount as `签约金额`,

	min_sale_month as `首次履约日期`,

	one_min_sale_month as `首次签约日期1月`,
	one_sale_month as `1月`,
	one_sale_amt as `1月销售额`,

	two_min_sale_month as `首次签约日期2月`,
	two_sale_month as `2月`,
	two_sale_amt as `2月销售额`,

	three_min_sale_month as `首次签约日期3月`,
	three_sale_month as `3月`,
	three_sale_amt as `3月销售额`,

	four_min_sale_month as `首次签约日期4月`,
	four_sale_month as `4月`,
	four_sale_amt as `4月销售额`,

	five_min_sale_month as `首次签约日期5月`,
	five_sale_month as `5月`,
	five_sale_amt as `5月销售额`  
from csx_analyse_tmp.new_sign_customer_final 