
select
	b.quarter_of_year as `季度`,
	a.performance_region_name as `大区`,
	a.performance_province_name as `省份`,
	sum(sale_amt)/10000 as `销售额_福利BBC`,
	sum(profit)/10000 as `毛利额_福利BBC`,
	sum(profit)/abs(sum(sale_amt)) as `毛利率`
from
	(
	select 
		sdt,performance_region_name,performance_province_name,business_type_code,sale_amt,profit	
	from 
		csx_dws.csx_dws_sale_detail_di
	where 
		(sdt between '20220701' and '20220930' or sdt between '20230401' and '20230630')
		and channel_code in('1','7','9')
		and business_type_code in(2,6)
		and goods_name not like '%茅台%'
		and goods_name not like '%五浪液%'
	) a 
	left join
		(
		select
			calday,quarter_of_year,csx_week,csx_week_begin,csx_week_end
		from
			csx_dim.csx_dim_basic_date
		) b on b.calday=a.sdt
group by 
	b.quarter_of_year,
	a.performance_region_name,
	a.performance_province_name	
	
	
	
	
-- ===============================================================================================================================================================================
drop table csx_analyse_tmp.csx_analyse_tmp_cust_business_detail_03;
create table csx_analyse_tmp.csx_analyse_tmp_cust_business_detail_03
as

	select
		b.quarter_of_year,
		a.business_sign_month,a.business_number,a.customer_id,a.customer_code,a.customer_name,a.first_category_name,a.second_category_name,a.third_category_name,
		a.performance_region_name,a.performance_province_name,a.performance_city_name,
		a.contract_cycle_desc,a.estimate_contract_amount,a.business_sign_date	
	from 
		(
		select
			business_sign_time,
			regexp_replace(substr(to_date(business_sign_time),1,7),'-','') business_sign_month,business_number,customer_id,customer_code,customer_name,
			first_category_name,second_category_name,third_category_name,
			performance_region_name,performance_province_name,performance_city_name,
			contract_cycle_desc,estimate_contract_amount,
			regexp_replace(to_date(business_sign_time),'-','') business_sign_date
		from 
			csx_dim.csx_dim_crm_business_info
		where 
			sdt='current'
			-- and channel_code in('1','7','9')
			and business_attribute_code in (2, 5) -- 商机属性编码 1：日配客户 2：福利客户 3：大宗贸易 4：m端 5：bbc 6：内购
			and status=1  -- 是否有效 0.无效 1.有效 (status=0,'停止跟进')
			and business_stage=5
			and (to_date(business_sign_time) between '2022-07-01' and '2022-09-30' or to_date(business_sign_time) between '2023-04-01' and '2023-06-30')
		)a
		left join
			(
			select
				calday,quarter_of_year,csx_week,csx_week_begin,csx_week_end
			from
				csx_dim.csx_dim_basic_date
			) b on b.calday=a.business_sign_date
;
select * from csx_analyse_tmp.csx_analyse_tmp_cust_business_detail_03

