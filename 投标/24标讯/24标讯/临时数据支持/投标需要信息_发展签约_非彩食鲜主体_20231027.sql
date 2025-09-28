-- ====================================================================================================================================================

drop table if exists csx_analyse_tmp.csx_analyse_tmp_toubiao_non_20230104;
create table csx_analyse_tmp.csx_analyse_tmp_toubiao_non_20230104
as
select 
	a.performance_province_name,a.performance_city_name,a.customer_code,b.customer_name,b.sales_user_name,a.sale_amt,a.profit,c.table_type_name,
	regexp_replace(to_date(b.sign_time),'-','') as sign_date,a.sign_company_code,c.company_name,b.first_category_name,b.second_category_name,b.third_category_name,d.last_sale_date
from 
	(
	select
		performance_province_name,performance_city_name,sign_company_code,customer_code,
		sum(sale_amt) sale_amt,
		sum(profit) profit
	from 
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20190101' and sdt<='20231026'
		and channel_code in('1','7','9')
	group by 
		performance_province_name,performance_city_name,sign_company_code,customer_code
	)a 
	join 
		(
		select 
			customer_code,customer_name,sign_time,sales_user_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name
		from 
			csx_dim.csx_dim_crm_customer_info
		where 
			sdt='current'
		)b on a.customer_code=b.customer_code
	join
		(
		select 
			company_code,company_name,table_type,
			case when table_type=1 then '彩食鲜'
				when table_type=2 then '永辉'
				else '其他' 
			end table_type_name
		from 
			csx_dim.csx_dim_basic_company
		where 
			sdt = 'current'
			and table_type !=1
		)c on a.sign_company_code = c.company_code
	left join
		(
		select 
			customer_code,last_sale_date
		from 
			-- csx_dw.dws_crm_w_a_customer_active
			csx_dws.csx_dws_crm_customer_active_di
		where 
			sdt='current'
		group by 
			customer_code,last_sale_date
		) d on d.customer_code=a.customer_code
;
select * from csx_analyse_tmp.csx_analyse_tmp_toubiao_non_20230104



	